from App.config import getConfiguration
from collections import Counter
from collections import defaultdict
from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.dexterity.utils import iterSchemataForType
from zope.component import getUtility
from zope.intid.interfaces import IIntIds
from zope.schema import getFieldsInOrder
from zope.schema.interfaces import ValidationError
import logging
import os
import transaction
import sys

root_logger = logging.root

"""
Script to check schema conformance of objects.

    bin/instance run validate_object_schema_conformance.py

Options:
--verbose : errors will be grouped according to error message instead of their class.
            This only affects logging and will likely produce too much output on large
            installations. The full error message is always saved in the CSV report.

This script logs a detailed CSV report and a summary to var/log/, and displays
some progress info and stats on STDERR/STDOUT.
"""


class SchemaNonConformingObjectsFinder(object):

    CSV_HEADER = "intid;portal_type;path;created;missing_fields;invalid_fields;failed_fields"
    SCHEMA_CACHE = {}
    FIELD_CACHE = {}

    def __init__(self, options):
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)

        # This is a dictionary with defaults to a depth of 3 meant
        # for entries of the type stats[portal_type][field_name][error_type]
        self.stats = defaultdict(lambda: defaultdict(Counter))
        # stats["global"] keeps track of overall number of conforming objects
        self.stats["global"] = Counter(conforming=0, non_conforming=0)
        # stats["per_portal_type"] keeps track of number of conforming objects
        # per portal type
        self.stats["per_portal_type"] = defaultdict(Counter)

        ts = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        self.csv_log_path = self.get_logfile_path(
            'find-nonconforming-objects-%s.csv' % ts)
        self.summary_log_path = self.get_logfile_path(
            'find-nonconforming-objects-summary-%s.log' % ts)

        if options.verbose:
            self.error_repr = self.verbose_error_repr
        else:
            self.error_repr = self.non_verbose_error_repr

    @staticmethod
    def verbose_error_repr(err):
        return repr(err)

    @staticmethod
    def non_verbose_error_repr(err):
        return str(type(err))

    def run(self):
        sys.stderr.write("Checking for object not conforming to schema...\n\n")

        all_brains = self.catalog.unrestrictedSearchResults()
        total = len(all_brains)
        object_generator = (brain.getObject() for brain in all_brains)

        with open(self.csv_log_path, 'w') as self.csv_log:
            with open(self.summary_log_path, 'w') as self.summary_log:
                self.csv_log.write(self.CSV_HEADER + '\n')

                for i, obj in enumerate(object_generator):
                    missing_fields, invalid_fields, failed = self.validate_schema_conformance(obj)
                    self.update_stats(obj, missing_fields, invalid_fields, failed)

                    if missing_fields or invalid_fields or failed:
                        self.write_csv_row(obj, missing_fields, invalid_fields, failed)

                    if i % 100 == 0:
                        sys.stderr.write("Progress: %s of %s objects\n" % (i, total))
                self.display_stats()

    def validate_schema_conformance(self, obj):
        missing_fields = []
        invalid_fields = []
        failed_validations = []

        portal_type = obj.portal_type
        if portal_type not in self.SCHEMA_CACHE:
            self.SCHEMA_CACHE[portal_type] = list(iterSchemataForType(portal_type))
        schemas = self.SCHEMA_CACHE[portal_type]

        for schema in schemas:
            if schema.__identifier__ not in self.FIELD_CACHE:
                self.FIELD_CACHE[schema.__identifier__] = getFieldsInOrder(schema)
            fields = self.FIELD_CACHE[schema.__identifier__]

            for name, field in fields:
                field_instance = field.bind(obj)
                try:
                    value = field_instance.get(field.interface(obj))
                except AttributeError as err:
                    missing_fields.append((self.get_field_label(schema, name), err))
                    continue
                except Exception as err:
                    missing_fields.append((self.get_field_label(schema, name), err))
                    continue

                # Empty field is OK if the field is not required
                if value is None and not field.required:
                    continue
                try:
                    field_instance.validate(value)
                except ValidationError as err:
                    invalid_fields.append((self.get_field_label(schema, name), err))
                except Exception as err:
                    failed_validations.append((self.get_field_label(schema, name), err))

        missing_fields.sort()
        invalid_fields.sort()
        failed_validations.sort()
        return missing_fields, invalid_fields, failed_validations

    @staticmethod
    def get_field_label(schema, field_name):
        return ".".join((schema.__identifier__.split(".")[-1], field_name))

    @staticmethod
    def get_portal_type_label(portal_type):
        return ".".join(portal_type.split(".")[-2:])

    def write_csv_row(self, obj, missing_fields, invalid_fields, failed):
        created = str(obj.created())
        intid = self.intids.queryId(obj)
        row = (str(intid),
               obj.portal_type,
               '/'.join(obj.getPhysicalPath()),
               created,
               str(missing_fields),
               str(invalid_fields),
               str(failed),)
        self.csv_log.write(';'.join(row) + '\n')

    def update_stats(self, obj, missing_fields, invalid_fields, failed):
        portal_type = obj.portal_type

        if not (missing_fields or invalid_fields or failed):
            self.stats['global']['conforming'] += 1
            self.stats['per_portal_type'][portal_type]['conforming'] += 1
            return

        self.stats['global']['non_conforming'] += 1
        self.stats['per_portal_type'][portal_type]['non_conforming'] += 1

        for field, err in missing_fields:
            self.stats[portal_type][field]["missing"] += 1
            self.stats[portal_type][field][self.error_repr(err)] += 1

        for field, err in invalid_fields:
            self.stats[portal_type][field]["invalid"] += 1
            self.stats[portal_type][field][self.error_repr(err)] += 1

        for field, err in failed:
            self.stats[portal_type][field]["failed"] += 1
            self.stats[portal_type][field][self.error_repr(err)] += 1

    def display_stats(self):

        global_frmt = "{:<50} {:>10} {:>10} {:>10} {:>10}\n"
        detailed_frmt = "{:<50} {:>10} {:>10} {:>10} {:>10}\n"
        error_frmt = "{:<50} {:<70} {:>10}\n"

        def log(line):
            sys.stdout.write(line)
            self.summary_log.write(line)

        def get_global_stat_line(portal_type, stats):
            conforming = stats[portal_type]["conforming"]
            non_conforming = stats[portal_type]["non_conforming"]
            total = conforming + non_conforming
            frac = int(100*non_conforming/float(total))
            type_label = self.get_portal_type_label(portal_type)
            return global_frmt.format(type_label, total, conforming, non_conforming, frac)

        def get_detailed_stat_line(portal_type, field):
            missing = self.stats[portal_type][field]["missing"]
            invalid = self.stats[portal_type][field]["invalid"]
            failed = self.stats[portal_type][field]["failed"]
            total = (self.stats["per_portal_type"][portal_type]["conforming"] +
                     self.stats["per_portal_type"][portal_type]["non_conforming"])
            return detailed_frmt.format(field, total, missing, invalid, failed)

        def get_error_stat_line(portal_type, field, err):
            count = self.stats[portal_type][field][err]
            return error_frmt.format(field, err, count)

        log("\n")

        log("{:=>134}\n".format(""))
        log("Overall statistics:\n\n")
        log(global_frmt.format("Type", "Total", "Ok", "Not OK", "% Not OK"))
        log("{:->94}\n".format(""))
        log(get_global_stat_line("global", self.stats))

        for portal_type in sorted(self.stats["per_portal_type"]):
            log(get_global_stat_line(portal_type, self.stats["per_portal_type"]))

        log("\n\n")
        for portal_type in sorted(self.stats):
            if portal_type in ("global", "per_portal_type"):
                continue
            log("{:=>134}\n".format(""))
            log("\n\n")
            log("{}\n\n".format(portal_type))

            log(detailed_frmt.format("Field", "Total", "Missing", "Invalid", "Failed"))
            log("{:->94}\n".format(""))
            for field in sorted(self.stats[portal_type]):
                log(get_detailed_stat_line(portal_type, field))
            log("\n\n")

            log(error_frmt.format("Field", "Error", "Count"))
            log("{:->132}\n".format(""))
            for field in sorted(self.stats[portal_type]):
                for err in sorted(self.stats[portal_type][field]):
                    if err in ("missing", "invalid", "failed"):
                        continue
                    log(get_error_stat_line(portal_type, field, err))
            log("\n\n")

        log("\n")

        log("Detailed CSV report written to %s\n" % self.csv_log_path)
        log("Summary written to %s\n" % self.summary_log_path)

    def get_logfile_path(self, filename):
        log_dir = self.get_logdir()
        return os.path.join(log_dir, filename)

    def get_logdir(self):
        """Determine the log directory.
        This will be derived from Zope2's EventLog location, in order to not
        have to figure out the path to var/log/ ourselves.
        """
        zconf = getConfiguration()
        eventlog = getattr(zconf, 'eventlog', None)

        if eventlog is None:
            root_logger.error('')
            root_logger.error(
                "Couldn't find eventlog configuration in order to determine "
                "logfile location - aborting!")
            root_logger.error('')
            sys.exit(1)

        handler_factories = eventlog.handler_factories
        eventlog_path = handler_factories[0].section.path
        assert eventlog_path.endswith('.log')
        log_dir = os.path.dirname(eventlog_path)
        return log_dir


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    finder = SchemaNonConformingObjectsFinder(options)
    finder.run()
