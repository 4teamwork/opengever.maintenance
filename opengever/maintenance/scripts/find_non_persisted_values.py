"""
Script to find field values that haven't been persisted on objects.

    bin/instance run find_non_persisted_values.py

This script logs a detailed CSV report and a summary to var/log/, and displays
some progress info and stats on STDERR/STDOUT.
"""

from App.config import getConfiguration
from collections import Counter
from datetime import datetime
from opengever.base.default_values import get_persisted_value_for_field
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from operator import itemgetter
from plone import api
from plone.dexterity.utils import iterSchemataForType
from zope.component import getUtility
from zope.intid.interfaces import IIntIds
from zope.schema import getFieldsInOrder
import logging
import os
import sys
import transaction


root_logger = logging.root


class NonPersistedValueFinder(object):

    CSV_HEADER = "intid;portal_type;path;created;missing_fields"
    SCHEMA_CACHE = {}
    FIELD_CACHE = {}

    def __init__(self):
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)

        self.stats = Counter()
        self.stats['by_field'] = Counter()

        ts = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        self.csv_log_path = self.get_logfile_path(
            'find-nonpersistent-values-%s.csv' % ts)
        self.summary_log_path = self.get_logfile_path(
            'find-nonpersistent-values-summary-%s.log' % ts)

    def run(self):
        sys.stderr.write("Checking for non-persisted values...\n\n")

        all_brains = self.catalog.unrestrictedSearchResults()
        total = len(all_brains)

        with open(self.csv_log_path, 'w') as self.csv_log:
            with open(self.summary_log_path, 'w') as self.summary_log:
                self.csv_log.write(self.CSV_HEADER + '\n')

                for i, brain in enumerate(all_brains):
                    obj = brain.getObject()
                    missing_fields = self.check_for_missing_fields(obj)
                    self.update_stats(missing_fields)

                    if missing_fields:
                        self.write_csv_row(obj, missing_fields)

                    if i % 100 == 0:
                        sys.stderr.write("Progress: %s of %s objects\n" % (i, total))

                self.display_stats()

    def check_for_missing_fields(self, obj):
        missing_fields = []
        portal_type = obj.portal_type

        if portal_type not in self.SCHEMA_CACHE:
            self.SCHEMA_CACHE[portal_type] = list(iterSchemataForType(portal_type))
        schemas = self.SCHEMA_CACHE[portal_type]

        for schema in schemas:
            if schema.__identifier__ not in self.FIELD_CACHE:
                self.FIELD_CACHE[schema.__identifier__] = map(
                    itemgetter(1), getFieldsInOrder(schema))
            fields = self.FIELD_CACHE[schema.__identifier__]

            for field in fields:
                name = field.getName()

                if name == 'changeNote':
                    # The changeNote field from p.a.versioningbehavior
                    # is a "fake" field - it never gets persisted, but
                    # written to request annotations instead
                    continue

                if name == 'reference_number':
                    # reference_number is a special field. It never gets
                    # set directly, but instead acts as a computed field
                    # for all intents and purposes.
                    continue

                try:
                    get_persisted_value_for_field(obj, field)
                except AttributeError:
                    missing_fields.append((schema.__identifier__, name))

        missing_fields.sort()
        return missing_fields

    def write_csv_row(self, obj, missing_fields):
        created = str(obj.created())
        intid = self.intids.queryId(obj)
        row = [
            str(intid),
            obj.portal_type,
            '/'.join(obj.getPhysicalPath()),
            created,
            str([f[1] for f in missing_fields]),
        ]
        self.csv_log.write(';'.join(row) + '\n')

    def update_stats(self, missing_fields):
        if missing_fields:
            self.stats['missing'] += 1

            for schema_name, field_name in missing_fields:
                self.stats['by_field'][(schema_name, field_name)] += 1
        else:
            self.stats['ok'] += 1

    def display_stats(self):

        def log(line):
            sys.stdout.write(line)
            self.summary_log.write(line)

        log("\n")

        log("Missing (by field):\n")
        stats_by_field = sorted(self.stats['by_field'].items())
        for (schema_name, field_name), count in stats_by_field:
            dotted_name = '.'.join((schema_name, field_name))
            log("  %-120s %s\n" % (dotted_name, count))

        log("\n")

        log("Summary (by object):\n")
        log("Missing: %s\n" % self.stats['missing'])
        log("OK: %s\n" % self.stats['ok'])

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

    finder = NonPersistedValueFinder()
    finder.run()
