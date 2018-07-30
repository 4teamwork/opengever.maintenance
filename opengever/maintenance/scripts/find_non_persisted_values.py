"""
Script to find field values that haven't been persisted on objects.

    bin/instance run find_non_persisted_values.py > nonpersistent.csv

This script produces a CSV file on STDOUT (and some progress info on STDERR).

"""
from collections import Counter
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
import sys
import transaction


class NonPersistedValueFinder(object):

    CSV_HEADER = "intid;portal_type;path;created;missing_fields"
    SCHEMA_CACHE = {}
    FIELD_CACHE = {}

    def __init__(self):
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)

        self.stats = Counter()

    def run(self):
        sys.stderr.write("Checking for non-persisted values...\n\n")

        all_brains = self.catalog.unrestrictedSearchResults()
        total = len(all_brains)

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
        print ';'.join(row)

    def update_stats(self, missing_fields):
        if missing_fields:
            self.stats['missing'] += 1
        else:
            self.stats['ok'] += 1

    def display_stats(self):
        sys.stderr.write("\n")

        sys.stderr.write("Summary:\n")
        sys.stderr.write("Missing: %s\n" % self.stats['missing'])
        sys.stderr.write("OK: %s\n" % self.stats['ok'])


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    finder = NonPersistedValueFinder()
    finder.run()
