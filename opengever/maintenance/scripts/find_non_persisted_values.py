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


SCHEMA_CACHE = {}
FIELD_CACHE = {}

CSV_HEADER = "intid;path;created;missing_fields"


def find_non_peristed_values(plone, options):
    sys.stderr.write("Checking for non-persisted values...\n\n")

    catalog = api.portal.get_tool('portal_catalog')
    intids = getUtility(IIntIds)

    all_brains = catalog.unrestrictedSearchResults()
    total = len(all_brains)

    counts = Counter()
    for i, brain in enumerate(all_brains):
        obj = brain.getObject()
        missing_fields = check_for_missing_fields(obj)

        if missing_fields:
            counts['missing'] += 1
            missing_fields.sort()
            created = str(obj.created())
            intid = intids.queryId(obj)
            row = [
                str(intid),
                '/'.join(obj.getPhysicalPath()),
                created,
                str(missing_fields),
            ]
            print ';'.join(row)

        else:
            counts['ok'] += 1

        if i % 100 == 0:
            sys.stderr.write("Progress: %s of %s objects\n" % (i, total))

    sys.stderr.write("\n")
    sys.stderr.write("Summary:\n")
    sys.stderr.write("Missing: %s\n" % counts['missing'])
    sys.stderr.write("OK: %s\n" % counts['ok'])


def check_for_missing_fields(obj):
    missing_fields = []
    portal_type = obj.portal_type

    if portal_type not in SCHEMA_CACHE:
        SCHEMA_CACHE[portal_type] = list(iterSchemataForType(portal_type))
    schemas = SCHEMA_CACHE[portal_type]

    for schema in schemas:
        if schema.__identifier__ not in FIELD_CACHE:
            FIELD_CACHE[schema.__identifier__] = map(
                itemgetter(1), getFieldsInOrder(schema))
        fields = FIELD_CACHE[schema.__identifier__]

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
                missing_fields.append(name)

    return missing_fields


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    find_non_peristed_values(plone, options)
