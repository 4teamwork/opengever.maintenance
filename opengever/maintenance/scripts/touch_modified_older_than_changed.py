"""
Touch items in catalog where 'changed' is newer than 'modified'.

    bin/instance run touch_modified_older_than_changed.py [--dry-run]

"""

from datetime import timedelta
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from pytz import timezone
import transaction
from collections import Counter


TOLERANCE = timedelta(seconds=5)
ZURICH = timezone('Europe/Zurich')


def commit():
    print('Committing transaction...')
    transaction.commit()
    print('Done.')


def touch_modifed_older_than_changed(options):
    print("Checking catalog for objects that have a 'changed' timestamp "
          "that that is newer than the 'modified' timestamp...\n")

    catalog = api.portal.get_tool('portal_catalog')

    brains = catalog.unrestrictedSearchResults(sort_on='path')
    total_items = len(brains)

    found_total = 0
    found_by_type = Counter()

    for brain in brains:
        changed = brain.changed
        modified = brain.modified.asdatetime()

        if not changed:
            continue

        if changed > (modified + TOLERANCE):
            found_total += 1
            found_by_type[brain.portal_type] += 1

            print('%s (%s)' % (brain.getPath(), brain.Title))
            print('modified: %s' % modified.astimezone(ZURICH))
            print('changed : %s' % changed.astimezone(ZURICH))
            print('')

            if not options.dry_run:
                obj = brain.getObject()
                obj.setModificationDate()
                # Don't use obj.reindexObject() to avoid triggering a reindex
                # of 'modified' in Solr as well, which would defeat the purpose.
                catalog.reindexObject(obj, idxs=['modified'])
                if options.intermediate_commit and not options.dry_run:
                    if found_total % options.intermediate_commit == 0:
                        commit()

    if not options.dry_run:
        print("Touched 'modified' for %s items" % found_total)

    print('Found %s brains where changed is younger than modified:' % found_total)
    for portal_type, count in found_by_type.items():
        print('%-40s : %s' % (portal_type, count))
    print('')
    print('(Out of %s total objects in catalog)' % total_items)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true", dest="dry_run", default=False)
    parser.add_option("-i", "--intermediate-commit", dest="intermediate_commit",
                      default=None, type="int",
                      help="Intermediate commit every n processed elements. ")

    (options, args) = parser.parse_args()

    if options.dry_run:
        print('Dry-run...')
        transaction.doom()

    site = setup_plone(app, options)

    touch_modifed_older_than_changed(options)

    if not options.dry_run:
        commit()
