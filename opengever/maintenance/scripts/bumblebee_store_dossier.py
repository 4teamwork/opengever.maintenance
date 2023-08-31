"""
Stores all documents in the given dossier in Bumblebee.

    bin/instance run bumblebee_store_dossier.py -n <dossier_path>

"""
from ftw.bumblebee.interfaces import IBumblebeeable
from ftw.bumblebee.interfaces import IBumblebeeConverter
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import getUtility
import logging
import sys
import transaction


log = logging.getLogger('opengever.maintenance')
log.setLevel(logging.INFO)
log.root.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


def get_documents_to_store(dossier_path):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        object_provides=IBumblebeeable.__identifier__, path=dossier_path)
    return brains


def store_dossier(dossier_path, options):
    brains = get_documents_to_store(dossier_path)
    print("Considering %s documents total" % len(brains))

    for brain in brains:
        print("Considering: %s" % brain.getPath())

    if not options.dryrun:
        converter = getUtility(IBumblebeeConverter)

        # Patch GeverBumblebeeConverter's batch_query so we can limit
        # by date range instead of storing ALL documents
        query = {'object_provides': IBumblebeeable.__identifier__}
        query['path'] = dossier_path
        converter.batch_query = query

        converter.store(deferred=True, reset_timestamp=True)
    return


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if len(args) != 1:
        print("Must supply exactly one argument (<dossier_path>)")
        print("Usage: bin/instance run bumblebee_store_since.py -n <dossier_path>")
        sys.exit(1)

    dossier_path = args[0]
    print("Considering documents in dossier %s" % dossier_path)

    setup_plone(app, options)

    if options.dryrun:
        print('dryrun ...')
        transaction.doom()

    store_dossier(dossier_path, options)

    if not options.dryrun:
        transaction.commit()
