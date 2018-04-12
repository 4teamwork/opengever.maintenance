"""
Calculates Bumblebee checksums for documents that are missing them, and
also stores those checksums in Bumblebee.

This may be needed for documents created via the REST API before #4139 was
deployed.

    bin/instance run calculate_missing_bumblebee_checksums.py

"""
from ftw.bumblebee.interfaces import IBumblebeeable
from ftw.bumblebee.interfaces import IBumblebeeDocument
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


def calculate_missing_bumblebee_checksums(options):
    affected = get_affected_docs()
    for doc in affected:
        url = doc.absolute_url()
        print "Affected: %s" % url

        if not options.dryrun:
            print "Calculating checksum and storing: %s" % url
            # Force is not needed for this script because _handle_update
            # checks for a change in checksum. This is what we expect,
            # otherwise storing of the document *should* actually be skipped
            IBumblebeeDocument(doc)._handle_update(force=False)


def get_affected_docs():
    print "Gathering affected documents..."
    affected = []

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')

    for brain in brains:
        if brain.bumblebee_checksum is not None:
            # Only documents missing a checksum should be considered
            continue

        doc = brain._unrestrictedGetObject()

        if not IBumblebeeable.providedBy(doc):
            continue

        if doc.file is None:
            # Only consider documents that actually have a file
            continue

        affected.append(doc)

    print "Done."
    return affected

if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    if options.dryrun:
        print 'dryrun ...'
        transaction.doom()

    calculate_missing_bumblebee_checksums(options)

    if not options.dryrun:
        transaction.get().note(
            "Calculate Bumblebee checksums for docs that were missing them.")
        transaction.commit()
