from plone import api
from Products.Five.browser import BrowserView
import transaction


try:
    from ftw.bumblebee.interfaces import IBumblebeeable
    from ftw.bumblebee.interfaces import IBumblebeeDocument
    BUMBLEBEE_AVAILABLE = True
except ImportError:
    BUMBLEBEE_AVAILABLE = False


class CaluclateMissingBumblebeeChecksumsView(BrowserView):
    """
    Calculates Bumblebee checksums for documents that are missing them, and
    also stores those checksums in Bumblebee.

    This may be needed for documents created via the REST API before #4139 was
    deployed.

    Usage:

    /@@calculate-missing-bumblebee-checksums
    (Dry-run)

    /@@calculate-missing-bumblebee-checksums?run=true
    (Actually perform the work)
    """

    def __call__(self):
        if not BUMBLEBEE_AVAILABLE:
            return "Bumblebee not available."

        run = bool(self.request.form.get('run'))
        dryrun = not run

        if dryrun:
            print 'dryrun ...'
            transaction.doom()

        calculate_missing_bumblebee_checksums(dryrun)

        if not dryrun:
            transaction.get().note(
                "Calculate Bumblebee checksums for docs that were "
                "missing them.")
            transaction.commit()

        return "All done (dryrun=%r)" % dryrun


def calculate_missing_bumblebee_checksums(dryrun):
    affected = get_affected_docs()
    for doc in affected:
        url = doc.absolute_url()
        print "Affected: %s" % url

        if not dryrun:
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
