"""
This script fixes inconsistencies between the Plone Lexicon and ZCTextIndexes
as discovered by check_for_rid_key_errors.py.

The affected indices are document_author and searchable_filing_no. Assumes that
the `opengever.dossier:filing` profile is installed.

It seems that the only reliable way to fix the broken indices is to first CLEAR
and then rebuild them.
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.ZCatalog.ProgressHandler import StdoutHandler
import transaction


SEPARATOR = '-' * 78


AFFECTED_INDICES = ("document_author", "searchable_filing_no",)


def fix_broken_indices(portal, options):
    catalog = api.portal.get_tool('portal_catalog')
    print "Dropping indices ..."
    catalog.manage_clearIndex(AFFECTED_INDICES)
    print "done"

    for name in AFFECTED_INDICES:
        print "Reindexing index {} ...".format(name)

        progress_treshold = catalog._getProgressThreshold() or 100
        progress_handler = StdoutHandler(progress_treshold)
        catalog.reindexIndex(name, None, pghandler=progress_handler)

        print "done"

    transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    fix_broken_indices(plone, options)


if __name__ == '__main__':
    main()
