"""
Fixes permissions on documents that have previously been moved out of a
user's private area into the public repository. See opengever.core#4092

    bin/instance run fix_permissions_on_docs_moved_out_of_private_area.py

"""
from ftw.upgrade.helpers import update_security_for
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFCore.CMFCatalogAware import CatalogAware
import transaction


def fix_permissions_for_moved_documents(options):
    catalog = api.portal.get_tool('portal_catalog')

    private_root_paths = [b.getPath() for b in
                          catalog.unrestrictedSearchResults(
                              portal_type='opengever.private.root')]

    # The MemberAreaAdministrator role being in allowedRolesAndUsers is
    # indicative of a document that was moved out of a private area, but
    # didn't have its security updated (and reindexed).
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.document.document', 'ftw.mail.mail'],
        allowedRolesAndUsers='MemberAreaAdministrator')

    for brain in brains:
        if any([brain.getPath().startswith(p) for p in private_root_paths]):
            # Skip documents in private areas, we're interested in ones
            # that have been moved *out* of private areas
            continue

        print "Affected document: %s" % brain.getPath()

        if not options.dryrun:
            obj = brain._unrestrictedGetObject()
            changed = update_security_for(obj, reindex_security=False)
            if changed:
                catalog.reindexObject(
                    obj, idxs=CatalogAware._cmf_security_indexes,
                    update_metadata=0)
            print "Fixed document: %s" % brain.getPath()


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

    fix_permissions_for_moved_documents(options)

    if not options.dryrun:
        transaction.get().note(
            "Fix permissions on docs moved out of private area")
        transaction.commit()
