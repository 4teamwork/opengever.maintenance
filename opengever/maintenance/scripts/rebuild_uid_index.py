"""
Provides functionality to rebuild the UID index as a bin/instance script.

Implementation mostly coped from
https://github.com/4teamwork/ftw.copymovepatches/blob/65df9a04503161ed5df54bd255e0724d25ce4293/ftw/copymovepatches/browser/catalog_fixes.py#L20

    bin/instance run ./scripts/rebuild_uid_index.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.ZCatalog.ProgressHandler import StdoutHandler
import transaction


def rebuild_uid_index(plone):
    portal_catalog = api.portal.get_tool('portal_catalog')
    portal_catalog._catalog.clearIndex('UID')
    portal_catalog._catalog.reindexIndex(
        'UID', plone.REQUEST, pghandler=StdoutHandler())


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if options.dryrun:
        transaction.doom()

    app = setup_app()
    plone = setup_plone(app, options)
    rebuild_uid_index(plone)

    if not options.dryrun:
        transaction.commit()


if __name__ == '__main__':
    main()
