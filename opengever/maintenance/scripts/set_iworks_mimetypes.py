from ftw.upgrade.progresslogger import ProgressLogger
import mimetypes
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from os.path import splitext
from plone import api
import transaction


def set_iworks_mimetype():
    """Search all MS OneNote documents and fixes the contentType.
    """

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')
    # A getIcon value of 'application.png' indicates that the mimetype
    # coulnd't be resolved for this document - only consider these
    brains = filter(lambda b: b.getIcon == 'application.png', brains)
    for brain in ProgressLogger('Set IWorks mimetypes', brains):
        obj = brain.getObject()
        if not obj.file:
            continue
        fname = obj.file.filename
        ext = splitext(fname)[1]
        if ext in [".numbers", ".pages", ".key"]:
            obj.file.contentType = mimetypes.guess_type(fname)[0]
            obj.reindexObject(idxs=['id'])


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    set_iworks_mimetype()

    if not options.dry_run:
        transaction.commit()


if __name__ == '__main__':
    main()
