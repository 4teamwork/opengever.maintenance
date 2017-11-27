from opengever.document.document import IDocumentSchema
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from os.path import splitext
from plone import api
import transaction


def fix_one_note_mimetype():
    """Search all MS OneNote documents and fixes the contentType.
    """

    catalog = api.portal.get_tool('portal_catalog')
    for brain in catalog.unrestrictedSearchResults(
            {'object_provides': IDocumentSchema.__identifier__}):

        if brain.getContentType == 'application/octet-stream':
            obj = brain.getObject()
            if not obj.file:
                continue

            filename, ext = splitext(obj.file.filename)
            if ext == '.one':
                obj.file.contentType = 'application/onenote'
                obj.reindexObject()

                print u'File {} with {} fixed.'.format(obj, obj.file.filename)


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    fix_one_note_mimetype()

    if not options.dry_run:
        transaction.commit()


if __name__ == '__main__':
    main()
