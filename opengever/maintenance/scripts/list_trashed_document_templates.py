from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api


def list_trashed_document_templates():
    catalog = api.portal.get_tool("portal_catalog")
    template_folder_paths = [
        each.getPath() for each
        in catalog.unrestrictedSearchResults(
            portal_type='opengever.dossier.templatefolder')
    ]
    results = catalog.unrestrictedSearchResults(
        trashed=True,
        object_provides='opengever.document.behaviors.IBaseDocument',
        path={'query': template_folder_paths},
    )

    print "Trashed documents: {}".format(len(results))
    print ""
    if results:
        for brain in results:
            print brain.getPath()
        print ""


def main():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    app = setup_app()
    setup_plone(app)

    list_trashed_document_templates()


if __name__ == '__main__':
    main()
