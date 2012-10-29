from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
from opengever.dossier.behaviors.dossier import IDossierMarker


SEPARATOR = '-' * 78


def check_dossier_nesting(portal, options):
    """Find all Dossiers that are nested 3 levels or more (sub-sub-dossiers).
    """
    catalog = getToolByName(portal, 'portal_catalog')
    dossiers = catalog(object_provides=IDossierMarker.__identifier__)

    for brain in dossiers:
        obj = brain.getObject()
        subdossiers = [c for c in obj.getChildNodes()
                       if IDossierMarker.providedBy(c)]

        for subdossier in subdossiers:
            if subdossier.get_subdossiers() != []:
                badly_nested_dossiers = [c for c in subdossier.getChildNodes()
                                         if IDossierMarker.providedBy(c)]
                print badly_nested_dossiers


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-p", "--profile-id", action="store", dest="profile_id",
                      default=None)
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    check_dossier_nesting(plone, options)


if __name__ == '__main__':
    main()
