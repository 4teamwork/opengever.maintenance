from Acquisition import aq_parent
from opengever.base.interfaces import IReferenceNumberPrefix
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
from zope.annotation import IAnnotations


SEPARATOR = '-' * 78

DOSSIER_KEY = 'dossier_reference_mapping'
PREFIX_REF_KEY = 'reference_prefix'


def check_for_reference_numbers(portal, options):
    """Find all Dossiers that are missing a reference number.
    """
    catalog = getToolByName(portal, 'portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        object_provides=IDossierMarker.__identifier__)

    for brain in brains:
        dossier = brain.getObject()
        parent = aq_parent(dossier)
        ann = IAnnotations(parent)

        if DOSSIER_KEY not in ann:
            print "Key '{}' not found in annotations for {}".format(
                DOSSIER_KEY, parent.absolute_url())
            continue

        mapping = ann[DOSSIER_KEY]
        if PREFIX_REF_KEY not in mapping:
            print "Key '{}' not found in annotations for {}".format(
                PREFIX_REF_KEY, parent.absolute_url())

        number = IReferenceNumberPrefix(parent).get_number(dossier)
        if number is None:
            print "No reference number for object {}".format(
                dossier.absolute_url())


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    check_for_reference_numbers(plone, options)


if __name__ == '__main__':
    main()
