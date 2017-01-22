"""
Set document types not contained in vocabulary to `None`.
"""

from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import getUtility
from zope.intid.interfaces import IIntIds
import transaction


SEPARATOR = '-' * 78


def fix_invalid_document_types(portal):
    catalog = api.portal.get_tool('portal_catalog')
    int_ids = getUtility(IIntIds)
    field = IDocumentMetadata['document_type']

    document_brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')
    for brain in document_brains:
        obj = brain.getObject()
        voc = field.vocabulary(obj)
        term_values = [term.value for term in voc.vocab._terms]
        dt = obj.document_type

        if dt is not None and dt not in term_values:
            intid = int_ids.getId(obj)
            path = '/'.join(obj.getPhysicalPath())
            msg = ("IntID %-12s -  %-20r -> None - %s" % (intid, dt, path))
            print msg
            obj.document_type = None


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    fix_invalid_document_types(plone)
    transaction.commit()


if __name__ == '__main__':
    main()
