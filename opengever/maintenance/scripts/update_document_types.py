"""
A bin/instance run script for https://4teamwork.atlassian.net/browse/GEVER-791, which updates the document_type.
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from opengever.document.behaviors.metadata import IDocumentMetadata
import transaction


OLD_DOCUMENT_TYPE = u'BOTSCHAFT_BERICHT'
NEW_DOCUMENT_TYPE = u'BOTSCHAFT'


def update_document_types(plone):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(document_type=OLD_DOCUMENT_TYPE)

    print '{} documents with document_type "{}" found.'.format(
        len(brains), OLD_DOCUMENT_TYPE)

    for brain in brains:
        document = brain.getObject()
        IDocumentMetadata(document).document_type = NEW_DOCUMENT_TYPE
        document.reindexObject(idxs=['document_type'])

    print '{} documents updated, "{}" set'.format(len(brains), NEW_DOCUMENT_TYPE)


def main():
    plone = setup_plone(setup_app())
    update_document_types(plone)

    transaction.commit()


if __name__ == '__main__':
    main()
