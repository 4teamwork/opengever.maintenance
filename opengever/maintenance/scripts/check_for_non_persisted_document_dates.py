"""
Script to check for document dates that haven't been persisted because of
the known default value issue.

    bin/instance run check_for_non_persisted_document_dates.py

"""
from collections import Counter
from opengever.base.default_values import get_persisted_value_for_field
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api


def check_for_non_persisted_document_dates(plone):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.document.document', 'ftw.mail.mail'])

    counts = Counter()
    for brain in brains:
        obj = brain.getObject()
        field = IDocumentMetadata['document_date']
        try:
            get_persisted_value_for_field(obj, field)
            counts['persisted'] += 1
        except AttributeError:
            counts['not_persisted'] += 1
            print "Not persisted: (%s) %s" % (
                obj.portal_type, '/'.join(obj.getPhysicalPath()))

    print
    print "Summary:"
    print "Persisted: %s" % counts['persisted']
    print "Not Persisted: %s" % counts['not_persisted']


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    check_for_non_persisted_document_dates(plone)
