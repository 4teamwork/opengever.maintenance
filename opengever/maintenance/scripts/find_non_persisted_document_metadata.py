"""
Script to find documents with non-persisted metadata.

    bin/instance run find_non_persisted_document_metadata.py

"""
from collections import Counter
from opengever.base.default_values import get_persisted_values_for_obj
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


# This list contains a couple fields that are omitted from the form. They
# therefore aren't always present on the objects, depending on how they
# got created (add menu vs. quickupload).
DOC_METADATA_FIELDS = [
    'archival_file',
    # 'archival_file_state',  # omitted
    'classification',
    'delivery_date',
    'description',
    'digitally_available',
    'document_author',
    'document_date',
    'document_type',
    'foreign_reference',
    'keywords',
    'preserved_as_paper',
    # 'preview',  # omitted
    'privacy_layer',
    'public_trial',
    'public_trial_statement',
    'receipt_date',
    # 'thumbnail',  # omitted
]

DOCUMENT_FIELDS = ['file', 'title', 'relatedItems']

# The 'message' and 'title' fields currently can't be handled correcly by
# get_persisted_value_for_field() because they are properties on the storage
# implementation that proxy to a different attribute.
MAIL_FIELDS = []

EXPECTED_FIELDS = {
    'opengever.document.document': DOC_METADATA_FIELDS + DOCUMENT_FIELDS,
    'ftw.mail.mail': DOC_METADATA_FIELDS + MAIL_FIELDS,
}


def find_non_peristed_document_metadata(plone, options):
    print "Checking for non-persisted metadata..."
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.document.document', 'ftw.mail.mail'])

    counts = Counter()
    for i, brain in enumerate(brains):
        if i % 100 == 0:
            print "Progress: %s of %s objects" % (i, len(brains))

        obj = brain.getObject()
        persisted_field_names = get_persisted_values_for_obj(obj).keys()

        expected = EXPECTED_FIELDS[obj.portal_type]
        missing = set(expected) - set(persisted_field_names)
        if missing:
            created = str(obj.created())
            counts['missing'] += 1
            print "Object %r (created: %s) has missing metadata: %r" % (
                obj.absolute_url(), created, sorted(list(missing)))
        else:
            counts['ok'] += 1

    print
    print "Summary:"
    print "Missing: %s" % counts['missing']
    print "OK: %s" % counts['ok']


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    find_non_peristed_document_metadata(plone, options)
