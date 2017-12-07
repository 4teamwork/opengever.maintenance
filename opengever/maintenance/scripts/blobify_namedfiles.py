"""
Script to remove named files and convert them to blobs. Occasionally we seem
to have some NamedFile instances lying around. Those should be a NamedBlobFile,
so this script will perform the conversion.

    bin/instance run ./scripts/blobify_namedfiles.py.py

"""

from opengever.mail.mail import IOGMailMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.namedfile.file import NamedFile
from plone.rfc822.interfaces import IPrimaryFieldInfo
import transaction


def find_content_with_namedfile():
    catalog = api.portal.get_tool('portal_catalog')
    query = {'portal_type': ['ftw.mail.mail',
                             'opengever.document.document',
                             'opengever.meeting.proposaltemplate',
                             'opengever.meeting.sablontemplate']}

    brains = catalog.unrestrictedSearchResults(**query)
    for brain in brains:
        obj = brain.getObject()
        if IOGMailMarker.providedBy(obj):
            field = obj.message
        else:
            field = obj.file

        if not field:
            continue

        if isinstance(field, NamedFile):
            yield obj


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    transaction.doom()
    print "DRY-RUN"

    for obj in find_content_with_namedfile():
        path = '/'.join(obj.getPhysicalPath())
        print "Found NamedFile on {} at: {}".format(obj.__class__, path)
