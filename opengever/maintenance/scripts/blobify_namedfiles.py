"""
Script to remove named files and convert them to named blob files. Occasionally
we seem to have some NamedFile instances left. Those should be a NamedBlobFile,
so this script will perform the conversion.

    bin/instance run ./scripts/blobify_namedfiles.py

"""
from opengever.mail.mail import IOGMailMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.namedfile.file import NamedBlobFile
from plone.namedfile.file import NamedFile
from plone.rfc822.interfaces import IPrimaryFieldInfo
import transaction


def as_named_blobfile(named_file):
    return NamedBlobFile(data=named_file.data,
                         contentType=named_file.contentType,
                         filename=named_file.filename)


def find_content_with_namedfile():
    catalog = api.portal.get_tool('portal_catalog')
    query = {'portal_type': ['ftw.mail.mail', 'opengever.document.document']}

    for brain in catalog.unrestrictedSearchResults(**query):
        obj = brain.getObject()

        if IOGMailMarker.providedBy(obj):
            fieldname = 'message'
        else:
            fieldname = 'file'

        val = getattr(obj, fieldname, None)
        if not val:
            continue

        if isinstance(val, NamedFile):
            yield fieldname, obj


def convert_namedfiles_to_namedblobfiles():
    for fieldname, obj in find_content_with_namedfile():
        named_file = getattr(obj, fieldname)
        setattr(obj, fieldname, as_named_blobfile(named_file))

        path = '/'.join(obj.getPhysicalPath())
        print "Converted to NamedBlobFile on {} at: {}".format(
            obj.__class__, path)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    if options.dryrun:
        print 'dryrun ...'
        transaction.doom()

    convert_namedfiles_to_namedblobfiles()

    if not options.dryrun:
        transaction.get().note("Convert NamedFile to NamedBlobFile")
        transaction.commit()
