from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
import transaction


def fix_wrong_mime_types(portal, wrong_mimetype, fix):
    """ Fixes mimetypes of files in plone objects.

    'wrong_mimetype' is the mimetype the file currenty has. It will be
    replaced by the value looked up in the plone/python mimetype registry.
    """
    for obj in get_objects_by_mimetype(portal, wrong_mimetype):
        registry_type = lookup_mimetype(portal, obj.file.filename).mimetypes[0]

        if registry_type and registry_type != obj.file.contentType:
            if not fix:
                print "%s: %s" % (
                    registry_type.ljust(30),
                    obj.file.filename)
            else:
                obj.file.contentType = registry_type

    transaction.commit()


def get_objects_by_mimetype(portal, specific_type):
    """ Returns a list with plone objects containing files.

    The list is filtered by the mimetype of the containing file
    specified by 'specific_type'.
    """
    result = []
    catalog = portal.portal_catalog
    queryresult = catalog(portal_type='opengever.document.document')

    for brain in queryresult:
        obj = brain.getObject()
        if obj.file:
            if obj.file.contentType == specific_type:
                result.append(obj)

    return result


def lookup_mimetype(portal, filename):
    """ Looks up mimetype to a given filename.

    It will check the plone mimetype registry and
    fall back to the python mimetype module if needed.
    Returns None if lookup fails.
    """
    registry = getToolByName(portal, 'mimetypes_registry')
    type_from_registry = registry.lookupExtension(filename)
    if not type_from_registry:
        type_from_registry = registry.globFilename(filename)
    if not type_from_registry:
        return None
    return type_from_registry


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-f", "--fix", action="store_true", dest="fix",
                      help="""Flags whether the script should fix the
                      "types or just display them.""")
    parser.add_option("-t", "--mimetype", dest="specific_type",
                      help="Search for the given MIMETYPE.")
    (options, args) = parser.parse_args()
    plone = setup_plone(app, options)

    if not options.specific_type:
        print "No MIMETYPE given."
        return

    fix_wrong_mime_types(plone, options.specific_type, options.fix)

if __name__ == '__main__':
    main()
