from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
import transaction


def list_objects_with_specific_mime_types(portal, specific_type):
    """ Lists all plone objects containing a file with the given mimetype.

    Also mentiones the mimetype according to the plone registry.
    """
    print "List of objects with wrong mimetype '%s':" % specific_type
    for obj in get_objects_by_mimetype(portal, specific_type):
        lookuped_mimetype = lookup_mimetype(portal, obj.file.filename)
        print "Looked up mimetype '%s' for %s (correct: %s)" % \
            (obj.file.contentType,
             '/'.join(obj.getPhysicalPath()),
             lookuped_mimetype)


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


def fix_wrong_mime_types(portal, wrong_type, expected_type=None):
    """ Fixes mimetypes of files in plone objects.

    'wrong_type' is the mimetype the file currenty has. It will be
    replaced by 'expected_type'. If 'expected_type' is not specified,
    the correct value according to the plone registry will be used.
    """
    for obj in get_objects_by_mimetype(portal, wrong_type):
        if not expected_type:
            expected_type = lookup_mimetype(portal, obj.file.filename).mimetypes[0]
        old_type = obj.file.contentType
        obj.file.contentType = expected_type
        print "Fixed mimetype on %s (%s). '%s' is now '%s'" % \
            ('/'.join(obj.getPhysicalPath()),
             obj.file.filename,
             old_type,
             obj.file.contentType)
    transaction.commit()


def lookup_mimetype(portal, filename):
    """ Looks up mimetype to a given filename.

    It will check the plone mimetype registry and
    fall back to the python mimetype module if needed.
    """
    registry = getToolByName(portal, 'mimetypes_registry')
    type_from_registry = registry.lookupExtension(filename)
    if not type_from_registry:
        type_from_registry = registry.globFilename(filename)
    if not type_from_registry:
        raise Exception("Cannot parse mimetype for %s." % filename)
    return type_from_registry


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-f", "--fix", action="store_true", dest="fix",
        help="Flags whether the script should fix or just display the wrong mimetypes.")
    parser.add_option("-t", "--mimetype", dest="specific_type",
        help="Search for the given MIMETYPE.")
    (options, args) = parser.parse_args()
    plone = setup_plone(app, options)

    if not options.specific_type:
        print "No MIMETYPE given."
        return

    if not options.fix:
        list_objects_with_specific_mime_types(plone, options.specific_type)
    else:
        fix_wrong_mime_types(plone, options.specific_type)

if __name__ == '__main__':
    main()
