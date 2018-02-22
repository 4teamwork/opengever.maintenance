"""
List all files with .url extension as a simple CSV.

    bin/instance run ./scripts/list_url_files.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from os.path import splitext
from plone import api


# Possible mimetypes for .url files
URL_MIMETYPES = [
    'text/x-uri',
    'application/internet-shortcut',
    'application/x-url',
    'text/url',
    'text/x-url',
    'application/octet-stream',
]


def dump_csv(obj_list):
    print
    print 'path,filename,url'
    for obj in obj_list:
        path = '/'.join(obj.getPhysicalPath())
        filename = obj.file.filename
        url = obj.absolute_url()
        print '%s,"%s",%s' % (path, filename, url)


def list_url_files():
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')

    url_files = []
    for brain in brains:
        if brain.getContentType in URL_MIMETYPES:
            obj = brain.getObject()
            filename = obj.file.filename
            if splitext(filename)[-1] == u'.url':
                url_files.append(obj)

    dump_csv(url_files)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    list_url_files()
