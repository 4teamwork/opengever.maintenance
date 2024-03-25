from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.propertysheets.storage import PropertySheetSchemaStorage
from plone.restapi.interfaces import ISerializeToJson
from zope.component import getMultiAdapter
import json


def export_propertysheets(site, options):
    storage = PropertySheetSchemaStorage()
    out = []
    for schema_definition in storage.list():
        serializer = getMultiAdapter((schema_definition, site.REQUEST), ISerializeToJson)
        out.append(serializer())

    with open(options.jsonfile, 'w') as jsonfile:
        json.dump(out, jsonfile, indent=2)


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option('-j', dest='jsonfile', default=None,
                      help='path to JSON file with the propertysheets.')
    options, args = parser.parse_args()
    site = setup_plone(app, options)
    export_propertysheets(site, options)


if __name__ == '__main__':
    main()
