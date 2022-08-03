#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Script to add propertysheets
# bin/instance run opengever.maintenance/opengever/maintenance/scripts/add_propertysheets.py -j opengever.maintenance/opengever/maintenance/scripts/example_propertysheets.json
#
# -D option clears propertysheet storage completly

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.propertysheets.definition import PropertySheetSchemaDefinition
from opengever.propertysheets.storage import PropertySheetSchemaStorage
import logging
import sys
import transaction
import json


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


def add_propertysheets(filepath, clear_storage=False):
    storage = PropertySheetSchemaStorage()

    if clear_storage:
        storage.clear()

    with open(filepath) as sheets_file:
        sheets = json.load(sheets_file)

    for sheet in sheets:
        schema_definition = PropertySheetSchemaDefinition.create(
            sheet['id'], assignments=sheet['assignments'])

        for field_data in sheet['fields']:
            name = field_data["name"]
            field_type = field_data["type"]

            label = field_data.get("label", name)
            if not isinstance(label, unicode):
                label = label.decode('utf-8')

            description = field_data.get("description", u"")
            required = field_data.get("required", False)
            default = field_data.get("default", None)
            if default:
                if not isinstance(default, unicode):
                    value = default.decode('utf-8')

            values = []

            for value in field_data.get("values", []):
                if not isinstance(value, unicode):
                    value = value.decode('utf-8')

                values.append(value)

            schema_definition.add_field(
                field_type, name, label, description, required,
                values, default=default)

        storage.save(schema_definition)


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("-D", dest="clear_storage", action="store_true", default=False)
    parser.add_option('-j', dest='json_file', default=None,
                      help='path to JSON file with the propertysheets.')
    options, args = parser.parse_args()

    if options.dry_run:
        logger.warn('transaction doomed because we are in dry-mode.')
        transaction.doom()

    setup_plone(app, options)
    add_propertysheets(options.json_file, options.clear_storage)

    if options.dry_run:
        logger.warn('skipping commit because we are in dry-mode.')
    else:
        transaction.commit()
        logger.info('done.')


if __name__ == '__main__':
    main()
