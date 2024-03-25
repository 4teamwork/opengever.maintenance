from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.propertysheets.definition import PropertySheetSchemaDefinition
from opengever.propertysheets.storage import PropertySheetSchemaStorage
import json
import transaction


def import_propertysheets(site, options):
    if options.dry_run:
        transaction.doom()

    storage = PropertySheetSchemaStorage()
    if options.clear_storage:
        storage.clear()

    with open(options.jsonfile, 'r') as jsonfile:
        sheets = json.load(jsonfile)

    for sheet in sheets:
        schema_definition = create_property_sheet(
            sheet['id'], sheet['assignments'], sheet['fields'])
        storage.save(schema_definition)

    if not options.dry_run:
        transaction.commit()


def create_property_sheet(sheet_id, assignments, fields):
    docprops = [field['name'] for field in fields if field.get('available_as_docproperty')]
    schema_definition = PropertySheetSchemaDefinition.create(
        sheet_id,
        assignments=assignments,
        docprops=docprops,
    )

    for field_data in fields:
        name = field_data['name']
        field_type = field_data['field_type']
        title = field_data.get('title', name.decode('ascii'))
        description = field_data.get('description', u'')
        required = field_data.get('required', False)

        kwargs = {
            'values': field_data.get('values'),
            'default': field_data.get('default'),
            'default_factory': field_data.get('default_factory'),
            'default_expression': field_data.get('default_expression'),
            'default_from_member': field_data.get('default_from_member'),
        }

        schema_definition.add_field(
            field_type, name, title, description, required,
            **kwargs
        )
    return schema_definition


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("-d", dest="clear_storage", action="store_true", default=False)
    parser.add_option('-j', dest='jsonfile', default=None,
                      help='path to JSON file with the propertysheets.')
    options, args = parser.parse_args()
    site = setup_plone(app, options)
    import_propertysheets(site, options)


if __name__ == '__main__':
    main()
