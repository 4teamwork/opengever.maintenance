"""Export a subtree as an OGGBundle.

Usage:

export_subtree_bundle.py
  (--with-local-roles | --without-local-roles)
  (--dossiers-with-parent-reference | --dossiers-with-parent-guid)
  <path>
"""

from Acquisition import aq_inner
from Acquisition import aq_parent
from collections import defaultdict
from collections import OrderedDict
from datetime import datetime
from jsonschema import FormatChecker
from jsonschema import validate
from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.base.interfaces import IReferenceNumber
from opengever.base.schemadump.config import SHORTNAMES_BY_ROLE
from opengever.bundle.loader import BUNDLE_JSON_TYPES
from opengever.bundle.loader import PORTAL_TYPES_TO_JSON_NAME
from opengever.dossier.behaviors.participation import IParticipationAware
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.trash.trash import ITrashed
from operator import itemgetter
from os.path import join as pjoin
from os.path import splitext
from pkg_resources import resource_filename as rf
from plone import api
from plone.dexterity.utils import iterSchemata
from plone.namedfile.field import NamedBlobFile
from z3c.relationfield.schema import RelationList
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
from zope.schema import Date
from zope.schema import Datetime
from zope.schema import getFieldsInOrder
import argparse
import codecs
import errno
import json
import os
import shutil
import sys
import transaction


SUPPORTED_TYPES = [
    'opengever.repository.repositoryroot',
    'opengever.repository.repositoryfolder',
    'opengever.dossier.businesscasedossier',
    'opengever.task.task',  # partially supported - check implementation
    'opengever.document.document',
    'ftw.mail.mail',
]

TYPES_WITHOUT_PERMISSIONS = [
    'opengever.document.document',
    'ftw.mail.mail',
]

FIELDS_OMITTED_FROM_EXPORT = {
    'opengever.dossier.businesscasedossier': [
        'relatedDossier',  # relationfields are not supported in import yet
        'dossier_manager',
        'temporary_former_reference_number',
        'reading',
        'reading_and_writing',
        'dossier_type',  # not supported by JSON schema yet for custom types
    ],
    'opengever.document.document': [
        'relatedItems',  # relationfields are not supported in import yet
        'changeNote',
        'archival_file_state',
        'digitally_available',
        'archival_file',
        'preview',
        'thumbnail',
    ],
    'ftw.mail.mail': [
        'archival_file',
        'archival_file_state',
        'changeNote',
        'digitally_available',
        'message_source',
        'preview',
        'thumbnail',
    ],
}

PROPERTIES_NOT_REQUIRED = {
    'opengever.dossier.businesscasedossier': [
        '_participations',
        '_old_paths',
        'dossier_type',
        'sequence_number',
        'relatedDossier',
    ],
    'opengever.repository.repositoryroot': [
        '_id',
        '_old_paths',
    ],
    'opengever.repository.repositoryfolder': [
        '_old_paths',
    ],
    'opengever.document.document': [
        '_old_paths',
        'sequence_number',
        'relatedItems',
        'original_message_path',
    ],
    'ftw.mail.mail': [
        '_old_paths',
        'relatedItems',  # mails don't have relatedItems
        'sequence_number',
    ],
}


SKIPPED_FIELDS_NOT_TO_REPORT = [
    'changeNote',
    'digitally_available',
    'message_source',
    'reading',
    'reading_and_writing',
    'temporary_former_reference_number',
]


class SubtreeBundleExporter(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.request = getRequest()
        self.json_schemas = self.load_schemas()
        alsoProvides(self.request, IOpengeverBaseLayer)

    def run(self):
        root_node_path = self.options.root_node_path
        bundle_dir = self.create_bundle_dir()

        print('Exporting subtree %r...' % root_node_path)
        root_node = self.portal.unrestrictedTraverse(root_node_path)

        serializer = SubtreeBundleSerializer(self.options)
        items_by_type = serializer.serialize_subtree(root_node)
        skipped_data = serializer.skipped_data

        self.validate_completeness(items_by_type)

        items_by_json_name = self.group_by_json_name(items_by_type)
        self.validate_schemas(items_by_json_name)
        self.copy_files(items_by_json_name, bundle_dir)
        self.write_bundle(items_by_json_name, bundle_dir)

        print
        print(
            'NOTE: The following data (fields or entire objects) was '
            'skipped during export:'
        )
        print
        for label, paths in skipped_data.items():
            print(label)
            for path in paths:
                print('  %s' % path)
            print

        print
        print('Done.')
        print('Bundle exported to %s' % bundle_dir)
        print

        print(
            'NOTE: You must now edit the bundle and correctly set up \n'
            'a parent reference for the item with the guid "TO_BE_DEFINED" \n'
            'before attempting to import it.'
        )

    def create_bundle_dir(self):
        output_dir = self.options.output_dir
        self.mkdir_p(output_dir)

        ts = datetime.now().strftime('%Y-%d-%m_%H_%M_%S')
        bundle_dir = pjoin(output_dir, 'export-%s.oggbundle' % ts)
        self.mkdir_p(bundle_dir)
        return bundle_dir

    def group_by_json_name(self, items_by_type):
        items_by_json_name = defaultdict(list)

        for portal_type, items in items_by_type.items():
            json_name = PORTAL_TYPES_TO_JSON_NAME[portal_type]

            items_by_json_name[json_name].extend(items)

        # Mails and documents got merged into documents.json - sort them again
        for items in items_by_json_name.values():
            items.sort(key=itemgetter('guid'))

        return items_by_json_name

    def validate_schemas(self, items_by_json_name):
        for json_name, items in items_by_json_name.items():
            schema = self.json_schemas[json_name]

            # Prevent our temporary meta infos from failing validation
            if json_name == 'documents.json':
                props = schema['definitions']['document']['properties']
                props['_source_file_path'] = {'type': ['null', 'string']}
                props['_source_file_filename'] = {'type': ['null', 'string']}
                props['_source_original_message_path'] = {'type': ['null', 'string']}
                props['_source_original_message_filename'] = {'type': ['null', 'string']}

            if json_name == 'dossiers.json':
                props = schema['definitions']['dossier']['properties']
                props['_no_completeness_validation'] = {'type': 'boolean'}

            validate(items, schema, format_checker=FormatChecker())

    def copy_files(self, items_by_json_name, bundle_dir):
        documentish_items = items_by_json_name['documents.json']

        bundle_files_dir = pjoin(bundle_dir, 'files')
        self.mkdir_p(bundle_files_dir)

        for file_no, item in enumerate(documentish_items):
            src_path = item.pop('_source_file_path')
            src_filename = item.pop('_source_file_filename')
            src_msg_path = item.pop('_source_original_message_path', None)
            src_msg_filename = item.pop('_source_original_message_filename', None)

            if not (src_path or src_msg_path):
                # Document without file (or mail without original)
                continue

            # Document with file or mail
            dst_filename = self.copy_file(
                src_path, src_filename, bundle_files_dir, file_no)
            item['filepath'] = 'files/%s' % dst_filename

            if src_msg_path:
                # Mail with original_message
                dst_msg_filename = self.copy_file(
                    src_msg_path, src_msg_filename, bundle_files_dir, file_no)
                item['original_message_path'] = 'files/%s' % dst_msg_filename

    def copy_file(self, src_path, src_filename, bundle_files_dir, file_no):
        ext = splitext(src_filename)[-1]
        dst_filename = 'file_%s%s' % (file_no, ext)
        destination_path = pjoin(bundle_files_dir, dst_filename)

        print('Copying %s to %s' % (src_path, destination_path))
        shutil.copy2(src_path, destination_path)
        return dst_filename

    def write_bundle(self, items_by_json_name, bundle_dir):
        for json_name, items in items_by_json_name.items():
            json_path = pjoin(bundle_dir, json_name)
            self.dump_to_jsonfile(items, json_path)

    def validate_completeness(self, items_by_type):
        for portal_type, items in items_by_type.items():
            self.assert_all_supported_properties_exported(items, portal_type)

    def assert_all_supported_properties_exported(self, items, portal_type):
        json_name = PORTAL_TYPES_TO_JSON_NAME[portal_type]
        schema = self.json_schemas[json_name]

        all_props = []
        for def_name, definition in schema['definitions'].items():
            if def_name == 'permission':
                continue
            for prop_name in definition['properties']:
                all_props.append(prop_name)

        not_required = PROPERTIES_NOT_REQUIRED.get(portal_type, [])
        for item in items:
            no_completeness_validation = item.pop('_no_completeness_validation', False)
            if no_completeness_validation:
                continue

            for prop_name in all_props:
                if prop_name in not_required or prop_name == '_no_completeness_validation':
                    continue

                if prop_name == '_permissions' and not self.options.with_local_roles:
                    continue

                if prop_name in ('parent_guid', 'parent_reference'):
                    # These are already required by validation anyway
                    continue

                if prop_name not in item:
                    msg = (
                        "Property %r, which is supported by the "
                        "bundle's schema for %s, is missing for item "
                        "of type %s. Make sure it's getting exported "
                        "or add it to PROPERTIES_NOT_REQUIRED." % (
                            prop_name, json_name, portal_type)
                    )
                    raise Exception(msg)

    def dump_to_jsonfile(self, data, json_path):
        with open(json_path, 'wb') as jsonfile:
            json.dump(
                data,
                codecs.getwriter('utf-8')(jsonfile),
                ensure_ascii=False,
                indent=4,
                separators=(',', ': ')
            )

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def load_schemas(self):
        schema_dir = rf('opengever.bundle', 'schemas/')
        schemas = {}
        filenames = os.listdir(schema_dir)
        for schema_filename in filenames:
            short_name = schema_filename.replace('.schema.json', '')
            if '%s.json' % short_name in BUNDLE_JSON_TYPES:
                schema_path = os.path.join(schema_dir, schema_filename)

                with codecs.open(schema_path, 'r', 'utf-8-sig') as schema_file:
                    schema = json.load(schema_file)
                schemas['%s.json' % short_name] = schema
        return schemas


class SubtreeBundleSerializer(object):

    def __init__(self, options):
        self.options = options
        self.skipped_data = defaultdict(list)

    def serialize_subtree(self, root_node):
        serialized_nodes_by_type = defaultdict(list)
        self.serialize_node(root_node, serialized_nodes_by_type)
        return serialized_nodes_by_type

    def serialize_node(self, node, serialized_nodes_by_type, parent_guid=None):
        path = '/'.join(node.getPhysicalPath())
        portal_type = node.portal_type

        if node.portal_type not in SUPPORTED_TYPES:
            if self.options.skip_unsupported_types:
                self.skipped_data['Skipped unsupported type'].append(
                    '{} ({})'.format(path, portal_type)
                )
                return
            raise Exception('Unable to export object %r. Export of type %r is not '
                            'supported.' % (path, portal_type))
        data = {}

        # guid and, for non-root nodes, parent_guid or parent_reference
        guid = path
        data['guid'] = guid

        dossiers_with_parent_reference = self.options.dossiers_with_parent_reference
        is_dossier = portal_type == 'opengever.dossier.businesscasedossier'

        if is_dossier and dossiers_with_parent_reference:
            parent = aq_parent(aq_inner(node))
            if parent.portal_type == 'opengever.repository.repositoryfolder':
                numbers = IReferenceNumber(parent).get_numbers()['repository']
                numbers = map(int, numbers)
                data['parent_reference'] = [numbers]
            else:
                # Subdossier - reference main dossier by GUID
                data['parent_guid'] = parent_guid

        else:
            if parent_guid is not None:
                data['parent_guid'] = parent_guid
            else:
                # Add a fake parent_guid to root node in order to pass
                # validation. Needs to be replaced before importing bundle.
                data['parent_guid'] = 'TO_BE_DEFINED'

            if portal_type == 'opengever.repository.repositoryroot':
                # This field does not exist for the repositoryroot
                del data['parent_guid']

        data.update(self.serialize_review_state(node))
        data.update(self.serialize_creator(node))

        # Behaviors
        data.update(self.serialize_field_data(node))

        # Satisfy schema validation.
        #
        # These will either get updated by the step that copies the actual
        # blobs, or left empty (to create documents without a file).
        if portal_type == 'opengever.document.document':
            data['filepath'] = ''

        if portal_type == 'ftw.mail.mail':
            data['filepath'] = ''
            data['original_message_path'] = ''

        if portal_type not in TYPES_WITHOUT_PERMISSIONS:
            if self.options.with_local_roles:
                data['_permissions'] = self.serialize_local_roles(node)

        if portal_type == 'opengever.dossier.businesscasedossier':
            old_reference = node.get_reference_number()
            data['former_reference_number'] = old_reference

        if portal_type != 'opengever.task.task':
            # Regular case - include the node in list of serialized nodes...
            serialized_nodes_by_type[portal_type].append(self.order_dict(data))

            # ... and recurse over children, with this node's guid as the
            # parent_guid, so the children get parented to it.
            for child_id in sorted(node.objectIds()):
                child = node[child_id]
                if self.should_skip_child(child):
                    continue
                self.serialize_node(child, serialized_nodes_by_type, parent_guid=guid)

        else:
            # Special case - it's a task
            #
            # We don't want to serialize tasks because we don't support their
            # import yet. However, we still want to include documents contained
            # in them.
            # So we don't add the task to the list of serialized nodes, and
            # we parent the children to the *task's parent* (i.e. dossier).
            self.skipped_data['Tasks (except contained docs)'].append(path)
            for child_id in sorted(node.objectIds()):
                child = node[child_id]
                if self.should_skip_child(child):
                    continue
                self.serialize_node(child, serialized_nodes_by_type, parent_guid=parent_guid)

    def should_skip_child(self, child):
        # Don't export inactive dossiers
        review_state = api.content.get_state(child)
        if review_state == 'dossier-state-inactive':
            self.skipped_data['Inactive Dossiers'].append(
                '/'.join(child.getPhysicalPath()))
            return True

        # Don't export trashed objects
        review_state = api.content.get_state(child)
        if ITrashed.providedBy(child):
            self.skipped_data['Trashed objects'].append(
                '/'.join(child.getPhysicalPath()))
            return True

        return False

    def serialize_review_state(self, obj):
        if obj.portal_type == 'ftw.mail.mail':
            # The WF state 'mail-state-active' is not currently allowed per the
            # JSON schema for documents.json. And since documents and mails
            # have a one-state workflow anyway, any review_state value for them
            # gets ignored by the bundle loader anyway. But since the
            # 'review_state' property is actually required, just pretend it's
            # a document state.
            return {'review_state': 'document-state-draft'}

        return {'review_state': api.content.get_state(obj)}

    def serialize_field_data(self, obj):
        data = {}
        portal_type = obj.portal_type

        for schema in iterSchemata(obj):
            for name, field in getFieldsInOrder(schema):

                value = field.get(field.interface(obj))

                if name in FIELDS_OMITTED_FROM_EXPORT.get(portal_type, []):
                    should_report = name not in SKIPPED_FIELDS_NOT_TO_REPORT
                    if value is not None and should_report:
                        path = '/'.join(obj.getPhysicalPath())
                        self.skipped_data['Field: %s' % name].append(path)
                    continue

                if value is not None:
                    if isinstance(field, (Date, Datetime)):
                        value = self.iso_datestr(value)

                    if isinstance(field, RelationList):
                        value = [rv.to_id for rv in value]

                    if isinstance(field, NamedBlobFile):
                        if name in ('file', 'message'):
                            # Docs and Mails - primary file field
                            #
                            # Track blobs to copy as internal meta infos on the
                            # items. They will be copied in a later step, and then
                            # referenced in 'filepath' / 'original_message_path'.
                            data['_source_file_path'] = value._blob.committed()
                            data['_source_file_filename'] = value.filename

                        if name == 'original_message':
                            # Mail - secondary file field with original *.msg
                            data['_source_original_message_path'] = value._blob.committed()
                            data['_source_original_message_filename'] = value.filename

                        continue

                    if isinstance(value, tuple):
                        value = list(value)

                if name == 'file' and value is None:
                    # Document without file
                    data['_source_file_path'] = None
                    data['_source_file_filename'] = None
                    continue

                if name == 'original_message' and value is None:
                    # Mail without original message
                    data['_source_original_message_path'] = None
                    data['_source_original_message_filename'] = None
                    continue

                data[name] = value

        if self._has_participations(obj):
            path = '/'.join(obj.getPhysicalPath())
            self.skipped_data['Participations'].append(path)

        return data

    def _has_participations(self, obj):
        adapter = IParticipationAware(obj, None)
        if adapter and adapter.handler.get_participations():
            return True
        return False

    def serialize_creator(self, obj):
        return {'_creator': obj.Creator()}

    def serialize_local_roles(self, obj):
        local_roles = obj.get_local_roles()
        permissions = {
            'block_inheritance': getattr(obj, '__ac_local_roles_block__', False),
        }
        for principal, roles in local_roles:
            for role in roles:
                short_name = SHORTNAMES_BY_ROLE.get(role)
                if short_name:
                    permissions.setdefault(short_name, []).append(principal)
        return permissions

    def order_dict(self, data):
        ordered = OrderedDict()
        ordered['guid'] = data.pop('guid')

        parent_guid = data.pop('parent_guid', None)
        permissions = data.pop('_permissions', None)

        if parent_guid:
            ordered['parent_guid'] = parent_guid

        ordered.update(OrderedDict(sorted(data.items())))

        if permissions:
            ordered['_permissions'] = permissions

        return ordered

    def iso_datestr(self, value):
        if value is not None:
            return value.isoformat()
        else:
            return value


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('root_node_path',
                        help='Path to root node of subtree to be exported')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-o', '--output-dir', default='var/bundles/',
                        help='Path to output directory in which to create '
                             'exported bundle')
    parser.add_argument('--skip-unsupported-types', action='store_true',
                        help='Wether to skip unsupported types or raise an error.')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--with-local-roles', dest='with_local_roles', action='store_true',
        help='Export local roles settings in "_permissions" key',
    )
    group.add_argument(
        '--without-local-roles', dest='with_local_roles', action='store_false',
        help="Don't export local roles settings"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--dossiers-with-parent-reference',
        dest='dossiers_with_parent_reference', action='store_true',
        help="Reference dossier's repofolder parent via reference number",
    )
    group.add_argument(
        '--dossiers-with-parent-guid',
        dest='dossiers_with_parent_reference', action='store_false',
        help="Reference dossier's repofolder parent via bundle GUID",
    )

    options = parser.parse_args(sys.argv[3:])

    transaction.doom()
    plone = setup_plone(app, options)

    generator = SubtreeBundleExporter(plone, options)
    generator.run()
