"""
Script to fix (persist) field values that haven't been persisted on objects.

    bin/instance run fix_non_persisted_values.py

This script logs a detailed CSV report and a summary to var/log/, and displays
some progress info and stats on STDERR/STDOUT.
"""

from App.config import getConfiguration
from collections import Counter
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from ftw.mail import utils
from ftw.solr.interfaces import ISolrConnectionManager
from ftw.solr.interfaces import ISolrIndexHandler
from opengever.base.default_values import get_persisted_value_for_field
from opengever.dossier.dossiertemplate.behaviors import IDossierTemplateSchema
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.meeting.interfaces import IMeetingDossier
from opengever.meeting.proposal import IProposal
from opengever.task.task import ITask
from operator import itemgetter
from plone import api
from plone.dexterity.utils import iterSchemataForType
from plone.indexer.interfaces import IIndexableObject
from plone.indexer.interfaces import IIndexer
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from zope.schema.interfaces import RequiredMissing
from zope.component import getMultiAdapter
from zope.component import getUtility
from zope.component import queryMultiAdapter
from zope.component import queryUtility
from zope.intid.interfaces import IIntIds
from zope.schema import getFieldsInOrder
import logging
import os
import sys
import transaction


# Lightweight data structure to keep track of field values that got persisted
FixedField = namedtuple(
    'FixedField', [
        'schema_name',    # (short) name of the field's schema interface
        'field_name',     # name of the field
        'new_value',      # repr() of the value that got persisted
        'value_changed',  # boolean indicating whether the value changed or
                          #   not (compared to the volatile value)
    ]
)

# Fields that are not required, and have a field.default (but *not*
# a field.defaultFactory, which could be dynamic).
OPTIONAL_WITH_STATIC_DEFAULT = {
    'IRepositoryFolderSchema': [
        'allow_add_businesscase_dossier',  # Bool - default=True
    ],
    'IDossier': [
        'relatedDossier',  # RelationList - default=[]
        'keywords',  # Tuple - default=()
    ],
    'IDocumentMetadata': [
        'keywords',  # Tuple - default=()
    ],
    'IProtectDossier': [
        'reading',  # List - default=[]
        'reading_and_writing',  # List - default=[]
    ],
    'IClassification': [
        'public_trial_statement',  # Text - default=u''
    ],
    'IRelatedDocuments': [
        'relatedItems',  # RelationList - default=[]
    ],
    'IRestrictAddableDossierTemplates': [
        'addable_dossier_templates',  # RelationList - default=[]
    ],
    'IProposal': [
        'relatedItems',  # RelationList - default=[]
    ],
    'ITask': [
        'relatedItems',  # RelationList - default=[]
        'revoke_permissions',  # Bool - default=True
    ],
    'ISubmittedProposal': [
        'excerpts',  # RelationList - default=[]
    ],
}

# Fields that are not required, and don't have any kind of default.
# We can persist their missing value for these fields.
OPTIONAL_WITHOUT_DEFAULT = {
    'IRepositoryFolderSchema': [
        'addable_dossier_types',  # List - mv=None
        'former_reference',  # TextLine - mv=None
        'location',  # TextLine - mv=None
        'referenced_activity',  # TextLine - mv=None
        'valid_from',  # Date - mv=None
        'valid_until',  # Date - mv=None
    ],
    'IDossier': [
        'comments',  # Text - mv=None
        'container_location',  # TextLine - mv=None
        'container_type',  # Choice - mv=None
        'external_reference',  # TextLine - mv=None
        'end',  # Date - mv=None
        'filing_prefix',  # Choice - mv=None
        'former_reference_number',  # TextLine - mv=None
        'number_of_containers',  # Int - mv=None
        'temporary_former_reference_number',  # TextLine - mv=None
    ],
    'ILifeCycle': [
        'archival_value_annotation',  # Text - mv=None
        'date_of_cassation',  # Date - mv=None
        'date_of_submission',  # Date - mv=None
        'retention_period_annotation',  # Text - mv=None

    ],
    'IDocumentMetadata': [
        'archival_file',  # NamedBlobFile - mv=None
        'archival_file_state',  # Int - mv=None
        'delivery_date',  # Date - mv=None
        'description',  # Text - mv=u''
        'document_author',  # TextLine - mv=None
        'document_type',  # Choice - mv=None
        'foreign_reference',  # TextLine - mv=None
        'preview',  # NamedBlobFile - mv=None
        'receipt_date',  # Date - mv=None
        'thumbnail',  # NamedBlobFile - mv=None
    ],
    'IFilingNumber': [
        'filing_no',  # TextLine - mv=None
    ],
    'IOGMail': [
        'message_source',  # Choice - mv=None
        'original_message',  # NamedBlobFile - mv=None
    ],
    'IPreview': [
        'preview_file',  # NamedBlobFile - mv=None
        'conversion_state',  # Int - mv=None
    ],
    'ITask': [
        'date_of_completion',  # Date - mv=None
        'effectiveCost',  # Float - mv=None
        'effectiveDuration',  # Float - mv=None
        'expectedCost',  # Float - mv=None
        'expectedDuration',  # Float - mv=None
        'expectedStartOfWork',  # Date - mv=None
        'predecessor',  # TextLine - mv=None
        'text',  # Text - mv=None
    ],
    'ITaskTemplate': [
        'text',  # Text - mv=None
        'responsible',  # Choice - mv=None
    ],
    'IForwarding': [
        'deadline',  # Date - required=False - mv=None
    ],
    'IContact': [
        'academic_title',  # TextLine - mv=None
        'address1',  # TextLine - mv=None
        'address2',  # TextLine - mv=None
        'city',  # TextLine - mv=None
        'company',  # TextLine - mv=None
        'country',  # TextLine - mv=None
        'department',  # TextLine - mv=None
        'email',  # TextLine - mv=None
        'email2',  # TextLine - mv=None
        'function',  # TextLine - mv=None
        'phone_fax',  # TextLine - mv=None
        'phone_home',  # TextLine - mv=None
        'phone_mobile',  # TextLine - mv=None
        'phone_office',  # TextLine - mv=None
        'salutation',  # TextLine - mv=None
        'url',  # URI - mv=None
        'zip_code',  # TextLine - mv=None
    ],
    'IProposal': [
        'date_of_submission',  # Date - mv=None
        'predecessor',  # TextLine - mv=None
        'predecessor_proposal',  # RelationChoice - mv=None

    ],
    'IResponsibleOrgUnit': [
        'responsible_org_unit',  # TextLine - mv=None
    ],
    'ICommittee': [
        'ad_hoc_template',  # RelationChoice - mv=None
        'agenda_item_header_template',  # RelationChoice - mv=None
        'agenda_item_suffix_template',  # RelationChoice - mv=None
        'agendaitem_list_template',  # RelationChoice - mv=None
        'allowed_ad_hoc_agenda_item_templates',  # List - mv=None
        'allowed_proposal_templates',  # List - mv=None
        'excerpt_header_template',  # RelationChoice - mv=None
        'excerpt_suffix_template',  # RelationChoice - mv=None
        'paragraph_template',  # RelationChoice - mv=None
        'protocol_header_template',  # RelationChoice - mv=None
        'protocol_suffix_template',  # RelationChoice - mv=None
        'toc_template',  # RelationChoice - mv=None
    ],
    'ICommitteeContainer': [
        'ad_hoc_template',  # RelationChoice - mv=None
        'agenda_item_header_template',  # RelationChoice - mv=None
        'agenda_item_suffix_template',  # RelationChoice - mv=None
        'agendaitem_list_template',  # RelationChoice - mv=None
        'excerpt_header_template',  # RelationChoice - mv=None
        'excerpt_suffix_template',  # RelationChoice - mv=None
        'paragraph_template',  # RelationChoice - mv=None
        'protocol_suffix_template',  # RelationChoice - mv=None
        'toc_template',  # RelationChoice - mv=None
    ],
    'IDispositionSchema': [
        'transfer_number',  # TextLine - mv=None
    ],
    'IARPCaseBehavior2': [
        'applicant',  # Choice - mv=None
        'location',   # Textline - mv=None
        'requestType',   # Choice - mv=None
        'usage',   # Choice - mv=None
        'coordinateX',   # Int - mv=None
        'coordinateY',   # Int - mv=None
        'areaNew',   # Choice - mv=None
        'volumeNew',   # Choice - mv=None
        'volumeNew',   # Choice - mv=None
        'agent',  # List - mv=None
        'community',  # List - mv=None
        'assekNumber',  # List - mv=None
        'zones',   # List - mv=None
        'approval',  # Bool - mv=None
        'conditionDmPf',  # Bool - mv=None
        'conditionEnvironment',  # Bool - mv=None
        'legalTitle',  # list - mv=None
    ],
}


# Indexes that are considered "safe" in the sense that their value only
# depends directly on the attributes / fields that their name suggests.
# Meaning they don't access any other arbitrary fields on the object from
# their indexer's Python code.
SAFE_INDEXERS = [
    'title_de',
    'date_of_completion',
    'last_comment_date',
    'total_comments',
    'reference',
    'sequence_number',
    'getIcon',
    'is_folderish',
    'containing_subdossier',
    'getObjSize',
    'is_subtask',
    'assigned_client',
    'commentators',
    'containing_dossier',
    'title_fr',
    'trashed',
    'UID',

    'bumblebee_checksum',
    'document_author',
    'checked_out',
    'receipt_date',
    'getContentType',
    'public_trial',
    'Subject',
    'delivery_date',
    'document_date',
    'has_sametype_children',
    'changed',

    'start',
    'responsible',
    'end',
    'retention_expiration',
]

# Mapping of (fieldname -> index name) that lists indexes that are dependent
# on the value of a particular field in a non-trivial way (i.e. the index name
# isn't exactly the same as the field name).
DEPENDENT_INDEXERS = {
    'retention_period': 'retention_expiration',
}


def get_volatile_value(obj, field):
    """Get the volatile field value by using the field accessor.
    This will trigger any fallbacks to default / missing
    value that are in place.
    """
    bound_field = field.bind(obj)
    volatile_value = bound_field.get(field.interface(obj))
    return volatile_value


class NonPersistedValueFixer(object):
    """Queries the catalog for all objects, and persists any field values that
    currently aren't persisted by
      - iterating over all of the schemas of the object's portal_type
      - iterating over every field of each schema
      - determining the value that the field should have
    """

    CSV_HEADER = "intid;portal_type;path;created;missing_fields;value_changed"
    SCHEMA_CACHE = {}
    FIELD_CACHE = {}

    def __init__(self, options):
        self.reindex = not options.no_reindex

        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)
        self.reindexer = None

        self.stats = Counter()
        self.stats['by_field'] = Counter()
        self.stats['value_changed_by_field'] = Counter()
        self.stats['update_metadata'] = Counter()

        ts = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        self.csv_log_path = self.get_logfile_path(
            'fix-nonpersistent-values-%s.csv' % ts)
        self.summary_log_path = self.get_logfile_path(
            'fix-nonpersistent-values-summary-%s.log' % ts)

    def log(self, line):
        if not line.endswith('\n'):
            line += '\n'
        sys.stdout.write(line)
        self.summary_log.write(line)

    def run(self):
        sys.stderr.write("Fixing non-persisted values...\n\n")

        all_brains = self.catalog.unrestrictedSearchResults()
        total = len(all_brains)

        self.reindexer = Reindexer(self)

        with open(self.csv_log_path, 'w') as self.csv_log:
            with open(self.summary_log_path, 'w') as self.summary_log:
                self.csv_log.write(self.CSV_HEADER + '\n')

                for i, brain in enumerate(all_brains):
                    try:
                        obj = brain.getObject()
                    except KeyError:
                        # Some deployments seem to have cataloged objects
                        # where the real object doesn't exist (any more).
                        self.log("KeyError when doing brain.getObject() "
                                 "for %s, skipping." % brain.getPath())
                        continue

                    if IDossierTemplateSchema.providedBy(obj):
                        # DossierTemplates are messed up - they provide the
                        # IDossier interface, but don't fully implement its
                        # schema
                        continue

                    fixed_fields = self.fix_missing_fields(obj, brain)
                    self.update_stats(fixed_fields)

                    if fixed_fields:
                        self.write_csv_row(obj, fixed_fields)

                    if i % 100 == 0:
                        sys.stderr.write("Progress: %s of %s objects\n" % (
                            i, total))

                if self.reindexer.solr_enabled:
                    self.reindexer._commit_to_solr()

                self.display_stats()

    def get_fields_for_schema(self, schema):
        """Return fields for given schema (memoized).
        """
        if schema.__identifier__ not in self.FIELD_CACHE:
            _fields = map(itemgetter(1), getFieldsInOrder(schema))
            self.FIELD_CACHE[schema.__identifier__] = _fields
        fields = self.FIELD_CACHE[schema.__identifier__]
        return fields

    def get_schemas_for_type(self, portal_type):
        if portal_type not in self.SCHEMA_CACHE:
            _schemas = list(iterSchemataForType(portal_type))
            self.SCHEMA_CACHE[portal_type] = _schemas
        schemas = self.SCHEMA_CACHE[portal_type]
        return schemas

    def fix_missing_fields(self, obj, brain):
        """Persist all field values for the given object.
        """
        fixed_fields = []
        portal_type = obj.portal_type

        schemas = self.get_schemas_for_type(portal_type)
        for schema in schemas:
            fields = self.get_fields_for_schema(schema)

            for field in fields:
                name = field.getName()

                if IMeetingDossier.providedBy(obj) and name == 'responsible':
                    portal = api.portal.get()
                    if portal.title == u'Finanzdirektion (FD) (Dev)':
                        # We have some invalid example content on DEV
                        continue

                if name == 'changeNote':
                    # The changeNote field from p.a.versioningbehavior
                    # is a "fake" field - it never gets persisted, but
                    # written to request annotations instead
                    continue

                if name == 'reference_number':
                    # reference_number is a special field. It never gets
                    # set directly, but instead acts as a computed field
                    # for all intents and purposes.
                    continue

                try:
                    get_persisted_value_for_field(obj, field)
                except AttributeError:
                    volatile_value = get_volatile_value(obj, field)
                    value = self.determine_value(obj, field)
                    field.set(field.interface(obj), value)

                    # Track whether or not the value actually changed
                    value_changed = volatile_value != value

                    fixed_fields.append(
                        FixedField(schema_name=schema.__name__,
                                   field_name=name,
                                   new_value=repr(value),
                                   value_changed=value_changed)
                    )

        if fixed_fields and self.reindex:
            self.reindexer.reindex_if_necessary(obj, fixed_fields, brain)

        fixed_fields.sort()
        return fixed_fields

    def get_volatile_value(self, obj, field):
        """Get the volatile field value by using the field accessor.
        This will trigger any fallbacks to default / missing
        value that are in place.
        """
        bound_field = field.bind(obj)
        volatile_value = bound_field.get(field.interface(obj))
        return volatile_value

    def determine_value(self, obj, field):
        """Determine which value should be persisted for a field.

        In most cases, this will be the volatile value (the value that is
        currently being returned by fallbacks). For some fields, especially
        those with defaultFactories, we have handlers though, that determine
        the correct value using some custom logic.
        """
        fieldname = field.getName()
        schema_name = field.interface.__name__

        # First check if a special handler exists for this field
        if CustomValueHandler().available_for(field):
            value = CustomValueHandler().get_value(obj, field)
            return value

        # If the field is required we want to persist the value. Under the
        # hood this will eventually call the `determine_default_value` from
        # `opengever.base.default_values` also used in default value patches
        # in `opengever.core`.
        # This case occurs with old objects in the database in combination
        # with fields added to the schema after the objects were created or
        # modified.
        if field.required:
            volatile_value = get_volatile_value(obj, field)
            return volatile_value

        if fieldname in OPTIONAL_WITH_STATIC_DEFAULT.get(schema_name, []):
            assert field.required is False, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)
            assert field.default is not None, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)
            assert field.defaultFactory is None, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)

            volatile_value = get_volatile_value(obj, field)
            # Field has a default - volatile value should therefore
            # be equal to the field's default
            assert volatile_value == field.default
            return volatile_value

        if fieldname in OPTIONAL_WITHOUT_DEFAULT.get(schema_name, []):
            assert field.required is False, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)
            assert field.default is None, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)
            assert field.defaultFactory is None, "Schema: %s Fieldname %s" % (
                schema_name, fieldname)

            volatile_value = get_volatile_value(obj, field)
            # Field has no default - volatile value should therefore
            # be equal to the field's missing value
            assert volatile_value == field.missing_value
            return volatile_value

        # We should not have any default factories that haven't been handled
        # yet at this point.
        default_factory = field.defaultFactory
        if default_factory:
            self.log("")
            self.log("Field %r has a defaultFactory and no custom handler, "
                     "refusing to persist its value" % fieldname)
            try:
                val = get_persisted_value_for_field(obj, field)
                self.log("Currently persisted value: %r" % val)
            except AttributeError:
                self.log("Currently persisted value: <NO PERSISTED VALUE>")

            self.log("")
            raise Exception(
                'Unexpected defaultFactory for field %r.%r' %
                (schema_name, fieldname))

        # If we end up here, it means that we encountered a field that has
        # not explicitly been handled (by either defining a custom handler,
        # or listing it in OPTIONAL_WITH_STATIC_DEFAULT or
        # OPTIONAL_WITHOUT_DEFAULT)
        self.log("Unhandled field:\n\n")

        def safe_format_op(param):
            """Wrap single tuples in an extra tuple as when passed a tuple the
            format operator uses the tuples contents as arguments.
            So formatting won't work with an empty or a too long tuple.
            """
            if isinstance(param, tuple):
                return (param,)
            return param

        self.log("Fieldname: %s" % fieldname)
        self.log("Field type: %s" % field.__class__.__name__)
        self.log("Schema: %s" % schema_name)
        self.log("required: %r" % field.required)
        self.log("missing_value: %r" % safe_format_op(field.missing_value))
        self.log("default: %r" % safe_format_op(field.__dict__['default']))
        self.log("defaultFactory: %r" % field.defaultFactory)

        raise Exception('Unhandled field: %s.%s' % (schema_name, fieldname))

    def write_csv_row(self, obj, missing_fields):
        created = str(obj.created())
        intid = self.intids.queryId(obj)
        values_changed = [f.value_changed for f in missing_fields]
        row = [
            str(intid),
            obj.portal_type,
            '/'.join(obj.getPhysicalPath()),
            created,
            str([f.field_name for f in missing_fields]),
            str(any(values_changed)),
        ]
        self.csv_log.write(';'.join(row) + '\n')

    def update_stats(self, missing_fields):
        if missing_fields:
            self.stats['missing'] += 1

            for f in missing_fields:
                self.stats['by_field'][(f.schema_name, f.field_name)] += 1
                if f.value_changed:
                    self.stats['value_changed_by_field'][(f.schema_name, f.field_name)] += 1
        else:
            self.stats['ok'] += 1

    def display_stats(self):
        self.log("")

        self.log("Missing (by field):")
        stats_by_field = sorted(self.stats['by_field'].items())
        for (schema_name, field_name), count in stats_by_field:
            dotted_name = '.'.join((schema_name, field_name))
            self.log("  %-120s %s" % (dotted_name, count))

        self.log("")

        self.log("Value changed (by field):")
        value_changed_by_field = sorted(self.stats['value_changed_by_field'].items())
        for (schema_name, field_name), count in value_changed_by_field:
            dotted_name = '.'.join((schema_name, field_name))
            self.log("  %-120s %s" % (dotted_name, count))

        self.log("")

        self.log("Summary (by object):")
        self.log("Missing: %s" % self.stats['missing'])
        self.log("OK: %s" % self.stats['ok'])

        self.log("")
        self.log("Metadata reindexed:")
        self.log("True: %s" % self.stats['update_metadata'][True])
        self.log("False: %s" % self.stats['update_metadata'][False])

        self.log("")
        self.log("Detailed CSV report written to %s" % self.csv_log_path)
        self.log("Summary written to %s" % self.summary_log_path)

    def get_logfile_path(self, filename):
        log_dir = self.get_logdir()
        return os.path.join(log_dir, filename)

    def get_logdir(self):
        """Determine the log directory.
        This will be derived from Zope2's EventLog location, in order to not
        have to figure out the path to var/log/ ourselves.
        """
        zconf = getConfiguration()
        eventlog = getattr(zconf, 'eventlog', None)

        if eventlog is None:
            root_logger = logging.root
            root_logger.error('')
            root_logger.error(
                "Couldn't find eventlog configuration in order to determine "
                "logfile location - aborting!")
            root_logger.error('')
            sys.exit(1)

        handler_factories = eventlog.handler_factories
        eventlog_path = handler_factories[0].section.path
        assert eventlog_path.endswith('.log')
        log_dir = os.path.dirname(eventlog_path)
        return log_dir


class RegistryCache(object):
    """Helper to memoize registry based defaults, since they never change
    during the runtime of this script.
    """

    cache = {}

    @classmethod
    def get(cls, record_name):
        if record_name not in cls.cache:
            value = api.portal.get_registry_record(record_name)
            cls.cache[record_name] = value

        return cls.cache[record_name]


class CustomValueHandler(object):
    """Class to group together custom handlers for fields that need some
    special logic to determine their value.
    """

    def get_value(self, obj, field):
        """Get the value for the given field by looking up the custom handler
        and calling it.
        """
        field_signature = (field.interface.__name__, field.getName())
        handler = self.handlers.get(field_signature)
        return handler(self, obj, field)

    def available_for(self, field):
        field_signature = (field.interface.__name__, field.getName())
        return field_signature in self.handlers

    def _get_volatile_value(self, obj, field):
        bound_field = field.bind(obj)
        volatile_value = bound_field.get(field.interface(obj))

        # Verify that the value is valid
        try:
            bound_field.validate(volatile_value)
        except RequiredMissing as e:
            # Check if its a part of the `Abnahme` repositoryfolder,
            # a relict of the konsulmigration (content which was not
            # created correctly). In that case we try to use the field default.
            if bound_field.get(field.interface(obj)) is None:
                if 'abnahme' in obj.getPhysicalPath():
                    volatile_value = bound_field.default

            # Re verify that the value is valid - when value has not been
            # changed or is still not valid it will raise again.
            bound_field.validate(volatile_value)

        return volatile_value

    def get_preserved_as_paper_value(self, obj, field):
        """Get value for preserved_as_paper field of IDocumentMetadata behavior.

        We fetch the default from the registry once, cache it, and then
        persist that value for all the fields that need it.
        """
        value = RegistryCache.get(
            'opengever.document.interfaces.IDocumentSettings.'
            'preserved_as_paper_default')

        # Verify that the value is valid
        bound_field = field.bind(obj)
        volatile_value = bound_field.get(obj)
        assert value == volatile_value
        bound_field.validate(value)

        return value

    def get_public_trial_value(self, obj, field):
        """Get value for public_trial field of IClassification behavior.

        We fetch the default from the registry once, cache it, and then
        persist that value for all the fields that need it.
        """
        value = RegistryCache.get(
            'opengever.base.behaviors.classification.IClassificationSettings.'
            'public_trial_default_value')

        # Verify that the value is valid
        bound_field = field.bind(obj)
        volatile_value = bound_field.get(obj)
        assert value == volatile_value
        bound_field.validate(value)

        return value

    def get_deadline_value(self, obj, field):
        """Get value for deadline field of tasks.

        The default for this field has always been 5 days in the future, so
        we set it to <task_creation_date> + 5d here.

        Note that there is also a deadline field on forwardings which is *not*
        required - that one is handled in OPTIONAL_WITHOUT_DEFAULT and will
        be set to missing value.
        """
        assert ITask.providedBy(obj)
        creation_date = obj.created().asdatetime()
        deadline = (creation_date + timedelta(days=5)).date()
        return deadline

    def get_document_date_value(self, obj, field):
        """Get value for document_date field of documents.

        We use the date of the latest CMFEditions version for this.
        """
        if obj.portal_type == "ftw.mail.mail":
            timestamp = utils.get_date_header(obj.msg, 'Date') or 0.0
            date_time = datetime.fromtimestamp(timestamp)
            return date_time.date()

        repository = api.portal.get_tool('portal_repository')
        history_metadata = repository.getHistoryMetadata(obj)
        # we've encountered empty lists as history_metadata, i.e. no
        # versions for the document. Could be the case when there is no
        # initial version yet, or for older documents. Fall back to
        # creation date in such cases.
        if not history_metadata:
            return obj.created().asdatetime().date()

        latest_version_id = history_metadata.getLength(countPurged=False) - 1
        latest_version = history_metadata.retrieve(latest_version_id)
        ts = latest_version['metadata']['sys_metadata']['timestamp']
        last_version_date = datetime.fromtimestamp(ts).date()

        # Verify that the value is valid
        bound_field = field.bind(obj)
        bound_field.validate(last_version_date)

        return last_version_date

    def get_dossier_start_value(self, obj, field):
        """Get value for start field of dossiers.

        We use the dossier's creation date for this. Technically the start
        field for dossiers isn't required, but since the field isn't persisted,
        it's safe to assume that the user accepted the default, which would
        have been the current date at that time.
        """
        created_date = obj.created().asdatetime().date()

        # Verify that the value is valid
        bound_field = field.bind(obj)
        bound_field.validate(created_date)

        return created_date

    def get_dossier_manager_value(self, obj, field):
        if obj.reading == [] and obj.reading_and_writing == []:
            return None

        print "Encountered a problematic dossier_manager field:"
        print "The object at %s doesn't have a value persisted" % obj
        print "for its dossier_manager field, but at the same time"
        print "has non-empty values for either the reading or "
        print "reading_and_writing field."
        print "This is a situation that can't be fixed automatically and"
        print "needs to be looked at and corrected manually"

        raise Exception("Unexpected dossier_manager settings")

    # For these four acquired restricted defaults, we simply persist the
    # volatile value. Because of the way these acquired defaults work, this
    # might not necessarily be what the user saw when they first saved the
    # form. But it's what last got displayed and effectively been used. It's
    # the best we can do.

    def get_classification_value(self, obj, field):
        """Get value for classification field.
        """
        return self._get_volatile_value(obj, field)

    def get_custody_period_value(self, obj, field):
        """Get value for custody_period field.
        """
        return self._get_volatile_value(obj, field)

    def get_retention_period_value(self, obj, field):
        """Get value for retention_period field.
        """
        return self._get_volatile_value(obj, field)

    def get_privacy_layer_value(self, obj, field):
        """Get value for privacy_layer field.
        """
        return self._get_volatile_value(obj, field)

    handlers = {
        ('IDocumentMetadata', 'preserved_as_paper'): get_preserved_as_paper_value,  # noqa
        ('IDocumentMetadata', 'document_date'): get_document_date_value,
        ('IDossier', 'start'): get_dossier_start_value,
        ('IProtectDossier', 'dossier_manager'): get_dossier_manager_value,
        ('ITask', 'deadline'): get_deadline_value,
        ('ILifeCycle', 'custody_period'): get_custody_period_value,
        ('ILifeCycle', 'retention_period'): get_retention_period_value,
        ('IClassification', 'public_trial'): get_public_trial_value,
        ('IClassification', 'classification'): get_classification_value,
        ('IClassification', 'privacy_layer'): get_privacy_layer_value,
    }


class Reindexer(object):

    def __init__(self, fixer):
        self.catalog = fixer.catalog
        self.stats = fixer.stats
        self.fixer = fixer

        self._metadata_names = None
        self._index_names = None
        self._solr_enabled = None
        self.processed = 0

    @property
    def solr_connection_manager(self):
        return queryUtility(ISolrConnectionManager)

    @property
    def solr_enabled(self):
        """Boolean from registry to indicate whether Solr is enabled (memoized).
        """
        if self._solr_enabled is None:
            self._solr_enabled = api.portal.get_registry_record(
                'opengever.base.interfaces.ISearchSettings.use_solr',
                default=False)
        return self._solr_enabled

    @property
    def metadata_names(self):
        """List of metadata column names (memoized).
        """
        if self._metadata_names is None:
            self._metadata_names = self.catalog._catalog.schema.keys()
        return self._metadata_names

    @property
    def index_names(self):
        """List of index names (memoized).
        """
        # we ignore indexed attrs here
        if self._index_names is None:
            self._index_names = self.catalog.indexes()
        return self._index_names

    def reindex_if_necessary(self, obj, fixed_fields, brain):
        """Reindex indexes and metadata for the given object if needed.
        """
        update_metadata = self.needs_metadata_update(
            obj, fixed_fields, brain)

        self.stats['update_metadata'][update_metadata] += 1

        idxs_needing_reindex = self.get_idxs_needing_reindex(
            obj, fixed_fields, brain)

        if update_metadata or idxs_needing_reindex:
            # If idxs == [] the catalog defaults to *all* indexes, so we
            # supply it a cheap index to trick it into only rebuilding metadata
            if not idxs_needing_reindex:
                idxs_needing_reindex = ['getId']

            sys.stderr.write(
                "Reindexing %s (update_metadata=%r, idxs=%r)\n" % (
                    obj, update_metadata, idxs_needing_reindex))
            self.catalog.reindexObject(obj, idxs=idxs_needing_reindex,
                                       update_metadata=update_metadata)
            if self.solr_enabled:
                self.reindex_in_solr(obj, idxs_needing_reindex, update_metadata)

    def reindex_in_solr(self, obj, idxs, update_metadata):
        if update_metadata:
            # If update_metadata is True, we need to force an update of all
            # indexes, irrespective of what `idxs` says. Setting `idxs` to
            # something falsy will cause the handler to not do atomic updates.
            idxs = None

        handler = getMultiAdapter(
            (obj, self.solr_connection_manager), ISolrIndexHandler)
        handler.add(idxs)

        if self.processed % 100 == 0:
            self._commit_to_solr()

        self.processed += 1

    def _commit_to_solr(self):
        conn = self.solr_connection_manager.connection
        conn.commit(extract_after_commit=False)
        print('Intermediate commit to solr (%d items '
              'processed)' % self.processed)

    def get_metadata_indexers(self, obj):
        """Get a mapping of (metadata_name => indexer) of the indexers for
        metadata columns for a given object.
        """
        metadata_indexers = {}
        for name in self.metadata_names:
            indexer = queryMultiAdapter(
                (obj, self.catalog), IIndexer, name=name)
            if indexer:
                metadata_indexers[name] = indexer
        return metadata_indexers

    def get_idxs_needing_reindex(self, obj, fixed_fields, brain):
        """Determine which indexes need reindexing, based on the list of

        fields that got fixed (persisted).
        """
        if self.no_dynamic_defaults_fixed(fixed_fields):
            values_changed = [f.value_changed for f in fixed_fields]
            if not any(values_changed):
                return []

        idxs_needing_reindex = []

        rid = brain.getRID()

        wrapper = queryMultiAdapter((obj, self.catalog), IIndexableObject)
        if wrapper is None:
            wrapper = obj

        for f in fixed_fields:
            if f.field_name in self.index_names:
                index = self.catalog._catalog.getIndex(f.field_name)
                index_value = index.getEntryForObject(rid)
                new_index_value = getattr(obj, f.field_name)
                if isinstance(index, DateIndex):
                    new_index_value = index._convert(new_index_value)

                if index_value != new_index_value:
                    idxs_needing_reindex.append(f.field_name)

        return idxs_needing_reindex

    def no_dynamic_defaults_fixed(self, fixed_fields):
        """Return True if the only values that got persisted are either
        static defaults (as opposed to defaultFactories) or fields that didn't
        have default, and therefore got their missing value persisted (which
        are static as well).

        Because these are static, and didn't get changed by us over the
        lifetime of the software (to my knowledge), we can safely assume that
        they had the same value back when the object was indexed, and
        therefore reindexing the object can be skipped.
        """
        static = (OPTIONAL_WITHOUT_DEFAULT, OPTIONAL_WITH_STATIC_DEFAULT)
        for f in fixed_fields:
            if not any([f.field_name in mapping.get(f.schema_name, [])
                       for mapping in static]):
                return False
        return True

    def needs_metadata_update(self, obj, fixed_fields, brain):
        """Check whether an object needs to have its metadata reindexed

        based on what fields got fixed (persisted).
        """
        # If the only field values that got persisted are ones that either
        # have no default (=> missing value got persisted) or a static default
        # (i.e., no defaultFactory), then we can safely assume that those
        # are the values that always got returned by the fallbacks, already
        # were present at object indexing time, and therefore are correctly
        # indexed.
        if self.no_dynamic_defaults_fixed(fixed_fields):
            values_changed = [f.value_changed for f in fixed_fields]
            if not any(values_changed):
                return False

        fixed_fieldnames = [f.field_name for f in fixed_fields]

        # If for any of the fixed fields a metadata column exists with
        # exactly that name, metadata needs to be rebuilt
        if any([fn in self.metadata_names for fn in fixed_fieldnames]):
            return True

        # If any of the fixed fields have an indexer that is dependent on them
        # in an indirect way, metadata needs to be rebuilt
        if any([fn in DEPENDENT_INDEXERS for fn in fixed_fieldnames]):
            return True

        # All remaining indexers that exist with a name that is present in
        # the available metadata columns need to be flagged "safe" - otherwise
        # metadata needs to be rebuilt.
        # (Safe means the indexer doesn't take into account any other field
        # data other than from the exact field that corresponds to the name
        # of the metadata column).
        indexers = self.get_metadata_indexers(obj)
        unsafe_indexers = {name: indexer for name, indexer in indexers.items()
                           if name not in SAFE_INDEXERS and
                           name not in DEPENDENT_INDEXERS}
        if unsafe_indexers:
            self.fixer.log("Obj has indexers not explicitly declared as safe:")
            for name, indexer in unsafe_indexers.items():
                self.fixer.log("%s (%r, %r)" % (
                    name, indexer.callable.__name__,
                    indexer.callable.__module__))

            # Fall back to doing a full metadata diff
            self.fixer.log("Falling back to full metadata diff for %r "
                           "(fields: %r)" % (obj.id, fixed_fieldnames))
            changed_columns = self.has_metadata_changed(obj, brain)
            if changed_columns:
                return True

        return False

    def has_metadata_changed(self, obj, brain):
        """Check if the indexed metadata would change for an object by doing

        a full comparison between the stored metadata and the metadata we get
        when calling _catalog.recordify().

        This is rather expensive, and should only be done as a last resort if
        the need for metadata reindexing can't be determined by simpler means.
        """
        wrapper = queryMultiAdapter((obj, self.catalog), IIndexableObject)
        if wrapper is None:
            wrapper = obj

        rid = brain.getRID()
        cols_in_order = sorted(
            self.catalog._catalog.schema.keys(),
            key=lambda name: self.catalog._catalog.schema[name])

        old_record = self.catalog._catalog.data[rid]
        old_metadata = dict(zip(cols_in_order, old_record))

        new_record = self.catalog._catalog.recordify(wrapper)
        new_metadata = dict(zip(cols_in_order, new_record))

        # Skip any timestamps for the comparison. They get calculated with
        # the local TZ, so their representation changes with DST rollover
        timestamps = ['CreationDate', 'Date', 'ModificationDate', 'effective',
                      'expires', 'modified', 'created']

        for name in timestamps:
            old_metadata.pop(name)
            new_metadata.pop(name)

        changed_columns = []
        if old_metadata != new_metadata:
            for name in new_metadata.keys():
                if old_metadata[name] != new_metadata[name]:
                    if name == 'issuer' and IProposal.providedBy(obj):
                        # See https://github.com/4teamwork/opengever.core/issues/4855  # noqa
                        continue
                    changed_columns.append(name)

                new_record = self.catalog._catalog.recordify(wrapper)
        return changed_columns


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()

    parser.add_option("--no-reindex", action="store_true",
                      dest="no_reindex", default=False)
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        print "Dry-run"
        transaction.doom()

    fixer = NonPersistedValueFixer(options)
    fixer.run()

    if not options.dryrun:
        transaction.commit()
