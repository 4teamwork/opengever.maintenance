from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.base.behaviors.lifecycle import ILifeCycle
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberFormatter
from opengever.base.interfaces import IReferenceNumberSettings
from opengever.base.interfaces import IRetentionPeriodRegister
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.journal import _
from opengever.journal.handlers import journal_entry_factory
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.repository.interfaces import IRepositoryFolder
from opengever.setup.sections.reference import PathFromReferenceNumberSection
from opengever.setup.sections.xlssource import XlsSource
from plone import api
from plone.i18n.normalizer.interfaces import IIDNormalizer
from plone.i18n.normalizer.interfaces import IURLNormalizer
from plone.registry.interfaces import IRegistry
from zope.annotation.interfaces import IAnnotations
from zope.component import getAdapter
from zope.component import getUtility
from zope.component import queryUtility
import logging
import os
import os.path
import sys
import transaction


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)

REPOSITORIES_FOLDER_NAME = 'opengever_repositories'

BACKUP_KEY = 'retention_period_backup'

# all affected customers use the default rentetion periods of
# ['5', '10', '15', '20', '25']. furthermore the default of 5 is hardcoded as
# a default value of ILifeCycle.retention_period
DEFAULT_PERIOD = 5


ACTION_RETENTION_PERIOD_FIXED = "Retention period fixed"


class Abort(Exception):
    pass


class FixerXlsSource(XlsSource):
    """Abuse xlsx-source transmogrifier section from opengever.core.

    No longer do transmogrifier related stuff but just load the excel file
    from the specified path.
    """
    def __init__(self, repository_file_path):
        self.repository_file_path = repository_file_path
        self.filename = os.path.split(repository_file_path)[1]
        self.repository_id, extension = os.path.splitext(self.filename)

    def __iter__(self):
        keys, sheet_data = self.read_excel_file(self.repository_file_path)
        # skip repository root
        for rownum, row in enumerate(sheet_data[1:], start=1):
            yield self.process_row(row, rownum, keys, self.repository_id)


class FixerPathFromReferenceNumber(PathFromReferenceNumberSection):
    """Abuse reference number inserter from opengever.core

    No longer do transmogrifier related stuff but just generate the reference
    numbers.
    """
    def __init__(self, previous, reference_formatter):
        self.logger = logger
        self.previous = previous

        self.refnum_mapping = {}
        self.normalizer = queryUtility(IURLNormalizer, name="de")
        self.id_normalizer = queryUtility(IIDNormalizer)
        self.reference_formatter = reference_formatter

    def has_grouped_by_three_reference_number_formatter(self):
        if isinstance(self.reference_formatter, basestring):
            return self.reference_formatter in [
                'grouped_by_three', 'no_client_id_grouped_by_three']

        return self.reference_formatter.is_grouped_by_three

    def get_reference_number(self, refnum):
        if self.has_grouped_by_three_reference_number_formatter():
            cl_refnum = refnum.replace('.', '')
            return '.'.join(cl_refnum)
        return refnum


class RepoRootDiff(object):

    def __init__(self, registry, context):
        self.new_period = DEFAULT_PERIOD
        self.context = context
        self.child_folders = []

        self._register_in(registry)

    def _register_in(self, registry):
        path = '/'.join(self.context.getPhysicalPath())
        if path in registry:
            raise Abort('object is already registered: {}'.format(path))
        registry[path] = self

    def append_child_folder(self, child_folder):
        self.child_folders.append(child_folder)

    def apply_recursively(self):
        for child_folder in self.child_folders:
            child_folder.apply_recursively()


class RepoFolderDiff(RepoRootDiff):

    def __init__(self, repo_root, registry, handled_folders, context, item,
                 reference_formatter, catalog, options):
        self._is_leaf_folder = None
        self.can_apply = True
        self.parent = None
        self.child_dossiers = []

        self.repo_root = repo_root
        self.handled_folders = handled_folders
        self.item = item
        self.reference_formatter = reference_formatter
        self.catalog = catalog
        self.options = options

        super(RepoFolderDiff, self).__init__(registry, context)
        self._diff()

    def _register_in(self, registry):
        super(RepoFolderDiff, self)._register_in(registry)

        parent_context = aq_parent(aq_inner(self.context))
        self.parent = registry['/'.join(parent_context.getPhysicalPath())]
        self.parent.append_child_folder(self)

    @property
    def is_leaf_folder(self):
        if self._is_leaf_folder is None:
            child_folders = self.catalog.unrestrictedSearchResults(
                path={'query': '/'.join(self.context.getPhysicalPath()),
                      'depth': 1},
                object_provides=IRepositoryFolder.__identifier__)
            self._is_leaf_folder = len(child_folders) == 0
        return self._is_leaf_folder

    def _diff(self):
        self.reference_number = self._get_repository_reference_number()
        if self.reference_number != self.item['reference_number']:
            logger.warn('reference numbers differ for: {}'
                        ' "{}" (site), "{}" (excel)'
                        .format(self.item['_query_path'],
                                self.reference_number,
                                self.item["reference_number"]))
            self.can_apply = False
            return

        self.current_period = ILifeCycle(self.context).retention_period
        xls_period = self.item.get('retention_period')
        self.new_period = xls_period if xls_period is not None else self.parent.new_period

    def _get_repository_reference_number(self):
        reference = IReferenceNumber(self.context)
        formatter = getAdapter(self.context, IReferenceNumberFormatter,
                               name=self.reference_formatter)
        return formatter.repository_number(reference.get_parent_numbers())

    def make_retention_period_backup(self, obj, backup_period):
        annotations = IAnnotations(obj)
        if BACKUP_KEY not in annotations:
            annotations[BACKUP_KEY] = backup_period

    def apply_recursively(self):
        if not self.can_apply:
            return

        if self.apply_to_repo_folder() and self.is_leaf_folder:
            self.apply_to_dossiers()

        super(RepoFolderDiff, self).apply_recursively()

    def apply_to_dossiers(self):
        child_dossiers = self.catalog.unrestrictedSearchResults(
            path='/'.join(self.context.getPhysicalPath()),
            object_provides=IDossierMarker.__identifier__)

        for brain in child_dossiers:
            self.apply_to_dossier(brain.getObject())

    def apply_to_repo_folder(self):
        kind = 'leaf ' if self.is_leaf_folder else ''
        if self.current_period != DEFAULT_PERIOD:
            if self.options.verbose:
                logger.info('skipping {}repo-folder ({}) {}, '
                            'non-default retention_period'
                            .format(kind,
                                    self.reference_number,
                                    self.item['_query_path']))
            return False

        current_title = self.context.Title(prefix_with_reference_number=False)
        current_title = current_title.decode('utf-8')  # Title returns utf-8
        xls_title = self.item['effective_title']
        if current_title != xls_title:
            if self.options.verbose:
                logger.info(u'skipping {}repo-folder ({}) {}, title changed '
                            u'from "{}" to "{}"'
                            .format(kind,
                                    self.reference_number,
                                    self.item['_query_path'],
                                    xls_title,
                                    current_title))
            return False

        self.handled_folders.add(self.context)

        if self.current_period == self.new_period:
            if self.options.verbose:
                logger.info('skipping {}repo-folder ({}) {}, no change '
                            'from default retention_period'
                            .format(kind,
                                    self.reference_number,
                                    self.item['_query_path']))
            return False

        if self.options.verbose:
            logger.info('fixing {}repo-folder ({}) {}, {}->{}'
                        .format(kind,
                                self.reference_number,
                                self.item['_query_path'],
                                self.current_period,
                                self.new_period))
        self.make_retention_period_backup(self.context, self.current_period)
        ILifeCycle(self.context).retention_period = self.new_period
        self.add_journal_entry(self.repo_root, self.context,
                               self.current_period, self.new_period)

        return True

    def apply_to_dossier(self, dossier):
        dossier_path = '/'.join(dossier.getPhysicalPath())
        dossier_period = ILifeCycle(dossier).retention_period

        if dossier_period != self.current_period:
            if self.options.verbose:
                logger.info('skipping dossier {}, dossier retention_period '
                            'deviated from repository'
                            .format(dossier_path))
            return

        if dossier_period == self.new_period:
            if self.options.verbose:
                logger.info('skipping dossier {}, dossier period already '
                            'matches new repo period'
                            .format(dossier_path))
            return

        if self.options.verbose:
            logger.info('fixing dossier {}, {}->{}'
                        .format(dossier_path, dossier_period, self.new_period))
        self.make_retention_period_backup(dossier, dossier_period)
        ILifeCycle(dossier).retention_period = self.new_period
        self.add_journal_entry(dossier, dossier,
                               dossier_period, self.new_period)

    def add_journal_entry(self, journal_context, fixed_context,
                          old_period, new_period):
        """Add a journal entry to `journal_context` indicating that
        `fixed_context` has been fixed.

        The journal entry is not translated, we don't want to add translations
        for this fix to opengever.core.

        We use message objects to make it possible to add translations should
        it be required, also this helps to avoid an UnicodeDecodeError.

        """
        title = _(u'label_retention_period_fixed',
                  default=u'Aufbewahrungsdauer korrigiert "${name}"',
                  mapping={'name':
                           fixed_context.Title().decode('utf-8')})
        # the comment is handled different from the title, it does not get
        # translated when displaying :-o.
        comment = 'Alter Wert: "{} Jahre", neuer Wert: "{} Jahre"'\
                  .format(old_period, new_period)

        journal_entry_factory(journal_context,
                              ACTION_RETENTION_PERIOD_FIXED,
                              title=title,
                              comment=comment)


class RetentionPeriodFixer(XlsSource):
    """Fix retention periods that were not set from the xls file by mistake.

    The problem was caused by a missing row-header in the init excel file.
    Thus all retention_period values specified by the customer were not set,
    instead the default value was used.

    We attempt to fix this by comparing the current repository to the initial
    excel file and setting the retention_period on the repository as follows:

                    plone: default value, plone: (re-)moved, plone: changed
    xls: no value   parent/default,       skip,              skip
    xls: value      from xls,             skip,              skip

    Values specified in the excel file are inherited by children. If an excel
    file contanis a value for a parent folder but none for a child folder the
    child folder inherits the parent's retention period setting.

    When a leaf folder's retention_period is updated, all dossiers in that
    folder that have the same retention_period as the folder are updated
    as well, under the assumption that the value was inherited.

    Note: this will set a wrong retention_period when the following two
    conditions are met:
      - the parent repo_folders retention_period changes to non-default, and ...
      - ... the child (dossier/repo-folder) should have a rentention_period
        equal to the value of what was previously a default, but was a
        deliberately chosen value for that dossier/repo-folder.

    Unfortunately this cannot be avoided since we cannot tell the difference
    between inherited and configured value.

    The old retention period is stored in the object annotations in case
    we need to recover from a failed fix.

    """
    def __init__(self, plone, options):
        self.plone = plone
        self.options = options
        self.profile = options.profile
        self.portal_setup = api.portal.get_tool('portal_setup')
        self.catalog = api.portal.get_tool('portal_catalog')
        self.diffs = {}
        self.handled_folders = set()

        registry = getUtility(IRegistry)
        proxy = registry.forInterface(IReferenceNumberSettings)
        self.reference_formatter = proxy.formatter

        retention_period_register = registry.forInterface(
            IRetentionPeriodRegister)
        if retention_period_register.is_restricted:
            raise Abort("The retention periods are restricted. I'm not sure "
                        "what that means so i don't handle it.")

    def run(self):
        xlssource = FixerXlsSource(self.get_repository_file_path())
        source = FixerPathFromReferenceNumber(xlssource,
                                              self.reference_formatter)

        repo_root = self.get_repo_root()
        diff_root = RepoRootDiff(self.diffs, repo_root)
        for item in source:
            self.init_diff(repo_root, item)

        diff_root.apply_recursively()

        self.log_stats()

    def init_diff(self, repo_root, item):
        path = item['_path'].lstrip('/').encode('utf-8')
        item['_query_path'] = path

        context = self.plone.unrestrictedTraverse(path, default=None)
        if not context:
            logger.warn('could not find repository folder: {}'.format(path))
            return

        RepoFolderDiff(repo_root, self.diffs, self.handled_folders, context,
                       item, self.reference_formatter, self.catalog,
                       self.options)

    def log_stats(self):
        if not self.options.verbose:
            return

        repo_folder_brains = self.catalog.unrestrictedSearchResults(
            object_provides=IRepositoryFolder.__identifier__)
        repo_folders = set(each.getObject() for each in repo_folder_brains)

        logger.info(20*'-')
        logger.info("{} of {} repository folders have been checked/fixed"
                    .format(len(self.handled_folders), len(repo_folders)))

        omitted_folders = repo_folders.difference(self.handled_folders)
        omitted_folders = sorted(omitted_folders,
                                 key=lambda content: content.absolute_url())
        logger.info('The following repository folders have not been '
                    'checked/fixed:')
        for omitted in omitted_folders:
            logger.info("{} {}".format(
                omitted.Title(), '/'.join(omitted.getPhysicalPath())))

    def get_repository_file_path(self):
        profile_info = self.portal_setup.getProfileInfo(self.profile)
        profile_path = profile_info['path']

        repositories_folder = os.path.join(profile_path,
                                           REPOSITORIES_FOLDER_NAME)

        def is_parsable(filename):
            if filename.startswith('.') or filename.startswith('~'):
                return False
            if not filename.endswith('.xlsx'):
                return False
            return True

        repository_filenames = [filename for filename in
                                os.listdir(repositories_folder)
                                if is_parsable(filename)]

        if len(repository_filenames) != 1:
            raise Abort("Expected one repository file but found {}, {}".format(
                        len(repository_filenames), repository_filenames))
        return os.path.join(repositories_folder, repository_filenames[0])

    def get_repo_root(self):
        repo_roots = self.catalog.unrestrictedSearchResults(
            portal_type={'query': 'opengever.repository.repositoryroot',
                         'depth': 1})
        if len(repo_roots) != 1:
            raise Abort('Expected exactly one repository root, found {}'
                        .format(len(repo_roots)))

        return repo_roots[0].getObject()


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("-p", dest="profile",
                      help="profile that contains the repository excel file.")
    options, args = parser.parse_args()

    if not options.profile:
        logger.error("the profile (-p) argument is required.")
        return
    if ":" not in options.profile:
        logger.error("invalid profile id: '{}', missing ':'"
                     .format(options.profile))
        return

    if options.dry_run:
        logger.warn('transaction doomed because we are in dry-mode.')
        transaction.doom()

    plone = setup_plone(app, options)
    RetentionPeriodFixer(plone, options).run()
    if options.dry_run:
        logger.warn('skipping commit because we are in dry-mode.')
    else:
        transaction.commit()
        logger.info('done.')


if __name__ == '__main__':
    main()
