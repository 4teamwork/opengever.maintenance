from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberFormatter
from opengever.base.interfaces import IReferenceNumberSettings
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


class RepoRootDiff(object):

    def __init__(self, registry, context):
        self.context = context
        self.child_folders = []

        self.register_in(registry)

    def register_in(self, registry):
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

    def __init__(self, registry, context, item,
                 reference_formatter, catalog, options):
        super(RepoFolderDiff, self).__init__(registry, context)
        self.item = item
        self.reference_formatter = reference_formatter
        self.catalog = catalog
        self.options = options
        self.child_documents = []

    def register_in(self, registry):
        super(RepoFolderDiff, self).register_in(registry)

        parent_context = aq_parent(aq_inner(self.context))
        parent_diff = registry['/'.join(parent_context.getPhysicalPath())]
        parent_diff.append_child_folder(self)

    def apply_recursively(self):
        self.fix_reference_number()
        super(RepoFolderDiff, self).apply_recursively()

    def get_repository_reference_number(self):
        reference = IReferenceNumber(self.context)
        formatter = getAdapter(self.context, IReferenceNumberFormatter,
                               name=self.reference_formatter)
        return formatter.repository_number(reference.get_parent_numbers())

    def fix_reference_number(self):
        reference_number = self.get_repository_reference_number()
        if reference_number != self.item['reference_number']:
            logger.warn('reference numbers differ for {}: '
                        '"{}" (site), "{}" (excel)'
                        .format(self.path,
                                reference_number,
                                self.item["reference_number"]))
            return

        if self.is_leaf_folder():
            self.fix_leaf_folder()
        else:
            self.fix_non_leaf_folder()

    def is_leaf_folder(self):
        child_folders = self.catalog.unrestrictedSearchResults(
            path={'query': '/'.join(self.context.getPhysicalPath()),
                  'depth': 1},
            object_provides=IRepositoryFolder.__identifier__)
        return len(child_folders) == 0

    def fix_non_leaf_folder(self):
        if self.options.verbose:
            logger.info('fixing non-leaf folder {}'
                        .format(self.item['_query_path']))

    def fix_leaf_folder(self):
        if self.options.verbose:
            logger.info('fixing leaf folder {}'
                        .format(self.item['_query_path']))


class RetentionPeriodFixer(XlsSource):

    def __init__(self, plone, options):
        self.plone = plone
        self.options = options
        self.profile = options.profile
        self.portal_setup = api.portal.get_tool('portal_setup')
        self.catalog = api.portal.get_tool('portal_catalog')
        self.diffs = {}

        registry = getUtility(IRegistry)
        proxy = registry.forInterface(IReferenceNumberSettings)
        self.reference_formatter = proxy.formatter

    def run(self):
        xlssource = FixerXlsSource(self.get_repository_file_path())
        source = FixerPathFromReferenceNumber(xlssource,
                                              self.reference_formatter)

        diff_root = RepoRootDiff(self.diffs, self.get_repo_root())
        for item in source:
            self.init_diff(item)

        diff_root.apply_recursively()

    def init_diff(self, item):
        path = item['_path'].lstrip('/').encode('utf-8')
        item['_query_path'] = path

        context = self.plone.unrestrictedTraverse(path, default=None)
        if not context:
            logger.warn('could not find repository folder: {}'.format(path))
            return

        RepoFolderDiff(self.diffs, context, item,
                       self.reference_formatter, self.catalog, self.options)

    def get_repository_file_path(self):
        profile_info = self.portal_setup.getProfileInfo(self.profile)
        profile_path = profile_info['path']

        repositories_folder = os.path.join(profile_path,
                                           REPOSITORIES_FOLDER_NAME)
        repository_filenames = [filename for filename in
                                os.listdir(repositories_folder)
                                if filename.endswith('.xlsx')]

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

    plone = setup_plone(app, options)
    RetentionPeriodFixer(plone, options).run()

    if options.dry_run:
        logger.warn('transaction doomed because we are in dry-mode.')
        transaction.doom()

    transaction.commit()


if __name__ == '__main__':
    main()
