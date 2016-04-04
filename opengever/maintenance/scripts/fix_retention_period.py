from opengever.base.interfaces import IReferenceNumberSettings
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.setup.sections.reference import PathFromReferenceNumberSection
from opengever.setup.sections.xlssource import XlsSource
from plone import api
from plone.i18n.normalizer.interfaces import IIDNormalizer
from plone.i18n.normalizer.interfaces import IURLNormalizer
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
from zope.component import queryUtility
import logging
import os
import os.path
import transaction


logger = logging.getLogger('opengever.maintenance')

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
    def __init__(self, previous):
        self.logger = logger
        self.previous = previous

        self.refnum_mapping = {}
        self.normalizer = queryUtility(IURLNormalizer, name="de")
        self.id_normalizer = queryUtility(IIDNormalizer)

        registry = getUtility(IRegistry)
        proxy = registry.forInterface(IReferenceNumberSettings)
        self.reference_formatter = proxy.formatter


class RetentionPeriodFixer(XlsSource):

    def __init__(self, plone, profile):
        self.plone = plone
        self.profile = profile
        self.portal_setup = api.portal.get_tool('portal_setup')

    def run(self):
        source = FixerPathFromReferenceNumber(
            FixerXlsSource(self.get_repository_file_path()))

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


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("-p", dest="profile",
                      help="profile that contains the repository excel file.")
    options, args = parser.parse_args()

    if not options.profile:
        print "the profile (-p) argument is required."
        return
    if ":" not in options.profile:
        print "invalid profile id: '{}', missing ':'".format(options.profile)
        return

    plone = setup_plone(app, options)
    RetentionPeriodFixer(plone, options.profile).run()

    if options.dry_run:
        print 'transaction doomed because we are in dry-mode.'
        print ''
        transaction.doom()

    transaction.commit()


if __name__ == '__main__':
    main()
