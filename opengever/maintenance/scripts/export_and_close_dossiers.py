"""
This script is used to export all the dossiers in a given repofolder and close
them.
"""
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.interfaces import IDossierResolver
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.workspaceclient.interfaces import IWorkspaceClientSettings
from plone import api
from zope.component import getAdapter
import logging
import os
import shutil
import sys
import time
import transaction

logger = logging.getLogger('opengever.maintenance')
logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.root.addHandler(handler)

TIMESTAMP = time.strftime('%d%m%Y-%H%M%S')


class PreconditionsError(Exception):
    """Raised when preconditions for the migration are not satisfied"""


class DisallowedReviewState(Exception):
    """Raised when trying to export a dossier not in an allowed final state"""


class DossierExporter(object):

    allowed_final_states = ['dossier-state-resolved', 'dossier-state-inactive']
    states_to_resolve = ['dossier-state-active']

    def __init__(self, context, output_directory, check_only=False, dont_close_dossiers=False):
        self.check_only = check_only
        self.output_directory = output_directory
        self.dont_close_dossiers = dont_close_dossiers
        self.context = context
        self.catalog = api.portal.get_tool("portal_catalog")

        # create output directory
        os.mkdir(self.output_directory)

    @property
    def dossiers(self):
        return self.catalog.unrestrictedSearchResults(
            path=self.context.absolute_url_path(),
            object_provides=IDossierMarker.__identifier__,
            is_subdossier=False)

    def __call__(self):
        # Temporary disable the workspaceclient, because zopemaster can not
        # use the workspaceclient.
        workspaceclient_flag = api.portal.get_registry_record(
            interface=IWorkspaceClientSettings, name='is_feature_enabled')
        if workspaceclient_flag:
            api.portal.set_registry_record(
                interface=IWorkspaceClientSettings, name='is_feature_enabled',
                value=False)

        self.check_preconditions()
        if self.check_only:
            return

        if not self.dont_close_dossiers:
            self.resolve_dossiers()

        self.export_dossiers()

        if workspaceclient_flag:
            api.portal.set_registry_record(
                interface=IWorkspaceClientSettings, name='is_feature_enabled',
                value=True)

    def check_preconditions(self):
        logger.info("Checking preconditions...")

        if self.dont_close_dossiers:
            logger.info("dont_close_dossiers: Skip checking dossier state and resolvability")
            return

        # All dossiers should be either inactive, resolved or active and resolvable.
        unresolvable_dossiers = []
        dossiers_in_bad_state = []
        for brain in self.dossiers:
            if brain.review_state in self.allowed_final_states:
                continue
            elif brain.review_state not in self.states_to_resolve:
                dossiers_in_bad_state.append(brain)
                continue

            obj = brain.getObject()
            resolver = getAdapter(obj, IDossierResolver, name="lenient")
            try:
                resolver.raise_on_failed_preconditions()
            except:
                unresolvable_dossiers.append(brain)

        if unresolvable_dossiers or dossiers_in_bad_state:
            self.log_and_write_table(
                unresolvable_dossiers,
                "Unresolvable dossiers",
                "unresolvable_dossiers")

            self.log_and_write_table(
                dossiers_in_bad_state,
                "Dossiers in bad review states",
                "dossiers_in_bad_state")

            raise PreconditionsError("Preconditions not satisfied")

        logger.info("Preconditions satisfied.")

    def resolve_dossiers(self):
        message = "Closing dossiers..."
        for brain in ProgressLogger(message, self.dossiers, logger):
            if brain.review_state not in self.states_to_resolve:
                continue

            obj = brain.getObject()
            resolver = getAdapter(obj, IDossierResolver, name="lenient")
            resolver.raise_on_failed_preconditions()
            resolver.resolve()
            obj.reindexObject()

        logger.info("Dossiers resolved.")

    def export_dossiers(self):
        message = "Exporting dossiers."
        for brain in ProgressLogger(message, self.dossiers, logger):
            if brain.review_state not in self.allowed_final_states and not self.dont_close_dossiers:
                raise DisallowedReviewState("Dossier in disallowed review state")
            self._export_dossier(brain.getObject())

    def _get_output_path(self, basedir, name, ext='', i=0):
        name = name.replace(u'/', u'_')
        if i > 0:
            output_path = os.path.join(basedir, u"{}_{}{}".format(name, i, ext))
        else:
            output_path = os.path.join(basedir, u"{}{}".format(name, ext))
        if os.path.exists(output_path):
            return self._get_output_path(basedir, name, ext, i=i+1)
        return output_path

    def _export_dossier(self, dossier):
        folder_path = self._get_output_path(self.output_directory, dossier.title)
        os.mkdir(folder_path)
        res = self.catalog.unrestrictedSearchResults(
            path=dossier.absolute_url_path(),
            object_provides=IBaseDocument.__identifier__)
        for brain in res:
            doc = brain.getObject()
            filename, ext = os.path.splitext(doc.get_filename())
            file_path = self._get_output_path(folder_path, filename, ext)
            shutil.copy2(doc.file._blob.committed(), file_path)

    def log_and_write_table(self, brains, title, filename):
        table = TextTable()
        table.add_row(
            (u"Path", u"Title", u"State"))

        for brain in brains:
            table.add_row((
                brain.getPath(),
                brain.title.replace(",", ""),
                brain.review_state))

        logger.info("\n{}".format(title))
        logger.info("\n" + table.generate_output() + "\n")

        log_filename = LogFilePathFinder().get_logfile_path(
            filename, extension="csv")
        with open(log_filename, "w") as logfile:
            table.write_csv(logfile)


def main():
    app = setup_app()
    setup_plone(app)

    parser = setup_option_parser()
    parser.add_option("--check-preconditions-only", dest="check_only", action="store_true", default=False)
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    parser.add_option("--dont-close-dossiers", action="store_true",
                      dest="dont_close_dossiers", default=False)
    parser.add_option(
        '-o', dest='output_directory',
        default='var/dossier_export-{}'.format(TIMESTAMP),
        help='Path to the export output directory')

    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print "Missing argument, please provide a path to a repofolder"
        sys.exit(1)

    if os.path.isdir(options.output_directory):
        logger.info("Output directory already exists")
        sys.exit(1)

    if not options.output_directory:
        logger.info("Invalid output directory")
        sys.exit(1)

    if options.dryrun:
        logger.info('Performing dryrun!\n')
        transaction.doom()

    path = args[0]
    context = app.unrestrictedTraverse(path)

    logger.info("Writing output to {}".format(options.output_directory))

    exporter = DossierExporter(
        context,
        options.output_directory,
        check_only=options.check_only,
        dont_close_dossiers=options.dont_close_dossiers)

    # setup logging to file in dossier export directory
    fileh = logging.FileHandler(os.path.join(options.output_directory, "export_dossier.log"), 'w')
    fileh.setFormatter(formatter)
    logging.root.addHandler(fileh)

    exporter()

    if not options.dryrun:
        logger.info("Committing...")
        transaction.commit()

    logger.info("Done!")


if __name__ == '__main__':
    main()
