"""
Script to reindex and store objects for bumblebee.

To reindex the objects run:

    bin/instance run ./scripts/bumblebee_installation.py -m reindex

To calculate checksums for objects archived in portal_repository run:

    bin/instance run ./scripts/bumblebee_installation.py -m history

To store the objects run:

    bin/instance run ./scripts/bumblebee_installation.py -m store

If you have to specify the path to your plone instance you can use following
parameter:

    -p <path/to/plonesite>

By default the timestamp won't be reset. That means, already stored objects
won't be stored again.

If you want to reset the timestamp and store all objects again you can
specify this with the following parameter:

    -r

For help-information type in the following:

    bin/instance run ./scripts/bumblebee_installation.py -h

"""
# Avoid import error for Products.Archetypes.BaseBTreeFolder
from Products.Archetypes import atapi  # noqa
from collective.indexing.monkey import unpatch as unpatch_collective_indexing
from ftw.bumblebee.document import DOCUMENT_CHECKSUM_ANNOTATION_KEY
from ftw.bumblebee.interfaces import IBumblebeeConverter
from ftw.bumblebee.interfaces import IBumblebeeDocument
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.base.archeologist import Archeologist
from opengever.bumblebee.interfaces import IGeverBumblebeeSettings
from opengever.core.debughelpers import get_first_plone_site
from opengever.core.debughelpers import setup_plone
from optparse import OptionParser
from plone import api
from zope.annotation.interfaces import IAnnotations
from zope.component import getUtility
import logging
import sys
import time
import transaction
from opengever.mail.mail import IOGMailMarker


# Set global logger to info - this is necessary for the log-output with
# bin/instance run.
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root_logger.addHandler(handler)

LOG = logging.getLogger('bumblebee-installation')


parser = OptionParser()

parser.add_option("-m", "--mode", dest="mode", type="choice",
                  help="REQUIRED: Specify the upgrade-mode.",
                  choices=['reindex', 'index-checksums', 'history', 'store', 'activate'],
                  metavar="reindex|history|store|activate")

parser.add_option("-r", "--reset-timestamp", dest="reset", default=False,
                  action="store_true",
                  help="If set to true, all objects will be reindexed again. "
                       "Otherwise it wont update already stored objects if "
                       "you restart the script",
                  metavar="True|False")

parser.add_option("-p", "--plone", dest="plone_path",
                  help="path to the plonesite.", metavar="path/to/platform")

parser.add_option("-c", "--config", dest="config",
                  help="Zope-Config (do not use this)")

parser.add_option("-i", "--intermediate-commit", dest="intermediate_commit",
                  default=None, type="int",
                  help="Intermediate commit every n processed elements. "
                       "Works with '-m history' and '-m reindex' at the "
                       "moment.")

parser.add_option("--skip-many-versions",
                  action="store_true",
                  default=False,
                  help="Skip objects with many (>50) versions when "
                       "calculating checksums for versions using -m history")


def main(app, argv=sys.argv[1:]):
    options, args = parser.parse_args()

    mode = options.mode.lower() if options.mode else None

    if not options.mode:
        parser.print_help()
        parser.error(
            'Please specify the "mode" with "bin/instance run <yourscript> -m '
            'reindex | history | store | activate"\n'
            )

    if options.plone_path:
        plone = app.unrestrictedTraverse(options.plone_path)
    else:
        plone = get_first_plone_site(app)

    setup_plone(plone)

    converter = getUtility(IBumblebeeConverter)

    if mode == 'reindex':
        LOG.info("Start indexing objects...")
        try:
            converter.reindex(intermediate_commit=options.intermediate_commit)
        except TypeError:
            if options.intermediate_commit:
                LOG.warn("Unsupported option intermediate_commit, updating "
                         "ftw.bumblebee is recommended.")
            converter.reindex()

        return transaction.commit()

    elif mode == 'index-checksums':
        LOG.info("Start indexing bumblebee checksums...")

        solr_enabled = api.portal.get_registry_record(
            'opengever.base.interfaces.ISearchSettings.use_solr',
            default=False)
        if not solr_enabled:
            unpatch_collective_indexing()
            LOG.info("Disabled collective.indexing")

        start = time.time()
        converter.index_checksums(intermediate_commit=options.intermediate_commit)
        transaction.commit()
        end = time.time()
        LOG.info("Finished indexing bumblebee checksums. Duration: %ss" % (
            end - start))
        return

    elif mode == 'history':
        LOG.info("Start creating checksums for portal repository ...")
        repository = api.portal.get_tool('portal_repository')
        catalog = api.portal.get_tool('portal_catalog')

        brains = catalog.unrestrictedSearchResults(
            {'object_provides': 'ftw.bumblebee.interfaces.IBumblebeeable'})

        skipped = []
        for index, brain in enumerate(ProgressLogger(
                'Create checksums for objects in portal repository', brains,
                logger=LOG)):

            obj = brain.getObject()
            versions = repository.getHistory(obj)

            if options.skip_many_versions:
                if len(versions) > 50:
                    LOG.info('Skipping object with more '
                             'than 50 versions: %s' % obj.id)
                    skipped.append((obj.id, obj.absolute_url()))
                    continue

            LOG.info('  Calculating version checksums for %s' % obj.id)
            if IOGMailMarker.providedBy(obj):
                if len(versions) > 0:
                    LOG.warning('Found mail with versions: {}'.format('/'.join(obj.getPhysicalPath())))
                continue

            for version in versions:
                # we have to calculate the checksum on the "restored" object
                # returned by `portal_repository`. The archived object does not
                # contain an accessible file without `portal_repository` magic.
                version_checksum = IBumblebeeDocument(version.object).calculate_checksum()

                archived_obj = Archeologist(obj, version).excavate()
                annotations = IAnnotations(archived_obj)
                annotations[DOCUMENT_CHECKSUM_ANNOTATION_KEY] = version_checksum
                archived_obj._p_changed = True

            if options.intermediate_commit and index > 0:
                if index % options.intermediate_commit == 0:
                    LOG.info("Committing at {} documents.".format(index))
                    transaction.commit()

        transaction.commit()

        if skipped:
            LOG.warn('The following objects have been skipped:')
            for obj_id, obj_url in skipped:
                LOG.info("%s - %s" % (obj_id, obj_url))
            LOG.info('Skipped: %r' % [obj_id for obj_id, obj_url in skipped])

        return

    elif mode == 'store':
        LOG.info("Start storing objects...")
        if not options.reset:
            LOG.warning(
                "You started storing without reseting the timestamp. "
                "Already converted objects will be skipped.")

        return converter.store(deferred=True, reset_timestamp=options.reset)

    elif mode == 'activate':
        api.portal.set_registry_record(
            'is_feature_enabled', True, interface=IGeverBumblebeeSettings)
        LOG.info("activating bumblebee feature in registry.")
        return transaction.commit()

    else:
        parser.print_help()
        parser.error('You entered an invalid mode: {}\n'.format(mode))


if __name__ == '__main__':
    main(app, sys.argv[1:])
