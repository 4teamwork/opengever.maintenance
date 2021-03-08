"""
This script verifies that all persistent data related to pdfconverter has been
removed.

Example Usage:

    bin/instance run verify_pdfconverter_removal.py
"""
from collections import defaultdict
from opengever.base.archeologist import Archeologist
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFPlone.utils import getToolByName
from zc.relation.interfaces import ICatalog
from zope.annotation import IAnnotations
from zope.component import getUtility
import logging
import re
import time
import transaction


IPREVIEW_MARKER = 'opengever.pdfconverter.behaviors.preview.IPreviewMarker'
RELCAT_MAPPINGS = ('to_interfaces_flattened', 'from_interfaces_flattened')
ANNOTATION_KEYS = (
    'opengever.pdfconverter.behaviors.preview.IPreview.conversion_state',
    'opengever.pdfconverter.behaviors.preview.IPreview.preview_file',
)
CLONED_NAMED_FILE_BLOB_KEY = (
    'CloneNamedFileBlobs/opengever.pdfconverter'
    '.behaviors.preview.IPreview.preview_file')

logger = logging.getLogger('opengever.maintenance')
logger.setLevel(logging.INFO)
logger.root.handlers[0].setLevel(logging.INFO)


class RemovalChecker(object):
    def __init__(self, portal):
        self.portal = portal

    def run(self):
        start = time.time()

        all_leftovers = defaultdict(list)

        self.catalog = api.portal.get_tool('portal_catalog')
        self.repository = api.portal.get_tool('portal_repository')

        all_leftovers['relcat'] = self.check_relation_catalog()

        query = {'portal_type': 'opengever.document.document'}

        brains = self.catalog(query)
        for i, brain in enumerate(brains):
            obj = brain.getObject()
            obj_path = '/'.join(obj.getPhysicalPath())

            logger.info("Checking %r" % obj_path)

            ann = IAnnotations(obj)
            removal_flag = ann.get('pdf_converter_removal_complete', False)
            if not removal_flag:
                # Just log presence of flag as a debugging hint, but don't
                # include objects missing it in summary statistics. Newly
                # created objects won't have the flag anyway, so reporting
                # them would just be misleading.
                logger.warn("pdf_converter_removal_complete flag not set")

            all_leftovers['annotations'].extend(self.check_obj_annotations(obj))

            shadow_history = self.repository.getHistoryMetadata(obj)
            if shadow_history:
                logger.info("%s versions for %r" % (len(shadow_history), obj))
                logger.info("Checking shadow history for %r" % obj)
                all_leftovers['versions_namedfile'].extend(self.check_namedfile_in_versions(obj, shadow_history))
                all_leftovers['versions_annotations'].extend(self.check_annotations_in_versions(obj, shadow_history))
                logger.info("DONE Checking shadow history for %r" % obj)

            if i % 100 == 0:
                # Regularly rollback txn to avoid performance hit by
                # CMFEditions' savepoint shenanigans
                logger.info("Intermediate rollback at %s items" % i)
                transaction.abort()
                transaction.doom()
                logger.info("Done")

        all_leftovers['uninstallation'] = self.check_profile_and_product_uninstalled()
        all_leftovers['object_provides'] = self.check_object_provides_reindexed()

        end = time.time()
        duration = end - start
        logger.info("\n\n")
        logger.info("=" * 70)
        logger.info("Total Duration: %.2fs" % duration)

        logger.info("\n\nLeftovers:\n")
        for key, leftover_items in sorted(all_leftovers.items()):
            logger.info('%s: %s' % (key, len(leftover_items)))
            for item in leftover_items:
                logger.info('    %s' % (item, ))

        total = sum(map(len, all_leftovers.values()))
        if total == 0:
            logger.info('No leftovers found, all good')
        else:
            logger.warn('Found %s leftovers total' % total)

    def check_relation_catalog(self):
        leftovers = []

        relcat = getUtility(ICatalog)

        def get_ifaces_to_remove(ifaces):
            return [i for i in ifaces if 'opengever.pdfconverter' in str(i)]

        logger.info("Checking interfaces in relation catalog")

        # Check mappings
        for mapping_name in RELCAT_MAPPINGS:
            logger.info("Checking mapping %s" % mapping_name)
            mapping = relcat._name_TO_mapping[mapping_name]
            ifaces_to_remove = get_ifaces_to_remove(mapping.keys())

            for iface in ifaces_to_remove:
                leftovers.append(('_name_TO_mapping', mapping_name, repr(iface)))
                logger.warn("Found left over %r " % iface)

        # Check objtokensets
        logger.info("Checking objtokensets")
        for (key, tokenset) in relcat._reltoken_name_TO_objtokenset.items():
            token, mapping_name = key
            if mapping_name in RELCAT_MAPPINGS:
                ifaces_to_remove = get_ifaces_to_remove(tokenset)

                for iface in ifaces_to_remove:
                    leftovers.append(('_reltoken_name_TO_objtokenset', token, mapping_name, repr(iface)))
                    logger.info("WARNING: Found left over %r " % iface)

        return leftovers

    def check_obj_annotations(self, obj):
        leftovers = []

        logger.info("  Checking pdfconverter related annotations")
        ann = IAnnotations(obj)

        for key in ANNOTATION_KEYS:
            if key in ann:
                obj_path = '/'.join(obj.getPhysicalPath())
                leftovers.append((obj_path, key))
                logger.warn("    Found left over annotation key: %r" % key)

        return leftovers

    def check_namedfile_in_versions(self, obj, shadow_history):
        leftovers = []

        numvers = len(shadow_history)
        logger.info("  Checking preview_file in %s versions" % numvers)

        for i, key in enumerate(shadow_history._full):
            logger.info("    Checking namedfile in version %s (version_id: %s)" % (i, key))
            referenced_data = shadow_history._full[key].get('referenced_data', {})

            if CLONED_NAMED_FILE_BLOB_KEY in referenced_data:
                obj_path = '/'.join(obj.getPhysicalPath())
                leftovers.append((obj_path, key, CLONED_NAMED_FILE_BLOB_KEY))
                logger.warn("    Found leftover namedfile reference")

        return leftovers

    def check_annotations_in_versions(self, obj, shadow_history):
        leftovers = []

        numvers = len(shadow_history)
        logger.info("  Checking annotations in %s versions" % numvers)

        for version_number in range(len(shadow_history)):
            logger.info("    Checking annotations in version %s/%s" % (version_number, numvers))
            archeologist = Archeologist(
                obj, self.repository.retrieve(obj, selector=version_number))

            archived_obj = archeologist.excavate()
            archived_ann = IAnnotations(archived_obj)

            for key in ANNOTATION_KEYS:
                if key in archived_ann:
                    obj_path = '/'.join(obj.getPhysicalPath())
                    leftovers.append((obj_path, version_number, key))
                    logger.warn("    Found left over annotations")

        return leftovers

    def check_profile_and_product_uninstalled(self):
        leftovers = []
        profile_name = 'opengever.pdfconverter:default'

        if self.is_profile_installed(profile_name):
            leftovers.append(('profile', profile_name))

        return leftovers

    def is_profile_installed(self, profileid):
        """Checks whether a generic setup profile is installed.
        Respects product uninstallation via quickinstaller.

        (copied from ftw.upgrade)
        """
        profileid = re.sub(r'^profile-', '', profileid)

        portal_setup = api.portal.get_tool('portal_setup')

        try:
            profileinfo = portal_setup.getProfileInfo(profileid)
        except KeyError:
            return False

        if not self.is_product_installed(profileinfo['product']):
            return False

        version = portal_setup.getLastVersionForProfile(profileid)
        return version != 'unknown'

    def is_product_installed(self, product_name):
        """Check whether a product is installed (copied from ftw.upgrade)
        """
        try:
            from Products.CMFPlone.utils import get_installer
        except ImportError:
            get_installer = None

        if get_installer is not None:
            quickinstaller = get_installer(self.portal, self.portal.REQUEST)
            return (quickinstaller.is_product_installable(product_name)
                    and quickinstaller.is_product_installed(product_name))
        else:
            quickinstaller = getToolByName(self.portal, 'portal_quickinstaller')
            return (quickinstaller.isProductInstallable(product_name)
                    and quickinstaller.isProductInstalled(product_name))

    def check_object_provides_reindexed(self):
        brains = self.catalog.unrestrictedSearchResults(object_provides=IPREVIEW_MARKER)
        leftovers = [brain.getPath() for brain in brains]
        return leftovers


def verify_pdfconverter_removal(plone, options):
    checker = RemovalChecker(plone)
    checker.run()


def parse_options():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':
    app = setup_app()

    options, args = parse_options()
    plone = setup_plone(app, options)

    transaction.doom()

    verify_pdfconverter_removal(plone, options)
