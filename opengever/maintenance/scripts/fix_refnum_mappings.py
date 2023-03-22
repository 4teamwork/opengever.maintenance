"""
A bin/instance run script for https://4teamwork.atlassian.net/browse/CA-4356,
which fixes the reference number mappings.
"""

from Acquisition import aq_base
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberPrefix
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
import logging
import sys
import transaction

logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


def mapping_needs_update(obj):
    intids = getUtility(IIntIds)
    ref_adapter = IReferenceNumberPrefix(obj)
    prefix_mapping = ref_adapter.get_prefix_mapping(obj)
    children_intids = set()
    for child in obj.listFolderContents():
        intid = intids.getId(aq_base(child))
        children_intids.add(intid)
        if not prefix_mapping.get(intid) == IReferenceNumber(child).get_local_number():
            return True

    # If there are intids in the prefix mapping that are not children,
    # they should be of objects that were deleted
    for intid in set(prefix_mapping.keys()) - children_intids:
        obj = intids.queryObject(intid)
        if obj is not None:
            return True

    mapping_intids = set()
    for intid in ref_adapter.get_child_mapping().values():
        # if the object has been deleted, then it can remain in the mapping
        # and will be displayed as freeable. That is a normal situation
        obj = intids.queryObject(intid)
        if obj:
            mapping_intids.add(intid)

    if not children_intids == mapping_intids:
        return True

    return False


def regenerate_reference_number_mapping(obj):
    logger.info('Updating mapping for {}'.format(obj.absolute_url_path()))
    ref_adapter = IReferenceNumberPrefix(obj)
    logger.info('Current mapping {}'.format(ref_adapter.get_number_mapping()))

    # This purges also the dossier mapping, but the parents does not
    # contain any dossier otherwise something is wrong and an
    # exception will be raised when looping over the childs.
    ref_adapter.purge_mappings()

    for child in obj.listFolderContents():
        if not IRepositoryFolder.providedBy(child):
            raise Exception(
                'A parent of a repositoryfolder contains dossiers')
        ref_adapter.set_number(
            child, number=IReferenceNumber(child).get_local_number())

    logger.info('New mapping {}\n'.format(ref_adapter.get_number_mapping()))


def fix_refnum_mappings(plone):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.repository.repositoryroot',
                     'opengever.repository.repositoryfolder']
    )

    n_tot = len(brains)
    n_updated = 0
    logger.info('\n\nFixing {} reference number mappings\n'.format(n_tot))

    for i, brain in enumerate(brains, 1):
        if i % 10 == 0:
            logger.info(u'Done {} / {}'.format(i, n_tot))
        obj = brain.getObject()
        if hasattr(obj, 'is_leaf_node') and obj.is_leaf_node():
            continue
        if mapping_needs_update(obj):
            n_updated += 1
            regenerate_reference_number_mapping(obj)

    print 'Updated a total of {} mappings'.format(n_updated)


def reindex_repository_folders(plone):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.repository.repositoryroot',
                     'opengever.repository.repositoryfolder']
    )

    n_tot = len(brains)
    logger.info('\n\nReindexing {} reference numbers\n'.format(n_tot))

    for i, brain in enumerate(brains, 1):
        if i % 100 == 0:
            logger.info(u'Done {} / {}'.format(i, n_tot))
        obj = brain.getObject()
        obj.reindexObject()

    print 'Reindexed {} reference numbers'.format(n_tot)


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    parser.add_option("-r", "--reindex", action="store_true",
                      dest="reindex", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(setup_app())
    fix_refnum_mappings(plone)

    if options.reindex:
        logger.info("reindexing...")
        reindex_repository_folders(plone)

    if not options.dryrun:
        logger.info("Committing...")
        transaction.commit()
    else:
        logger.info("Dry run, not committing.")

    logger.info("Done!")


if __name__ == '__main__':
    main()
