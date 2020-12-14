"""
This script fixes persistent remnants of the ILinkedWorkspacesMarker interface
that was removed from the code, but not properly deleted from the DB.

    bin/instance run fix_broken_linked_workspace_marker_interface.py

Loosely based on the upgrade step in
https://github.com/4teamwork/opengever.core/blob/2020.1.0/opengever/core/upgrades/20171024114128_cleanup_grokcore_component_interfaces_i_context_interfaces_from_relation_catalog/upgrade.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zc.relation.interfaces import ICatalog
from zope.component import getUtility
import logging
import transaction


logger = logging.getLogger('opengever.maintenance')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


TO_REMOVE = ['opengever.workspaceclient.interfaces.ILinkedWorkspacesMarker']


def fix_broken_linked_workspace_marker_interface(plone, options):
    print '\n' * 4
    logger.warning('Fixing issues with removed ILinkedWorkspacesMarker interface...')
    fix_relation_catalog(options)
    fix_object_provides(options)


def fix_relation_catalog(options):
    print
    logger.warning('Checking relation catalog')
    for iface_name in TO_REMOVE:
        cleanup_to_mapping(options, iface_name, 'to_interfaces_flattened')
        cleanup_to_mapping(options, iface_name, 'from_interfaces_flattened')
        cleanup_objtokenset(
            options, ['from_interfaces_flattened', 'to_interfaces_flattened'])
    print


def fix_object_provides(options):
    print
    logger.warning('Checking portal catalog object_provides')
    catalog = api.portal.get_tool('portal_catalog')
    for broken_iface_name in TO_REMOVE:
        broken_brains = catalog.unrestrictedSearchResults(object_provides=broken_iface_name)
        if broken_brains:
            logger.warning(
                'Found %s objects that need reindexing of '
                'object_provides' % len(broken_brains))

            if not options.dryrun:
                for brain in broken_brains:
                    logger.warning('  Reindexing %s' % brain.getPath())
                    brain.getObject().reindexObject(idxs=['object_provides'])
    print


def cleanup_to_mapping(options, broken_iface_name, mapping_key):
    # Check the BTree consistency, to avoid key errors when deleting
    # entries in the BTree.
    # If the check method detects an inconsistency, we fix the Btree by
    # creating a copy of it.
    #
    # See http://do3.cc/blog/2012/09/264/debugging-zcrelations---broken-btrees/
    # for more information.

    relcat = getUtility(ICatalog)
    btree = relcat._name_TO_mapping[mapping_key]

    broken_keys = [key for key in btree.keys() if broken_iface_name in str(key)]

    if broken_keys:
        logger.warning(
            'Found broken interface %s as key of BTree '
            'relcat._name_TO_mapping[%r]' % (broken_iface_name, mapping_key))

        if not options.dryrun:
            for iface in relcat._name_TO_mapping[mapping_key].keys():
                if dottedname(iface) == broken_iface_name:
                    logger.warning(
                        '  Deleting interface %s from '
                        'relcat._name_TO_mapping[%r]' % (
                            broken_iface_name, mapping_key))
                    del relcat._name_TO_mapping[mapping_key][iface]
                    break


def cleanup_objtokenset(options, attribute_names):
    relcat = getUtility(ICatalog)

    for (key, value) in relcat._reltoken_name_TO_objtokenset.items():
        token, name = key
        if name in attribute_names:
            broken_values = [iface for iface in value
                             if dottedname(iface) in TO_REMOVE]
            if broken_values:
                logger.warning(
                    'Found broken interface in _reltoken_name_TO_objtokenset: '
                    'token=%r, name=%r, broken_values=%r' % (
                        token, name, broken_values))

            if not options.dryrun:
                for broken in broken_values:
                    logger.warning('  Removing %r' % broken)
                    value.remove(broken)


def dottedname(cls):
    return '{}.{}'.format(cls.__module__, cls.__name__)


def parse_options():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':
    app = setup_app()

    options, args = parse_options()

    if options.dryrun:
        transaction.doom()

    plone = setup_plone(app, options)

    fix_broken_linked_workspace_marker_interface(plone, options)

    if not options.dryrun:
        logger.info('Committing transaction...')
        transaction.commit()
        logger.info('Done.')
