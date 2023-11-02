"""
A scripts wich fixes the forward allowedRolesAndPrincipals index based on
the backward index. This can be used to fix deployments that were migrated
with the migrate_groups.py script when it still contained a bug leading to
inconsistent data in the allowedRolesAndPrincipals index.

Usage:

bin/instance0 run fix_allowed_roles_and_users_after_group_migration.py mapping

  - mapping is a path to a json file containing the group mapping (dictionary
    with old group names as keys and new group names as values).

optional arguments:
  -s : siteroot
  -n : dry-run
"""
from BTrees.IIBTree import IITreeSet
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import argparse
import json
import logging
import os
import sys
import transaction

logger = logging.getLogger('fix_allowed_roles_and_users')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


class AllowedRolesAndUsersFixer(object):

    def __init__(self, options):
        self.options = options
        with open(options.mapping, "r") as fin:
            self.group_mapping = json.load(fin)
        self.old_group_ids = set(self.group_mapping.keys())
        self.catalog = api.portal.get_tool('portal_catalog')

    def __call__(self):
        """As data is correct in the _unindex, we walk over that index and
        collect all the rids for the migrated groups and then use that to fix
        the _index.

        Forward index is of the form _index[principal] = [docid1, docid2]
        Backward index is of the form _unindex[docid] = [principal1, principal2]
        """
        index = self.catalog._catalog.indexes["allowedRolesAndUsers"]

        principal_mapping = {"user:{}".format(key): "user:{}".format(value)
                             for key, value in self.group_mapping.items()}
        new_principal_ids = set(principal_mapping.values())

        new_rids_mapping = dict(((principal, IITreeSet()) for principal in new_principal_ids))

        for rid, principals in index._unindex.items():
            to_update = set(principals).intersection(new_principal_ids)
            for principal in to_update:
                new_rids_mapping[principal].add(rid)

        for principal, new_rids in new_rids_mapping.items():
            old_rids = set(index._index.get(principal, set()))
            if set(new_rids) != old_rids:
                to_add = set(new_rids).difference(old_rids)
                to_remove = old_rids.difference(set(new_rids))
                logger.info(u"\nUpdating index for {}".format(principal))
                logger.info(u"Adding {}".format(to_add))
                logger.info(u"Removing {}".format(to_remove))
                index._index[principal] = new_rids


def main():

    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('mapping', help='Path to json file containing a '
                        'mapping from old to new groups.')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-n', dest='dryrun', default=False, help='Dryrun')

    options = parser.parse_args(sys.argv[3:])

    setup_plone(app, options)
    logger.info('Fixing allowedRolesAndUsers index')

    if options.dryrun:
        transaction.doom()
        logger.info('Dryrun enabled')

    if not os.path.isfile(options.mapping):
        raise ValueError("{} is not a file.".format(options.mapping))

    AllowedRolesAndUsersFixer(options)()

    if not options.dryrun:
        logger.info('Committing...')
        transaction.commit()

    logger.info("All done")


if __name__ == '__main__':
    main()
