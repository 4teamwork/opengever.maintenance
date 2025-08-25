#!/usr/bin/env python
"""
A script to add a specific group with given roles to a folder object in Plone.

Usage:
    bin/instance run add_group_to_folder.py folder_path group_id role [role ...] [-n]

Arguments:
    folder_path : The absolute path to the folder within the Plone site (e.g. "/Plone/myfolder").
    group_id    : The id of the group to add.
    role        : One or more roles to assign to the group.

Optional arguments:
    -n, --dry-run  : Run in dry-run mode (do not commit changes).
"""

import argparse
import logging
import transaction
from plone import api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("add_group_to_folder")


def main():
    parser = argparse.ArgumentParser(
        description="Add a specific group with given roles to a folder object in Plone."
    )
    parser.add_argument("folder_path", help="Absolute path to the folder (e.g. /Plone/myfolder)")
    parser.add_argument("group_id", help="ID of the group to add")
    parser.add_argument("roles", nargs="+", help="Role(s) to assign to the group on the folder")
    parser.add_argument(
        "-n", "--dry-run", action="store_true", default=False, help="Dry run mode, do not commit changes"
    )
    args = parser.parse_args()

    site = api.portal.get()
    folder = site.unrestrictedTraverse(args.folder_path, None)
    if folder is None:
        logger.error("Folder at path %s not found.", args.folder_path)
        return

    existing_roles = {}
    for principal, roles in folder.get_local_roles():
        existing_roles[principal] = list(roles)

    current_roles = existing_roles.get(args.group_id, [])
    logger.info("Current roles for group '%s': %s", args.group_id, current_roles)

    new_roles = list(current_roles)
    for role in args.roles:
        if role not in new_roles:
            new_roles.append(role)

    logger.info(
        "Setting local roles for group '%s' on folder '%s' to: %s",
        args.group_id, args.folder_path, new_roles
    )

    if not args.dry_run:
        folder.manage_setLocalRoles(args.group_id, new_roles)
        folder.reindexObjectSecurity()
        transaction.commit()
        logger.info("Local roles updated and transaction committed.")
    else:
        logger.info("Dry run mode: No changes have been committed.")


if __name__ == "__main__":
    main()
