"""
This script is able to reindex the local roles of a specific object and all of its children
in a transaction safe way. This allows us to update a huge number of local roles
during office hours

    bin/instance run src/opengever.maintenance/opengever/maintenance/scripts/reindex_allowed_roles_and_users.py --collect --reindex

The script can also be used to just collect or reindex the local roles or to add or remove groups to an object.
This is useful for some maintenance work:

Example how to add a group and reindex the root object.:

    bin/instance run src/opengever.maintenance/opengever/maintenance/scripts/reindex_allowed_roles_and_users.py --add-group MY_DUMMY_GROUP  --add-group-roles Contributor Editor Reader Reviewer --collect --reindex

Example how to remove a group without reindexing:

    bin/instance run src/opengever.maintenance/opengever/maintenance/scripts/reindex_allowed_roles_and_users.py --remove-group MY_DUMMY_GROUP

Usage:

Help: bin/instance run reindex_allowed_roles_and_users.py -h

"""
from datetime import datetime
from opengever.base.role_assignments import ASSIGNMENT_VIA_SHARING
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.base.role_assignments import SharingRoleAssignment
from opengever.base.security import reindex_object_security_without_children
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.scripts.item_processor import Processor
from plone import api
from Products.CMFPlone.CatalogTool import allowedRolesAndUsers
import argparse
import json
import os
import sys
import transaction


class Collector(object):
    """Collects outdated objects and provides some stats
    """

    def __init__(self, root_obj):
        self.portal = api.portal.get()
        self.root_obj = root_obj

        self.catalog = api.portal.get_tool('portal_catalog')
        self.brains = self.catalog.unrestrictedSearchResults(
            path='/'.join(root_obj.getPhysicalPath()))

        self.outdated_uids = set()
        self.users_and_groups_out_of_sync = set()

    def collect(self):
        Processor().run(self.brains, batch_size=1000, process_item_method=self.process_item)

    def process_item(self, brain):
        obj = brain.getObject()
        index_value = self.catalog.getIndexDataForRID(brain.getRID()).get('allowedRolesAndUsers')
        obj_value = allowedRolesAndUsers(obj)()
        if set(index_value) == set(obj_value):
            return

        self.outdated_uids.add(obj.UID())
        self.users_and_groups_out_of_sync.update(
            set(obj_value).symmetric_difference(set(index_value)))

    def print_stats(self):
        print("Collector Stats")
        print("-" * 50)
        if self.outdated_uids:
            print("Found unhealthy objects: {}".format(len(self.outdated_uids)))
            print("The following users and groups are not in sync: {}".format(
                self.users_and_groups_out_of_sync))
        else:
            print("All objects are in sync!")
        print("#" * 50)

    def collected_uids(self):
        return list(self.outdated_uids)


class Reindexer(object):
    """Reindexes the allowedRolesAndUsers
    """
    def __init__(self, data_persistor, dry_run=False):
        self.dry_run = dry_run
        self.catalog = api.portal.get_tool('portal_catalog')
        self.data_persistor = data_persistor
        self.remaining_uids = data_persistor.json_load()
        self.brains = self.catalog.unrestrictedSearchResults({"UID": self.remaining_uids})

    def reindex(self):
        Processor().run(self.brains, dry_run=self.dry_run,
                        process_item_method=self.process_item,
                        batch_committed_method=self.on_batch_committed)

    def process_item(self, brain):
        reindex_object_security_without_children(brain.getObject())
        try:
            self.remaining_uids.remove(brain.UID)
        except ValueError:
            pass

    def on_batch_committed(self):
        self.data_persistor.json_dump(self.remaining_uids)

    def print_stats(self):
        print("Reindexer Stats")
        print("-" * 50)
        print("Reindexed: {} brains".format(len(self.brains)))
        print("#" * 50)


class MaintenanceDataPersistor(object):
    def __init__(self, file_path=None, slug="default"):
        self.file_path = file_path

        if not file_path:
            dir_path = "var/maintenance_tmp_data"
            ts = datetime.now().strftime('%Y-%d-%m_%H_%M_%S')
            file_name = 'maintenance_{}_{}.json'.format(slug, ts)
            self.file_path = os.path.join(dir_path, file_name)

        if not os.path.exists(os.path.dirname(self.file_path)):
            os.makedirs(os.path.dirname(self.file_path))

    def json_dump(self, data):
        print("Persisting data to: {}".format(self.file_path))
        with open(self.file_path, 'w+') as json_file:
            json.dump(data, json_file, indent=4)

    def json_load(self):
        print("Loading data from: {}".format(self.file_path))
        with open(self.file_path) as json_file:
            return json.load(json_file)

    def print_stats(self):
        print("DataPersistor Stats")
        print("-" * 50)
        print("Written data to filepath: {}".format(self.file_path))
        print("#" * 50)


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser(
        description="Reindex local roles of a specific root path")
    parser.add_argument('-n', dest='dry_run', default=False, help='Dryrun')
    parser.add_argument(
        '--site-root',
        dest='site_root',
        default=None,
        help='Absolute path to the Plone site')
    parser.add_argument(
        '--root-node-path',
        default='ordnungssystem',
        help='Path to root node of subtree to be exported')
    parser.add_argument(
        '--add-group',
        default=None,
        help="Add the given group name to the root node object")
    parser.add_argument(
        '--add-group-roles',
        nargs='+',
        default=['Contributor', 'Editor', 'Reader', 'Reviewer'],
        help="Assign the given roles to the group defined with --add-group")
    parser.add_argument(
        '--remove-group',
        default=None,
        help="Removes the given group name from the root node object")
    parser.add_argument(
        '--collect',
        action='store_true',
        help="Collect objs with outdated object security")
    parser.add_argument(
        '--reindex',
        action='store_true',
        help="Reindex the object security")
    parser.add_argument(
        '--data-file-path',
        default=None,
        help="Path to the data-json for storing and retrieving the UIDs")

    options = parser.parse_args(sys.argv[3:])
    plone = setup_plone(app, options)

    if options.reindex and not options.collect and not options.data_file_path:
        sys.exit("Please set a data_file_path if you don't want to previously "
                 "collect the items with --collect")

    answer = raw_input("Running with dry-run: {}. Are you sure? (Y/n)".format(options.dry_run)) or 'y'
    if answer.lower() not in ["y", "yes"]:
        sys.exit("Aborted by the user")

    if options.dry_run:
        print("Dry run: true")
        transaction.doom()
    else:
        print("Dry run: false")

    root_obj = plone.unrestrictedTraverse(options.root_node_path)

    if not options.add_group and not options.remove_group and \
            not options.collect and not options.reindex:
        sys.exit("Nothing to do... exiting the script.")

    # Remove local roles
    if options.remove_group:
        print("I will remove the group: {} from the object: {}".format(
            options.remove_group,
            root_obj.absolute_url(),
        ))
        answer = raw_input("Are you sure? (Y/n)") or 'y'
        if answer.lower() in ["y","yes"]:
            RoleAssignmentManager(root_obj).storage.clear_by_cause_and_principal(
                ASSIGNMENT_VIA_SHARING, options.remove_group)
            RoleAssignmentManager(root_obj)._update_local_roles(reindex=False)
            print("Successfully updated the local roles without reindex the security index")
        else:
            sys.exi("Aborted by the user.")

    # Add local roles
    if options.add_group:
        print("I will add the group: {} to the object: {} with the following roles: {}".format(
            options.add_group,
            root_obj.absolute_url(),
            options.add_group_roles,
        ))
        answer = raw_input("Are you sure? (Y/n)") or 'y'
        if answer.lower() in ["y","yes"]:
            assignment = SharingRoleAssignment(options.add_group, options.add_group_roles)
            RoleAssignmentManager(root_obj).add_or_update_assignment(assignment, reindex=False)
            print("Successfully updated the local roles without reindex the security index")
        else:
            sys.exi("Aborted by the user.")

    data_persistor = MaintenanceDataPersistor(options.data_file_path)

    # Collecting oudated objects
    collector = None
    if options.collect:
        collector = Collector(
            root_obj=root_obj
        )

        print("Start collecting objects having an outdated allowedRolesAndUsers index")
        collector.collect()

        data_persistor.json_dump(collector.collected_uids())

    # Reindex oudated objects
    reindexer = None
    if options.reindex:
        reindexer = Reindexer(
            data_persistor=data_persistor,
            dry_run=options.dry_run
        )
        reindexer.reindex()

    if not options.dry_run:
        print("Committing...")
        transaction.commit()
        print("Transaction committed. Everything done!")

    print("#" * 50)
    print("Overall stats")
    print("#" * 50)

    if collector:
        collector.print_stats()

    if reindexer:
        reindexer.print_stats()

    data_persistor.print_stats()
