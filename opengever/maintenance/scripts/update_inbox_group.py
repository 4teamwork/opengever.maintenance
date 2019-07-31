"""A script to migrate role assignments of tasks and related documents and
the distinct parent, after changing the inbox_group of an org_unit.

See https://extranet.4teamwork.ch/support/stadt-nidau/tracker-gever/140

bin/instance run src/opengever.maintenance/opengever/maintenance/scripts/update_inbox_group.py 'sd' 'GG_NID_SD' 'GG_NID_SD_Bereichsleiter'
"""

from opengever.base.oguid import Oguid
from opengever.base.role_assignments import ASSIGNMENT_VIA_TASK
from opengever.base.role_assignments import ASSIGNMENT_VIA_TASK_AGENCY
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.localroles import LocalRolesSetter
from opengever.task.task import ITask
from plone import api
import argparse
import sys
import transaction


def main():
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('org_unit', help='Org Unit ID')
    parser.add_argument('old_inbox_group', help='Old inbox group ID')
    parser.add_argument('new_inbox_group', help='New inbox group ID')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-n', dest='dryrun', default=False, help='Dryrun')

    options = parser.parse_args(sys.argv[3:])
    setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    migrator = InboxGroupMigrator(
        options.org_unit, options.old_inbox_group, options.new_inbox_group)
    migrator.migrate()

    if not options.dryrun:
        transaction.commit()


class InboxGroupMigrator(object):

    def __init__(self, org_unit, old_inbox_group, new_inbox_group):
        self.org_unit = org_unit
        self.old_inbox_group = old_inbox_group
        self.new_inbox_group = new_inbox_group

    def migrate(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(object_provides=ITask.__identifier__)
        org_units_tasks = [brain.getObject() for brain in brains
                           if brain.getObject().responsible_client == self.org_unit]

        for task in org_units_tasks:
            changed = self.migrate_role_assignments(task)
            if changed:
                task_oguid = Oguid.for_object(task).id
                # related items
                for item in task.relatedItems:
                    self.migrate_role_assignments(item.to_object, reference_oguid=task_oguid)

                # distinct parent
                distinct_parent = LocalRolesSetter(task).get_distinct_parent()
                self.migrate_role_assignments(distinct_parent, reference_oguid=task_oguid)

                print 'Assigments for {} migrated'.format(task.absolute_url())

    def migrate_role_assignments(self, obj, reference_oguid=None):
        manager = RoleAssignmentManager(obj)
        assignments = manager.get_assignments_by_principal_id(self.old_inbox_group)
        changed = False

        for assignment in assignments:
            if reference_oguid == assignment.reference:
                continue

            if assignment.cause in [ASSIGNMENT_VIA_TASK, ASSIGNMENT_VIA_TASK_AGENCY]:
                reference = Oguid.parse(assignment.reference).resolve_object()
                manager.add_or_update(self.new_inbox_group, assignment.roles,
                                      assignment.cause, reference)
                manager.clear(assignment.cause, self.old_inbox_group, reference)
                changed = True

        return changed


if __name__ == '__main__':
    main()
