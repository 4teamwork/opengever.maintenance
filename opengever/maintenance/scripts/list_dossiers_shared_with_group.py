"""
Searches for all resolved dossiers, on which local_roles were set for a given
group via sharing.

    bin/instance run ./scripts/list_dossiers_shared_with_group.py group_name

mandatory arguments:
  group_name : name of the group for which we will search dossiers.

"""
from ftw.upgrade.utils import SavepointIterator
from opengever.base.role_assignments import ASSIGNMENT_VIA_SHARING
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from plone import api
import sys


class DossierSharedWithGroupLister(object):

    def __init__(self, group_id):
        self.table = TextTable(col_max_width=60)
        self.table.add_row(["dossier path", "dossier title", "roles"])
        self.group_id = group_id

    def list_dossiers_shared_with_group(self):
        for dossier, assignment in self.get_dossiers_shared_with_group():
            self.table.add_row([dossier.absolute_url_path(), dossier.Title(),
                                "; ".join(map(str, assignment.roles))])

    def print_table(self):
        print("Table of dossiers shared with {}".format(self.group_id))
        print(self.table.generate_output())
        print("\nSummary:")
        print("There are {} dossiers shared with {}".format(self.table.nrows,
                                                            self.group_id))

    def dossier_iterator(self):
        """We use the SavepointIterator to better control memory consumption
        """
        dossier_brains = api.content.find(allowedRolesAndUsers=u'user:{}'.format(self.group_id))

        ndossiers = len(dossier_brains)
        print("found {} dossiers".format(ndossiers))

        dossier_generator = (brain.getObject() for brain in dossier_brains)
        for i, dossier in enumerate(SavepointIterator(dossier_generator, 500)):
            if i % 5000 == 0:
                print("done with {}/{}; {:.2}%".format(i, ndossiers,
                                                       100. * i / ndossiers))
            yield dossier

    def get_dossiers_shared_with_group(self):
        """ Searches for all dossier shared with a given group.
        We make the assumption that the group has at least view permissions
        on such dossiers.
        """
        for dossier in self.dossier_iterator():
            assignments = RoleAssignmentManager(dossier).get_assignments_by_principal_id(self.group_id)
            sharing_assignment = self._find_sharing_assignment(assignments)
            if sharing_assignment:
                yield (dossier, sharing_assignment)

    @staticmethod
    def _find_sharing_assignment(assignments):
        sharing_assignments = [assignment for assignment in assignments
                               if assignment.cause == ASSIGNMENT_VIA_SHARING]
        if sharing_assignments:
            return sharing_assignments[0]
        return None


def main():
    parser = setup_option_parser()
    parser.add_option("-f", "--force", action="store_true",
                      dest="force", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print "Missing argument, please provide a group name for which to search"
        sys.exit(1)

    app = setup_app()
    setup_plone(app)

    group_id = args[0]
    group = api.group.get(group_id)
    if not group and not options.force:
        print "Group does not exist"
        print "Available groups"
        print map(lambda group: group.id, api.group.get_groups())
        sys.exit(1)

    dossier_lister = DossierSharedWithGroupLister(group_id)
    dossier_lister.list_dossiers_shared_with_group()
    dossier_lister.print_table()

    log_filename = LogFilePathFinder().get_logfile_path(
        'list_dossiers_shared_with_{}'.format(group_id), extension="csv")
    with open(log_filename, "w") as logfile:
        dossier_lister.table.write_csv(logfile)

    print "done."


if __name__ == '__main__':
    main()
