"""
Script that checks that participants set on workspace folders are valid,
i.e. are also participants on their parent.

bin/instance0 run src/opengever.maintenance/opengever/maintenance/scripts/check_workspace_participants.py
"""
from Acquisition import aq_parent
from collections import namedtuple
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.base.role_assignments import SharingRoleAssignment
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.workspace.interfaces import IWorkspace
from opengever.workspace.participation.browser.manage_participants import ManageParticipants
from plone import api
from zope.globalrequest import getRequest
import sys
import transaction


Correction = namedtuple(
    'Correction', ['obj', 'set_roles_block', 'deleted', 'added', 'updated'])


class ParticipantsChecker(object):

    def __init__(self):
        self.catalog = api.portal.get_tool("portal_catalog")

    def get_url(self, obj):
        url_tool = api.portal.get_tool('portal_url')
        public_url = get_current_admin_unit().public_url
        path = "/".join(url_tool.getRelativeContentPath(obj))
        return "/".join([public_url, path])

    def get_titles(self, obj):
        titles = []
        self._recurse_title(obj, titles)
        return " | ".join(reversed(titles))

    def _recurse_title(self, obj, titles):
        titles.append(obj.Title())
        if IWorkspace.providedBy(obj):
            return
        parent = aq_parent(obj)
        self._recurse_title(parent, titles)

    @property
    def workspaces_and_workspace_folders(self):
        """Sorting on path is essential to first fix the parents and then their
        children.
        """
        brains = self.catalog.unrestrictedSearchResults(
            portal_type=['opengever.workspace.folder',
                         'opengever.workspace.workspace'],
            sort_on="path")
        for brain in brains:
            yield brain.getObject()

    @staticmethod
    def get_participants_with_multiple_roles(obj, manager):
        participants = manager.get_participants()
        return [participant for participant in participants
                if len(participant["roles"]) > 1]

    @staticmethod
    def get_misconfigured_participants(obj, manager):
        if obj.portal_type == 'opengever.workspace.workspace':
            return []

        participants = manager.get_participants()
        allowed_userids = {
            each["userid"] for each in
            ManageParticipants(obj.get_parent_with_local_roles(), getRequest()).get_participants()}

        misconfigured_participants = []
        for participant in participants:
            if not participant["userid"] in allowed_userids:
                misconfigured_participants.append(participant)

        return misconfigured_participants

    def correct_misconfigured_participants(self, obj, manager):
        misconfigured_participants = self.get_misconfigured_participants(obj, manager)
        deleted = []
        for participant in misconfigured_participants:
            manager._delete(participant["type_"], participant["token"])
            deleted.append(participant)
        return deleted

    @staticmethod
    def is_local_roles_block_missing(obj, manager):
        if obj.portal_type == 'opengever.workspace.workspace':
            return False

        participants = manager.get_participants()
        return bool(participants and not getattr(obj, '__ac_local_roles_block__', False))

    @staticmethod
    def get_role_with_most_permissions(roles):
        SORTED_ROLES = ['WorkspaceAdmin', 'WorkspaceMember', 'WorkspaceGuest']
        for role in SORTED_ROLES:
            if role in roles:
                return role
        raise ValueError("No teamraum role found in {}".format(roles))

    @staticmethod
    def get_participant_for_token(participants, token):
        matches = filter(lambda participant: participant["token"] == token, participants)
        if len(matches) > 1:
            raise ValueError("There should only be a single participant for a given token")
        if matches:
            return matches[0]

    def fix_roles_block(self, obj, manager):
        added = []
        updated = []
        if not self.is_local_roles_block_missing(obj, manager):
            return False, added, updated

        obj.__ac_local_roles_block__ = True

        # We need to copy the local roles over from the parent
        # It is enough to take over the roles from participations
        participants = manager.get_participants()
        parent_manager = ManageParticipants(obj.get_parent_with_local_roles(),
                                            getRequest())
        parent_participants = parent_manager.get_participants()
        for parent_participant in parent_participants:
            # find matching participant on obj if present
            participant = self.get_participant_for_token(
                participants, parent_participant["token"])

            # determine which role to set (role with most permissions)
            roles = list(parent_participant["roles"])
            if participant:
                roles.extend(participant["roles"])
            role = self.get_role_with_most_permissions(roles)
            if participant and role in participant["roles"]:
                continue

            # give or update role for participant
            assignment = SharingRoleAssignment(
                parent_participant["token"], [role], obj)
            RoleAssignmentManager(obj).add_or_update_assignment(assignment)
            data = {"userid": parent_participant["userid"], "roles": [role]}
            if participant:
                updated.append(data)
            else:
                added.append(data)

        return True, added, updated

    @staticmethod
    def is_admin_missing(obj, manager):
        participants = manager.get_participants()
        return bool(
                participants and not any(["WorkspaceAdmin" in participant["roles"]
                                          for participant in participants])
                )

    def check_misconfigurations(self):
        self.misconfigured = TextTable()
        self.misconfigured.add_row((
            u"Pfad",
            "Title",
            u"Fehlender Admin",
            u"Fehlende Zugriffseinschr\xe4nkung",
            u"Teilnehmer mit mehreren Rollen",
            u"Teilnehmer ohne Berechtigung auf \xfcbergeordneten Objekt"))

        for i, obj in enumerate(self.workspaces_and_workspace_folders, 1):
            manager = ManageParticipants(obj, getRequest())
            misconfigured_participants = self.get_misconfigured_participants(obj, manager)
            missing_local_roles_block = self.is_local_roles_block_missing(obj, manager)
            missing_admin = self.is_admin_missing(obj, manager)
            with_multiple_roles = self.get_participants_with_multiple_roles(obj, manager)
            if any((missing_admin, misconfigured_participants,
                    missing_local_roles_block, with_multiple_roles)):
                self.misconfigured.add_row((
                    self.get_url(obj),
                    self.get_titles(obj),
                    'x' if missing_admin else'',
                    'x' if missing_local_roles_block else '',
                    'x' if with_multiple_roles else '',
                    u" ".join([participant["userid"] for participant in misconfigured_participants])))

    @staticmethod
    def _format_participant(participant):
        roles = ",".join(participant["roles"])
        return "{}:{}".format(participant["userid"], roles)

    def format_participants(self, participants):
        return "\n".join([self._format_participant(participant)
                          for participant in participants])

    def correct_misconfigurations(self):
        corrections = []
        for i, obj in enumerate(self.workspaces_and_workspace_folders, 1):
            manager = ManageParticipants(obj, getRequest())
            deleted = self.correct_misconfigured_participants(obj, manager)
            set_roles_block, added, updated = self.fix_roles_block(obj, manager)

            if any((deleted, set_roles_block)):
                corrections.append(
                    Correction(obj, set_roles_block, deleted, added, updated))

        self.corrections_table = TextTable()
        self.corrections_table.add_row((
            u"Pfad",
            "Title",
            u"Zugriffseinschr\xe4nkung hinzugef\xfcgt",
            u"Teilnehmer gel\xf6scht",
            u"Teilnehmer hinzugef\xfcgt",
            u"Teilnehmer modifiziert"))
        for corr in corrections:
            self.corrections_table.add_row((
                self.get_url(corr.obj),
                self.get_titles(corr.obj),
                'x' if corr.set_roles_block else '',
                self.format_participants(corr.deleted),
                self.format_participants(corr.added),
                self.format_participants(corr.updated)
            ))


def main():
    parser = setup_option_parser()

    parser.add_option(
        '-f', '--fix', action='store_true', default=None,
        help='Correct the permissions',
        dest='fix')

    parser.add_option(
        '-n', '--dry-run', dest='dryrun',
        default=False, action="store_true",
        help='Dryrun, do not commit changes. Only relevant for correction.')

    (options, args) = parser.parse_args()

    app = setup_app()
    setup_plone(app)

    if options.dryrun:
        print('Performing dryrun!\n')
        transaction.doom()

    participants_checker = ParticipantsChecker()
    if not options.fix:
        participants_checker.check_misconfigurations()

        sys.stdout.write("\n\nTable of all misconfigured dossiers:\n")
        sys.stdout.write(participants_checker.misconfigured.generate_output())

        log_filename = LogFilePathFinder().get_logfile_path(
            'misconfigured_workspace_folders', extension="csv")
        with open(log_filename, "w") as logfile:
            participants_checker.misconfigured.write_csv(logfile)

    else:
        participants_checker.correct_misconfigurations()

        sys.stdout.write("\n\nTable of all corrections:\n")
        sys.stdout.write(participants_checker.corrections_table.generate_output())

        log_filename = LogFilePathFinder().get_logfile_path(
            'corrected_workspace_folders', extension="csv")
        with open(log_filename, "w") as logfile:
            participants_checker.corrections_table.write_csv(logfile)

        if not options.dryrun:
            transaction.commit()


if __name__ == '__main__':
    main()
