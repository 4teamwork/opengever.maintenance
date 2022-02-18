"""
Script that checks that participants set on workspace folders are valid,
i.e. are also participants on their parent.

bin/instance0 run src/opengever.maintenance/opengever/maintenance/scripts/check_workspace_participants.py
"""
from Acquisition import aq_parent
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.workspace.interfaces import IWorkspace
from opengever.workspace.participation.browser.manage_participants import ManageParticipants
from plone import api
from zope.globalrequest import getRequest
import sys


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
    def workspace_folders(self):
        brains = self.catalog.unrestrictedSearchResults(
            portal_type='opengever.workspace.folder',
            sort_on="path")
        for brain in brains:
            yield brain.getObject()

    @staticmethod
    def get_misconfigured_participants(obj, manager):
        participants = manager.get_participants()
        allowed_userids = {
            each["userid"] for each in
            ManageParticipants(obj.get_parent_with_local_roles(), getRequest()).get_participants()}

        misconfigured_participants = []
        for participant in participants:
            if not participant["userid"] in allowed_userids:
                misconfigured_participants.append(participant)

        return misconfigured_participants

    @staticmethod
    def is_local_roles_block_missing(obj, manager):
        participants = manager.get_participants()
        return bool(participants and not getattr(obj, '__ac_local_roles_block__', False))

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
            u"Teilnehmer ohne Berechtigung auf \xfcbergeordneten Objekt"))

        for i, obj in enumerate(self.workspace_folders, 1):
            manager = ManageParticipants(obj, getRequest())
            misconfigured_participants = self.get_misconfigured_participants(obj, manager)
            missing_local_roles_block = self.is_local_roles_block_missing(obj, manager)
            missing_admin = self.is_admin_missing(obj, manager)

            if any((missing_admin, misconfigured_participants, missing_local_roles_block)):
                self.misconfigured.add_row((
                    self.get_url(obj),
                    self.get_titles(obj),
                    'x' if missing_admin else'',
                    'x' if missing_local_roles_block else '',
                    u" ".join([participant["userid"] for participant in misconfigured_participants])))


def main():
    app = setup_app()
    setup_plone(app)

    participants_checker = ParticipantsChecker()
    participants_checker.check_misconfigurations()

    sys.stdout.write("\n\nTable of all misconfigured dossiers:\n")
    sys.stdout.write(participants_checker.misconfigured.generate_output())

    log_filename = LogFilePathFinder().get_logfile_path(
        'misconfigured_workspace_folders', extension="csv")
    with open(log_filename, "w") as logfile:
        participants_checker.misconfigured.write_csv(logfile)


if __name__ == '__main__':
    main()
