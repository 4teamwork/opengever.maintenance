"""
This script is used to prepare deletion of the meetings from
Gever and use RIS in its stead.
"""
from Acquisition import aq_parent
from opengever.base.model.favorite import Favorite
from opengever.base.oguid import Oguid
from opengever.base.transport import BASEDATA_KEY
from opengever.base.transport import DexterityObjectCreator
from opengever.base.transport import DexterityObjectDataExtractor
from opengever.base.transport import FIELDDATA_KEY
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.meeting.model import Meeting
from opengever.meeting.model import Proposal
from opengever.ogds.base.utils import decode_for_json
from opengever.ogds.base.utils import encode_after_json
from persistent.mapping import PersistentMapping
from plone import api
import logging
import sys
import transaction

logger = logging.getLogger('opengever.maintenance')
logging.root.setLevel(logging.INFO)


class PreconditionsError(Exception):
    """Raised when preconditions for the migration are not satisfied"""


class MeetingsContentMigrator(object):

    def __init__(self):
        self.catalog = api.portal.get_tool("portal_catalog")
        self.check_preconditions()

    def __call__(self):
        self.replace_meeting_dossier_with_normal_dossier()

    def check_preconditions(self):
        # There should be no active meetings
        active_meetings = Meeting.query.active()
        if active_meetings.count():
            active_meetings_table = TextTable()
            active_meetings_table.add_row(
                (u"Path", u"Title", u"State"))

            for meeting in active_meetings:
                active_meetings_table.add_row((
                    meeting.get_url(),
                    meeting.get_title().replace(",", ""),
                    meeting.workflow_state))

            self.log_and_write_table(active_meetings_table, "Active Meetings", "active_meetings")

        # There should be no active proposals
        active_proposals = Proposal.query.active()
        if active_proposals.count():
            active_proposals_table = TextTable()
            active_proposals_table.add_row(
                (u"Path", u"Title", u"State"))

            for proposal in active_proposals:
                active_proposals_table.add_row((
                    proposal.get_url(),
                    proposal.title.replace(",", ""),
                    proposal.workflow_state))

            self.log_and_write_table(active_proposals_table, "Active Proposals", "active_proposals")

        if active_meetings.count() or active_proposals.count():
            raise PreconditionsError("Preconditions not satisfied")

    def replace_meeting_dossier_with_normal_dossier(self):
        meeting_dossiers = self.catalog.unrestrictedSearchResults(portal_type="opengever.meeting.meetingdossier")
        for brain in meeting_dossiers:
            meeting_dossier = brain.getObject()

            # create simple dossier
            parent = aq_parent(meeting_dossier)
            data = DexterityObjectDataExtractor(meeting_dossier).extract()
            data = encode_after_json(data)
            data[BASEDATA_KEY][u'portal_type'] = u'opengever.dossier.businesscasedossier'
            del data[FIELDDATA_KEY][u'IMeetingDossier']
            data[FIELDDATA_KEY][u'IBusinessCaseDossier'] = {}
            data[FIELDDATA_KEY][u'IProtectDossier'] = {}
            data = decode_for_json(data)
            dossier = DexterityObjectCreator(data).create_in(parent)

            # Move all content of meeting dossier to normal dossier
            for obj in meeting_dossier.contentValues():
                api.content.move(obj, dossier)

            # update reference in meeting
            meeting = meeting_dossier.get_meeting()
            meeting.dossier_oguid = Oguid.for_object(dossier)

            # update favorites
            query = Favorite.query.by_object(meeting_dossier)
            query.update({'oguid': Oguid.for_object(dossier)})

            # delete meeting_dossier
            api.content.delete(meeting_dossier)

    def log_and_write_table(self, table, title, filename):
        logger.info("\n{}".format(title))
        logger.info("\n" + table.generate_output() + "\n")

        log_filename = LogFilePathFinder().get_logfile_path(
            filename, extension="csv")
        with open(log_filename, "w") as logfile:
            table.write_csv(logfile)


def main():
    app = setup_app()
    setup_plone(app)

    parser = setup_option_parser()
    parser.add_option("-n", dest="dryrun", action="store_true", default=False)
    (options, args) = parser.parse_args()

    if options.dryrun:
        logger.info('Performing dryrun!\n')
        transaction.doom()

    migrator = MeetingsContentMigrator()
    migrator()

    if not options.dryrun:
        transaction.commit()


if __name__ == '__main__':
    main()
