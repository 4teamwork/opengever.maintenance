"""
This script is used to prepare deletion of the meetings from
Gever and use RIS in its stead.
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.meeting.model import Meeting
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

        if active_meetings:
            raise PreconditionsError("Preconditions not satisfied")

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

    if not options.dryrun:
        transaction.commit()


if __name__ == '__main__':
    main()
