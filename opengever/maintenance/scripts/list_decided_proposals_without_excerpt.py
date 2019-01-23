"""
Searches for all resolved proposals with no excerpt and lists them.

    bin/instance run ./scripts/list_decided_proposals_without_excerpt.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
import sys

from opengever.meeting.model.proposal import Proposal


class DecidedProposalWithoutExcerptLister(object):

    def __init__(self):
        self.table = TextTable(col_max_width=60)
        self.table.add_row(["proposal path", "proposal title"])

    def list_proposals_without_excerpt(self):
        for proposal in self.get_proposals_without_excerpt():
            self.table.add_row([proposal.physical_path, proposal.title])

    def print_table(self):
        print("Table of proposals in closed meetings lacking a returned excerpt")
        print(self.table.generate_output())
        print("\nSummary:")
        print("There are {} proposals in closed meetings lacking a returned excerpt"
              .format(self.table.nrows))

    def get_proposals_without_excerpt(self):
        """ Searches for all resolved proposals with no excerpt and yields them.
        """
        query = Proposal.query.decided().filter_by(excerpt_document=None)
        for proposal in query:
            if proposal.agenda_item.meeting.is_closed():
                yield proposal
        return


def main():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 0:
        print "Not expecting any argument"
        sys.exit(1)

    app = setup_app()
    setup_plone(app)

    proposal_lister = DecidedProposalWithoutExcerptLister()
    proposal_lister.list_proposals_without_excerpt()
    proposal_lister.print_table()

    log_filename = LogFilePathFinder().get_logfile_path(
        'list_decided_proposals_without_excerpt', extension="csv")
    with open(log_filename, "w") as logfile:
        proposal_lister.table.write_csv(logfile)

    print "done."


if __name__ == '__main__':
    main()
