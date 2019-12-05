from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.base.model import create_session
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFCore.utils import getToolByName
from sqlalchemy import MetaData
from zope.sqlalchemy.datamanager import mark_changed
import sys
import transaction

"""Delete SPV objects and tables.

    bin/instance run ./scripts/delete_spv.py

optional argument:
  -n : dry run
  --delete_tables: Also drop the SPV tables from the DB

"""


class DeleteSPVObjectsAndTables(object):

    def __init__(self, dry_run):
        self.session = create_session()
        self.connection = self.session.connection()
        portal = api.portal.get()
        self.catalog = getToolByName(portal, 'portal_catalog')
        self.dry_run = dry_run

    def __call__(self, delete_tables=False):
        self.remove_plone_content()
        if delete_tables:
            self.clear_sql_tables()

    def remove_plone_content(self):
        portal_types = (
            'opengever.meeting.committee',
            'opengever.meeting.committeecontainer',
            'opengever.meeting.period',
            'opengever.meeting.submittedproposal',
            'opengever.meeting.proposal',
            'opengever.meeting.proposaltemplate',
            'opengever.meeting.sablontemplate',
            'opengever.meeting.meetingdossier',
            'opengever.meeting.meetingtemplate',
            'opengever.meeting.paragraphtemplate',
            )

        for portal_type in portal_types:
            print("Starting deletion of {}".format(portal_type))
            for brain in self.catalog.unrestrictedSearchResults({'portal_type': portal_type}):
                obj = brain.getObject()
                print("deleting {}".format(obj.absolute_url()))
                aq_parent(aq_inner(obj)).manage_delObjects([obj.getId()])

    def clear_sql_tables(self):
        print("Starting deletion of tables")
        metadata = MetaData(self.connection, reflect=True)
        for table_name in (
                'excerpts',  # => agendaitems
                'agendaitems',  # => proposals, meetings
                'submitteddocuments',  # => proposals
                'meeting_participants',  # => (meetings), (members)
                'meetings',  # => (committees), (members)
                'proposals',  # => (committees)
                'generateddocuments',  #
                'memberships',   # => (committees), (members)
                'members',  #
                'periods',  # committees
                'committees',
                ):
            print("Deleting {}".format(table_name))
            if not self.dry_run:
                self.session.execute(metadata.tables.get(table_name).delete())

        mark_changed(self.session)


def main():
    parser = setup_option_parser()
    parser.add_option("--delete_tables", dest="delete_tables",
                      action="store_true", default=False)
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    app = setup_app()
    setup_plone(app)

    if not len(args) == 0:
        print "Not expecting any argument"
        sys.exit(1)

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    delete_tables = options.delete_tables
    cleaner = DeleteSPVObjectsAndTables(dry_run=options.dryrun)
    cleaner(delete_tables=delete_tables)

    if not options.dryrun:
        print "committing ..."
        transaction.commit()

    print "done."


if __name__ == '__main__':
    main()
