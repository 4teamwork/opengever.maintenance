"""
This script is used to prepare deletion of the meetings from
Gever and use RIS in its stead.
"""
from Acquisition import aq_parent
from collective.taskqueue.interfaces import ITaskQueue
from collective.taskqueue.interfaces import ITaskQueueLayer
from collective.taskqueue.taskqueue import LocalVolatileTaskQueue
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.base.model.favorite import Favorite
from opengever.base.oguid import Oguid
from opengever.base.transport import BASEDATA_KEY
from opengever.base.transport import DexterityObjectCreator
from opengever.base.transport import DexterityObjectDataExtractor
from opengever.base.transport import FIELDDATA_KEY
from opengever.dossier.behaviors.dossier import IDossier
from opengever.dossier.deactivate import DossierDeactivator
from opengever.dossier.interfaces import IDossierResolver
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.meeting.interfaces import IDuringMeetingMigration
from opengever.meeting.interfaces import IMeetingSettings
from opengever.meeting.model import Meeting
from opengever.meeting.model import Proposal
from opengever.ogds.base.utils import decode_for_json
from opengever.ogds.base.utils import encode_after_json
from persistent.mapping import PersistentMapping
from plone import api
from plone.app.uuid.utils import uuidToObject
from plone.subrequest import subrequest
from zope.annotation import IAnnotations
from zope.component import getAdapter
from zope.component import provideUtility
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
from zope.interface import noLongerProvides
import logging
import sys
import transaction

logger = logging.getLogger('opengever.maintenance')
logging.root.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.root.addHandler(handler)

MEETING_MIGRATION_KEY = 'MEETING_MIGRATION_DATA'


class PreconditionsError(Exception):
    """Raised when preconditions for the migration are not satisfied"""


class MeetingsContentMigrator(object):

    def __init__(self, dryrun=False):
        self.dryrun = dryrun
        self.catalog = api.portal.get_tool("portal_catalog")
        self.check_preconditions()
        self.request = getRequest()
        # Do not prepend document titles with 'Copy of'
        self.request['prevent-copyname-on-document-copy'] = True

    def __call__(self):
        if self.dryrun:
            logger.info('Performing dryrun!\n')
            transaction.doom()

        # Mark the request
        alsoProvides(self.request, IDuringMeetingMigration)

        self._task_queue = self.setup_task_queue()
        self.migrate_meetings()
        self.disable_meeting_feature()

        if not self.dryrun:
            logger.info("Committing...")
            transaction.commit()

        self.process_task_queue()

        noLongerProvides(self.request, IDuringMeetingMigration)
        logger.info("Done!")

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

        # Meeting dossiers should be in state active, resolved or inactive
        meeting_dossiers = self.catalog.unrestrictedSearchResults(portal_type="opengever.meeting.meetingdossier")
        allowed_states = ['dossier-state-resolved', 'dossier-state-inactive', 'dossier-state-active']
        dossiers_in_bad_state = []
        for brain in meeting_dossiers:
            if brain.review_state not in allowed_states:
                dossiers_in_bad_state.append(brain)

        if dossiers_in_bad_state:
            dossiers_in_bad_state_table = TextTable()
            dossiers_in_bad_state_table.add_row(
                (u"Path", u"Title", u"State"))

            for brain in dossiers_in_bad_state:
                dossiers_in_bad_state_table.add_row((
                    brain.getPath(),
                    brain.title.replace(",", ""),
                    brain.review_state))

            self.log_and_write_table(
                dossiers_in_bad_state_table,
                "Dossiers in bad review states",
                "dossiers_in_bad_state")

        # All meeting dossiers should be linked to a meeting
        meeting_dossiers = {dossier.UID for dossier in meeting_dossiers}
        linked_dossiers = set()
        for meeting in Meeting.query:
            linked_dossiers.add(meeting.get_dossier().UID())
        unhealthy_links = meeting_dossiers.symmetric_difference(linked_dossiers)

        if unhealthy_links:
            unhealthy_links_table = TextTable()
            unhealthy_links_table.add_row(
                (u"Path", u"Title", u"State"))

            for uid in unhealthy_links:
                obj = uuidToObject(uid)
                unhealthy_links_table.add_row((
                    obj.absolute_url_path(),
                    obj.Title().replace(",", ""),
                    api.content.get_state(obj)))

            self.log_and_write_table(
                unhealthy_links_table,
                "Meeting dossiers with unhealthy links to meeting",
                "unhealthy_links")

        if active_meetings.count() or active_proposals.count() or dossiers_in_bad_state or unhealthy_links:
            raise PreconditionsError("Preconditions not satisfied")

    def migrate_meetings(self):
        message = "Migrating meetings."
        for meeting in ProgressLogger(message, Meeting.query.all(), logger):
            meeting_dossier = meeting.get_dossier()
            dossier = self.replace_meeting_dossier_with_normal_dossier(meeting_dossier)
            self.migrate_agendaitems_to_subdossiers(meeting, dossier)
            self.set_meeting_dossier_state(dossier)

    def replace_meeting_dossier_with_normal_dossier(self, meeting_dossier):
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
        logger.info("Moving content from meeting dossier {} to {}".format(
            meeting_dossier.absolute_url_path(), dossier.absolute_url_path()))
        for obj in meeting_dossier.contentValues():
            api.content.move(obj, dossier)

        # update reference in meeting
        meeting = meeting_dossier.get_meeting()
        meeting.dossier_oguid = Oguid.for_object(dossier)

        # update favorites
        query = Favorite.query.by_object(meeting_dossier)
        query.update({'oguid': Oguid.for_object(dossier)})

        # Save state of the meeting dossier on the new dossier
        migration_annotations = self.get_migration_annotations(dossier)
        migration_annotations['state'] = api.content.get_state(meeting_dossier)
        migration_annotations['former_path'] = meeting_dossier.absolute_url_path()

        # delete meeting_dossier
        logger.info("Deleting {}".format(meeting_dossier.absolute_url_path()))
        assert not meeting_dossier.contentValues(), "Will not delete non-empty meeting dossier!"
        api.content.delete(meeting_dossier)
        return dossier

    def migrate_agendaitems_to_subdossiers(self, meeting, meeting_dossier):
        """ We create a subdossier in the meeting dossier for each agendaitem
        and copy or move its documents into that subdossier.
        """
        responsible = IDossier(meeting_dossier).responsible
        message = "Migrating agendaitems for {}".format(meeting.physical_path)
        for agendaitem in ProgressLogger(message, meeting.agenda_items, logger):
            # create a subdossier
            dossier = api.content.create(
                type='opengever.dossier.businesscasedossier',
                title=agendaitem.get_title(include_number=True, formatted=True),
                responsible=responsible,
                container=meeting_dossier)

            # Copy documents from submitted proposal into subdossier
            for doc in agendaitem.get_documents():
                self.copy_and_add_to_mapping(doc, dossier, meeting_dossier)

            # Move excerpts
            for doc in agendaitem.get_excerpt_documents():
                assert doc in meeting_dossier.objectValues(), "Excerpt should be in meeting dossier."
                self.move_and_add_to_mapping(doc, dossier, meeting_dossier)

            # Move the proposal document
            document = agendaitem.resolve_document()
            self.move_and_add_to_mapping(document, dossier, meeting_dossier)

    def set_meeting_dossier_state(self, meeting_dossier):
        logger.info("Setting state for {}\n".format(meeting_dossier.absolute_url_path()))
        migration_annotations = self.get_migration_annotations(meeting_dossier)
        state = migration_annotations['state']
        if state == 'dossier-state-active':
            return
        elif state == 'dossier-state-resolved':
            resolver = getAdapter(meeting_dossier, IDossierResolver, name="lenient")
            resolver.raise_on_failed_preconditions()
            resolver.resolve()
        elif state == 'dossier-state-inactive':
            DossierDeactivator(meeting_dossier).deactivate()
        else:
            logger.info("Could not set state {} for {}".format(
                            state, meeting_dossier.absolute_url_path()))
        api.content.transition(meeting_dossier, to_state=migration_annotations['state'])

    def disable_meeting_feature(self):
        api.portal.set_registry_record(
            'is_feature_enabled', False, interface=IMeetingSettings)

    def get_migration_annotations(self, obj):
        annotations = IAnnotations(obj)
        if MEETING_MIGRATION_KEY not in annotations:
            annotations[MEETING_MIGRATION_KEY] = PersistentMapping()
            annotations[MEETING_MIGRATION_KEY]['copied'] = PersistentMapping()
            annotations[MEETING_MIGRATION_KEY]['moved'] = PersistentMapping()
        return annotations[MEETING_MIGRATION_KEY]

    def copy_and_add_to_mapping(self, obj, container, meeting_dossier):
        former_path = obj.absolute_url_path()
        copied = api.content.copy(source=obj, target=container)
        new_path = copied.absolute_url_path()
        migration_annotations = self.get_migration_annotations(meeting_dossier)
        migration_annotations['copied'][new_path] = former_path
        return copied

    def move_and_add_to_mapping(self, obj, container, meeting_dossier):
        former_path = obj.absolute_url_path()
        moved = api.content.move(obj, container)
        new_path = moved.absolute_url_path()
        migration_annotations = self.get_migration_annotations(meeting_dossier)
        migration_annotations['moved'][new_path] = former_path
        return moved

    def log_and_write_table(self, table, title, filename):
        logger.info("\n{}".format(title))
        logger.info("\n" + table.generate_output() + "\n")

        log_filename = LogFilePathFinder().get_logfile_path(
            filename, extension="csv")
        with open(log_filename, "w") as logfile:
            table.write_csv(logfile)

    def setup_task_queue(self):
        task_queue = LocalVolatileTaskQueue()
        provideUtility(task_queue, ITaskQueue, name='default')
        return task_queue

    def process_task_queue(self):
        queue = self._task_queue.queue

        logger.info('Processing %d task queue jobs...' % queue.qsize())
        request = getRequest()
        alsoProvides(request, ITaskQueueLayer)

        while not queue.empty():
            job = queue.get()

            # Process job using plone.subrequest
            response = subrequest(job['url'])
            assert response.status == 200

            # XXX: We don't currently handle the user that is supposed to be
            # authenticated, and the task ID, both of which c.taskqueue
            # provides in the job.

        noLongerProvides(request, ITaskQueueLayer)
        logger.info('All task queue jobs processed.')


def main():
    app = setup_app()
    portal = setup_plone(app)
    portal.setupCurrentSkin()
    alsoProvides(getRequest(), IOpengeverBaseLayer)

    parser = setup_option_parser()
    parser.add_option("-n", dest="dryrun", action="store_true", default=False)
    (options, args) = parser.parse_args()

    migrator = MeetingsContentMigrator(options.dryrun)
    migrator()


if __name__ == '__main__':
    main()
