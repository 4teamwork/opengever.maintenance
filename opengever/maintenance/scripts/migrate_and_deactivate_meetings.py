"""
This script is used to prepare deletion of the meetings from
Gever and use RIS in its stead.
"""
from Acquisition import aq_parent
from collective.taskqueue.interfaces import ITaskQueue
from collective.taskqueue.interfaces import ITaskQueueLayer
from collective.taskqueue.taskqueue import LocalVolatileTaskQueue
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
from opengever.meeting.interfaces import IMeetingSettings
from opengever.meeting.model import Meeting
from opengever.meeting.model import Proposal
from opengever.ogds.base.utils import decode_for_json
from opengever.ogds.base.utils import encode_after_json
from persistent.mapping import PersistentMapping
from plone import api
from plone.subrequest import subrequest
from zope.annotation import IAnnotations
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
logging.root.addHandler(handler)

PATH_MAPPING_KEY = 'MEETING_MIGRATION_PATH_MAPPING'


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

        self._task_queue = self.setup_task_queue()
        self.replace_meeting_dossier_with_normal_dossier()
        self.migrate_agendaitems_to_subdossiers()
        self.disable_meeting_feature()

        if not self.dryrun:
            transaction.commit()

        self.process_task_queue()

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

    def migrate_agendaitems_to_subdossiers(self):
        """ We create a subdossier in the meeting dossier for each agendaitem
        and copy or move its documents into that subdossier.
        """
        for meeting in Meeting.query:
            meeting_dossier = meeting.get_dossier()
            for agendaitem in meeting.agenda_items:
                # create a subdossier
                dossier = api.content.create(
                    type='opengever.dossier.businesscasedossier',
                    title=agendaitem.get_title(include_number=True, formatted=True),
                    container=meeting_dossier)

                # Copy documents from submitted proposal into subdossier
                for doc in agendaitem.get_documents():
                    self.copy_and_add_to_mapping(doc, dossier)

                # Move excerpts
                for doc in agendaitem.get_excerpt_documents():
                    self.move_and_add_to_mapping(doc, dossier)

                # Move the proposal document
                document = agendaitem.resolve_document()
                self.move_and_add_to_mapping(document, dossier)

    def disable_meeting_feature(self):
        api.portal.set_registry_record(
            'is_feature_enabled', False, interface=IMeetingSettings)

    def get_path_mapping(self, obj):
        annotations = IAnnotations(obj)
        if PATH_MAPPING_KEY not in annotations:
            annotations[PATH_MAPPING_KEY] = PersistentMapping()
        return annotations[PATH_MAPPING_KEY]

    def copy_and_add_to_mapping(self, obj, container):
        former_path = obj.absolute_url_path()
        copied = api.content.copy(source=obj, target=container)
        new_path = copied.absolute_url_path()
        mapping = self.get_path_mapping(container)
        mapping[new_path] = former_path
        return copied

    def move_and_add_to_mapping(self, obj, container):
        former_path = obj.absolute_url_path()
        moved = api.content.move(obj, container)
        new_path = moved.absolute_url_path()
        mapping = self.get_path_mapping(container)
        mapping[new_path] = former_path
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
    setup_plone(app)

    parser = setup_option_parser()
    parser.add_option("-n", dest="dryrun", action="store_true", default=False)
    (options, args) = parser.parse_args()

    migrator = MeetingsContentMigrator(options.dryrun)
    migrator()


if __name__ == '__main__':
    main()
