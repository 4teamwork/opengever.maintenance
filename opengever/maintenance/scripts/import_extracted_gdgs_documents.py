from Acquisition import aq_base
from Acquisition.interfaces import IAcquirer
from collective.taskqueue.interfaces import ITaskQueue
from collective.taskqueue.interfaces import ITaskQueueLayer
from collective.taskqueue.taskqueue import LocalVolatileTaskQueue
from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.document.versioner import Versioner
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from operator import itemgetter
from os.path import join as pjoin
from plone.api.env import adopt_user
from plone.namedfile.file import NamedBlobFile
from plone.restapi.exceptions import DeserializationError
from plone.restapi.interfaces import IDeserializeFromJson
from plone.restapi.services.content.utils import add
from plone.restapi.services.content.utils import create
from plone.subrequest import subrequest
from Products.CMFPlone.utils import safe_hasattr
from zExceptions import BadRequest
from zExceptions import Unauthorized
from zope.component import provideUtility
from zope.component import queryMultiAdapter
from zope.event import notify
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
from zope.interface import noLongerProvides
from zope.lifecycleevent import ObjectCreatedEvent
import argparse
import json
import os
import sys
import transaction


EXTRACTION_PATH = '/home/zope/03-gever-gdgs/extracted_documents'


CREATED_OBJS_BY_OLD_PATH = {}


def create_and_add(container, type_, data, id_=None, title=None):
    try:
        obj = create(container, type_, id_=id_, title=title)
    except Unauthorized:
        raise
    except BadRequest:
        raise

    # Acquisition wrap temporarily to satisfy things like vocabularies
    # depending on tools
    temporarily_wrapped = False
    if IAcquirer.providedBy(obj) and not safe_hasattr(obj, "aq_base"):
        obj = obj.__of__(container)
        temporarily_wrapped = True

    # Update fields
    deserializer = queryMultiAdapter((obj, getRequest()), IDeserializeFromJson)
    if deserializer is None:
        raise Exception('No deserializer found')

    try:
        deserializer(validate_all=True, data=data, create=True)
    except DeserializationError:
        raise

    if temporarily_wrapped:
        obj = aq_base(obj)

    if not getattr(deserializer, "notifies_create", False):
        notify(ObjectCreatedEvent(obj))

    obj = add(container, obj, rename=not bool(id_))
    return obj


class ObjectImporter(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options

        self.request = getRequest()
        self.portal.setupCurrentSkin()
        alsoProvides(self.request, IOpengeverBaseLayer)
        self.missing_parent_count = 0
        self._task_queue = self.setup_task_queue()

    def setup_task_queue(self):
        task_queue = LocalVolatileTaskQueue()
        provideUtility(task_queue, ITaskQueue, name='default')
        return task_queue

    def run(self):
        all_object_metadata = []

        # Load all metadata
        for fn in os.listdir(EXTRACTION_PATH):
            if not fn.endswith('.json'):
                continue
            metadata_path = pjoin(EXTRACTION_PATH, fn)
            metadata = json.load(open(metadata_path))
            all_object_metadata.append(metadata)

        self.all_paths = [item['relative_path'] for item in all_object_metadata]

        for item in sorted(all_object_metadata, key=itemgetter('relative_path')):
            path = item['relative_path'].encode('utf-8')
            portal_type = item['@type']
            path = item['relative_path'].encode('utf-8')
            # id_ = path.split('/')[-1]

            try:
                existing_obj = self.portal.unrestrictedTraverse(path)
                self.update_object(existing_obj, item)
            except KeyError:
                username = item.get('creator', {}).get('identifier', 'zopemaster')
                portal_type = item['@type']
                if portal_type == 'opengever.task.task':
                    self.create_object(path, portal_type, item)
                else:
                    with adopt_user(username=username):
                        self.create_object(path, portal_type, item)

        self.process_task_queue()
        print('Missing parents: %s' % self.missing_parent_count)

    def update_object(self, existing_obj, item):
        portal_type = item['@type']

        # We don't update other types of objects
        if portal_type == 'opengever.document.document':

            # Set file
            blob_path = pjoin(EXTRACTION_PATH, item['_blob_path'].split('/')[-1])
            filename = item['_blob_filename']
            content_type = item['_blob_content_type']
            data = open(blob_path, 'rb').read()
            existing_obj.file = NamedBlobFile(data=data, filename=filename, contentType=content_type)
            Versioner(existing_obj).create_version('Restored from backup')
            print('Updated object %r' % existing_obj)

    def create_object(self, old_path, portal_type, item):
        parent_path = '/'.join(old_path.split('/')[:-1])

        parent = CREATED_OBJS_BY_OLD_PATH.get(parent_path)
        if parent is None:
            try:
                parent = self.portal.unrestrictedTraverse(parent_path)
            except KeyError:
                if parent_path not in self.all_paths:
                    print('Parent does not exist for %s' % old_path)
                    self.missing_parent_count += 1
                    return

        title = item.get('title', item.get('title_de'))

        if portal_type == 'opengever.dossier.businesscasedossier':
            data = item.copy()
            data.pop('relatedDossier', None)
            created_obj = create_and_add(parent, portal_type, data, title=title)

        elif portal_type == 'opengever.document.document':
            data = item.copy()
            created_obj = create_and_add(parent, portal_type, data, title=title)

            # Set file
            blob_path = pjoin(EXTRACTION_PATH, item['_blob_path'].split('/')[-1])
            filename = item['_blob_filename']
            content_type = item['_blob_content_type']
            data = open(blob_path, 'rb').read()

            created_obj.file = NamedBlobFile(data=data, filename=filename, contentType=content_type)
            Versioner(created_obj).create_initial_version()

        elif portal_type == 'ftw.mail.mail':
            data = item.copy()
            # title = u'tmp'
            try:
                created_obj = create_and_add(parent, portal_type, data, title=title)
            except Exception as exc:
                print('Failed to create object at %s' % old_path)
                print(exc)
                return
            # created_obj.sync_title_and_filename()

            # Set file
            blob_path = pjoin(EXTRACTION_PATH, item['_blob_path'].split('/')[-1])
            filename = item['_blob_filename']
            content_type = item['_blob_content_type']
            data = open(blob_path, 'rb').read()
            created_obj.message = NamedBlobFile(data=data, filename=filename, contentType=content_type)
            # no intial version needed for mails

        elif portal_type == 'opengever.task.task':
            data = item.copy()
            data.pop('relatedItems', None)
            created_obj = create_and_add(parent, portal_type, data, title=title)

        else:
            raise AssertionError("Unexpected type %s" % portal_type)

        CREATED_OBJS_BY_OLD_PATH[old_path] = created_obj
        print('Created %r' % created_obj)

    def process_task_queue(self):
        queue = self._task_queue.queue

        print('Processing %d task queue jobs...' % queue.qsize())
        request = getRequest()
        alsoProvides(request, ITaskQueueLayer)

        while not queue.empty():
            job = queue.get()

            # Process job using plone.subrequest
            response = subrequest(job['url'])
            assert response.status == 200

        noLongerProvides(request, ITaskQueueLayer)
        print('All task queue jobs processed.')


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="Dry run")

    options = parser.parse_args(sys.argv[3:])

    if options.dry_run:
        print("DRY RUN")
        transaction.doom()

    plone = setup_plone(app, options)

    importer = ObjectImporter(plone, options)
    importer.run()

    if not options.dry_run:
        print('Committing transaction...')
        transaction.commit()
        print('Done.')
