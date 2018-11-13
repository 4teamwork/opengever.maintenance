from opengever.contact.contact import IContact
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.inbox.forwarding import IForwarding
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task.task import ITask
from opengever.tasktemplates.content.tasktemplate import ITaskTemplate
from plone import api
from Products.Five.browser import BrowserView
import logging
import transaction

log = logging.getLogger('opengever.maintenance')

COMMON_TYPES = [
    IContact,
    IBaseDocument,
    IDossierMarker,
    IForwarding,
    IRepositoryFolder,
    IRepositoryRoot,
    ITask,
    ITaskTemplate,
]

WARMUP_INDEXES = [
    'allowedRolesAndUsers',
    'trashed',
    'object_provides',
]


class WarmupView(BrowserView):
    """View to warm up a GEVER instance.
    """

    def __call__(self):
        # XXX: Check for filesystem token or ManagePortal permission
        transaction.doom()
        self.catalog = api.portal.get_tool('portal_catalog')

        mode = self.request.form.get('mode', 'minimal')
        log.info('Warming up instance (mode == {})...'.format(mode))

        if mode == 'minimal':
            self._warmup_minimal()
        elif mode == 'medium':
            self._warmup_medium()
        elif mode == 'full':
            self._warmup_full()
        elif mode == 'catalog':
            self._warmup_catalog()
        else:
            raise Exception('Warmup mode {!r} not recognized!'.format(mode))

        log.info('Done warming up.')
        return 'OK'

    def _warmup_minimal(self):
        # Fetch repository brains and objects
        repo_brains = self.catalog(
            object_provides=IRepositoryFolder.__identifier__)
        for brain in repo_brains:
            repo = brain.getObject()

    def _warmup_medium(self):
        # Fetch brains for all common types
        for type_iface in COMMON_TYPES:
            count = 0
            brains = self.catalog(object_provides=type_iface.__identifier__)
            for brain in brains:
                count += 1
            log.info('Fetched {} brains for type {}'.format(
                count, type_iface.__identifier__))

        self._warmup_minimal()

    def _warmup_full(self):
        # Fetch brains and objects for all common types
        for type_iface in COMMON_TYPES:
            count = 0
            brains = self.catalog(object_provides=type_iface.__identifier__)
            for brain in brains:
                obj = brain.getObject()
                count += 1
            log.info('Fetched {} brains and objects for type {}'.format(
                count, type_iface.__identifier__))

        self._warmup_medium()
        self._warmup_minimal()

    def _warmup_catalog(self):
        # Load catalog BTrees and forward index BTrees of the most used indexes

        def load_btree(node, level=0, maxlevel=2):
            if level >= maxlevel:
                return
            bucket = getattr(node, '_firstbucket', None)
            while bucket is not None:
                for key in bucket.keys():
                    load_btree(key, level+1, maxlevel)
                if hasattr(bucket, 'values'):
                    for value in bucket.values():
                        load_btree(value, level+1, maxlevel)
                bucket = bucket._next

        catalog = api.portal.get_tool('portal_catalog')
        load_btree(catalog._catalog.uids)
        load_btree(catalog._catalog.paths)
        load_btree(catalog._catalog.data)
        for index_name in WARMUP_INDEXES:
            index = catalog._catalog.indexes[index_name]
            load_btree(index._index)
