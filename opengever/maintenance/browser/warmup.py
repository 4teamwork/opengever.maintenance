from Acquisition import aq_base
from opengever.contact.contact import IContact
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.inbox.forwarding import IForwarding
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task.task import ITask
from opengever.tasktemplates.content.tasktemplate import ITaskTemplate
from plone import api
from plone.app.folder.nogopip import GopipIndex
from Products.ExtendedPathIndex.ExtendedPathIndex import ExtendedPathIndex
from Products.Five.browser import BrowserView
from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex
import logging
import time
import transaction
import threading

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


class WarmupView(BrowserView):
    """View to warm up a GEVER instance.
    """

    def __call__(self):
        # XXX: Check for filesystem token or ManagePortal permission
        transaction.doom()
        self.catalog = api.portal.get_tool('portal_catalog')
        thread = threading.current_thread().name
        conn = self.context._p_jar

        mode = self.request.form.get('mode', 'minimal')
        lexicons = self._to_bool(self.request.form.get('lexicons', True))

        log.info(
            'Warming up instance (mode == {}, Thread {!r}, '
            'Connection {!r})...'.format(mode, thread, conn))

        if mode == 'minimal':
            self._warmup_minimal()
        elif mode == 'medium':
            self._warmup_medium()
        elif mode == 'catalog':
            self._warmup_catalog(lexicons=lexicons)
        else:
            raise Exception('Warmup mode {!r} not recognized!'.format(mode))

        log.info('Done warming up.')
        return 'OK'

    def _to_bool(self, value):
        return str(value).lower() not in ('false', '0')

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

    def _warmup_catalog(self, lexicons=True):
        start = time.time()

        self._log_cache_stats('before warmup')
        warmup_catalog(self.catalog, lexicons=lexicons)
        self._log_cache_stats('after warmup')

        duration = time.time() - start
        log.info("Catalog warmup took %.2fs." % duration)

    def _log_cache_stats(self, when):
        conn = self.context._p_jar
        db = conn.db()
        log.info('')
        log.info("Cache size (# objs) (%s): %s" % (when, db.cacheSize()))
        log.info("Total estimated cache size (%s): %.2f MiB" %
                 (when, conn._cache.total_estimated_size / 1024.0 / 1024.0))
        log.info('')


def warmup_catalog(catalog, lexicons=True):
    log.info('Loading metadata...')
    list(catalog._catalog.data.items())

    log.info('Loading indexes...')
    for index_name in catalog.indexes():
        log.info('Loading index %r...' % index_name)
        index = catalog._catalog.indexes[index_name]
        warmup_index(index, lexicons=lexicons)

    log.info('Done warming up catalog.')


def warmup_index(index, lexicons=True):
    """Load internal index data structures by iterating over contents of
    *BTrees and *TreeSets, causing their respective buckets to be loaded.
    """
    if isinstance(index, (FieldIndex, DateIndex, KeywordIndex)):
        # value -> set of docids
        for key, value in index._index.items():
            list(value)

        # docid -> field value
        list(index._unindex.items())

    elif isinstance(index, ZCTextIndex):
        # wid -> doc2score
        list(index.index._wordinfo.items())
        # docid -> docweight
        list(index.index._docweight.items())
        # docid -> encoded wordids
        list(index.index._docwords.items())

        if lexicons:
            warmup_lexicon(index)

    elif isinstance(index, BooleanIndex):
        list(index._index)
        list(index._unindex.items())

    elif isinstance(index, ExtendedPathIndex):
        # path component -> level -> set of docids
        for comp, index_comp in index._index.items():
            for level, docids_set in index_comp.items():
                list(docids_set)

        # path -> set of docids
        for path, docids_set in index._index_parents.items():
            list(docids_set)

        # path -> docid
        for path, docid in index._index_items.items():
            assert isinstance(docid, int)

        # docid -> path
        for docid, path in index._unindex.items():
            assert isinstance(docid, int)

    elif isinstance(index, DateRangeIndex):
        # no forward index
        for docid, date_range in index._unindex.items():
            assert isinstance(docid, int)

    elif isinstance(index, GopipIndex):
        # not a real index
        return

    elif isinstance(index, UUIDIndex):
        list(index._index.items())
        list(index._unindex.items())

    else:
        log.warn('Unexpected index type %r for index %r, skipping.' %
                 (index.__class__, index.id))


def warmup_lexicon(zc_text_index):
    lexicon = aq_base(zc_text_index.index._lexicon)
    log.info('Loading lexicon %r for index %r...' % (
        lexicon.id, zc_text_index.id))

    # word -> wid
    list(lexicon._wids.items())

    # wid -> word
    list(lexicon._words.items())
