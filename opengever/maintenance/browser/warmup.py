from Acquisition import aq_base
from App.config import getConfiguration
from collections import OrderedDict
from datetime import datetime
from functools import wraps
from opengever.contact.contact import IContact
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.inbox.forwarding import IForwarding
from opengever.maintenance.utils import elevated_privileges
from opengever.maintenance.utils import get_rss
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
from Products.PluginIndexes.interfaces import IPluggableIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex
import json
import logging
import os
import threading
import time
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


class CacheStats(object):

    def __init__(self, conn, settings):
        self.conn = conn
        self.db = conn.db()
        self.stats_by_idx = {}
        self.settings = settings

    def get_rss(self):
        """Returns this process' current RSS (in kb).
        """
        return get_rss()

    def get_cache_size(self):
        """Returns the PickleCache's current size (in # of objects).
        """
        return self.conn.db().cacheSize()

    def get_estimated_size(self):
        """Returns the current estimated size of the PickleCache in bytes.
        """
        return self.conn._cache.total_estimated_size

    @staticmethod
    def track(func):
        """Decorator that tracks change in time, RSS, cache size in # objs
        and estimated cache size in bytes.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            view = self
            cache_stats = view.cache_stats

            # Choose a tag (label for the thing being tracked) based on which
            # method is being decorated.
            #
            # For the warmup_index / warmup_lexicon methods this will be the
            # index's name, for the stats regarding the entire catalog or the
            # warmup of metadata we'll just use a static label of 'total'
            # (since there is no granular tracking data available).
            if func.__name__ in ('warmup_catalog', 'warmup_metadata'):
                tag = 'total'
            else:
                index = args[0]

                if not IPluggableIndex.providedBy(index):
                    raise TypeError("%r is not an index" % index)

                tag = index.__name__

            category = func.__name__

            before = {
                'time': time.time(),
                'rss': cache_stats.get_rss(),
                'objs': cache_stats.get_cache_size(),
            }

            try:
                return func(self, *args, **kwargs)
            finally:

                after = {
                    'time': time.time(),
                    'rss': cache_stats.get_rss(),
                    'objs': cache_stats.get_cache_size(),
                }

                stats_by_idx = view.cache_stats.stats_by_idx
                if category not in stats_by_idx:
                    stats_by_idx[category] = {}

                stats = {
                    'duration': after['time'] - before['time'],
                    'rss_delta': after['rss'] - before['rss'],
                    'obj_delta': after['objs'] - before['objs'],
                }

                stats_by_idx[category][tag] = stats

        return wrapper

    def format_rss_delta(self, rss_delta):
        return 'RSS (delta): %7.2f MiB' % (rss_delta / 1024.0)

    def format_obj_delta(self, obj_delta):
        return '# objs in cache (delta): %s' % obj_delta

    def format_duration(self, duration):
        return 'Duration: %.2fs' % duration

    def format_stats_line(self, data):
        line = "%-20s %-30s %s" % (
            self.format_duration(data['duration']),
            self.format_rss_delta(data['rss_delta']),
            self.format_obj_delta(data['obj_delta'])
        )
        return line

    def display_current_stats(self):
        log.info('Current RSS: %.2f MiB' % (self.get_rss() / 1024.0))
        log.info('Current cache size (# objs): %s' %
                 self.get_cache_size())
        log.info('Current estimated cache size (MiB): %.2f' % (
            self.get_estimated_size() / 1024.0 / 1024.0))
        log.info('')

    def display_summary(self):
        """Log a summary of detailed collected delta stats.
        """
        log.info('')
        log.info('Stats (deltas)')

        log.info('')
        log.info('Metadata stats')
        data = self.stats_by_idx['warmup_metadata']['total']
        log.info('Metadata warmup took %.2fs.' % data['duration'])
        log.info('Instance memory growth: %s' %
                 self.format_rss_delta(data['rss_delta']))
        log.info('Cache size growth: %s' %
                 self.format_obj_delta(data['obj_delta']))

        log.info('')
        log.info('Per index stats')
        for index_name, data in self.stats_by_idx['warmup_index'].items():
            log.info("Index: %-25s %s " % (
                index_name, self.format_stats_line(data)))

        log.info('')
        log.info('Per lexicon stats')
        for lexicon_name, data in self.stats_by_idx['warmup_lexicon'].items():
            log.info("Lexicon: %-20s %s" % (
                lexicon_name, self.format_stats_line(data)))

        data = self.stats_by_idx['warmup_catalog']['total']
        log.info('')
        log.info('Catalog warmup took %.2fs.' % data['duration'])
        log.info('Instance memory growth: %s' %
                 self.format_rss_delta(data['rss_delta']))
        log.info('Cache size growth: %s' %
                 self.format_obj_delta(data['obj_delta']))

    def dump_warmup_stats(self):
        """Dump warmup stats to a file in var/log/.

        The file will be named uniqely per connection, and will be overwritten
        on every warmup (for that instance and connection).
        """
        eventlog_path = get_eventlog_path()
        logdir, logname = os.path.split(eventlog_path)
        deployment_name = logdir.split(os.sep)[-3]
        instance_name, _ = os.path.splitext(logname)
        thread_name = threading.current_thread().name

        # Choose a name that's unique per connection
        dump_name = 'warmup-stats-%s-%s.json' % (instance_name, thread_name)
        dump_path = os.path.join(logdir, dump_name)

        stats_dump = OrderedDict()
        stats_dump['timestamp'] = datetime.now().isoformat()
        stats_dump['deployment'] = deployment_name
        stats_dump['instance'] = instance_name
        stats_dump['thread'] = thread_name
        stats_dump['settings'] = self.settings
        stats_dump['stats_by_idx'] = self.stats_by_idx

        with open(dump_path, 'w') as stats_dump_file:
            json.dump(stats_dump, stats_dump_file,
                      indent=4, separators=(',', ': '))
        log.info('Warmup stats dumped to %r' % dump_path)


class WarmupView(BrowserView):
    """View to warm up a GEVER instance.
    """

    def __call__(self):
        # Doom transaction to ensure no writes can ever happen. This view
        # is accessible to Anonymous (zope2.Public), and we run it with
        # elevated privileges.
        transaction.doom()

        self.catalog = api.portal.get_tool('portal_catalog')
        thread = threading.current_thread().name
        conn = self.context._p_jar

        mode = self.request.form.get('mode', 'minimal')

        self.settings = {
            'zctext_indexes': self._to_bool(self.request.form.get('zctext_indexes', True)),
            'lexicons': self._to_bool(self.request.form.get('lexicons', True)),
            'unindexes': self._to_bool(self.request.form.get('unindexes', False)),
        }

        # Elevate privileges in order to be able to load objects
        with elevated_privileges():
            log.info(
                'Warming up instance (mode == {}, Thread {!r}, '
                'Connection {!r})...'.format(mode, thread, conn))
            log.info('Warmup settings: {!r} '.format(self.settings))

            if mode == 'minimal':
                self._warmup_minimal()
            elif mode == 'medium':
                self._warmup_medium()
            elif mode == 'catalog':
                self.cache_stats = CacheStats(conn, self.settings)
                self._warmup_catalog()
            else:
                raise Exception(
                    'Warmup mode {!r} not recognized!'.format(mode))

            log.info('Done warming up.')
        return 'OK'

    def _to_bool(self, value):
        return str(value).lower() not in ('false', '0')

    def _warmup_minimal(self):
        # Fetch repository brains and objects
        repo_brains = self.catalog.unrestrictedSearchResults(
            object_provides=IRepositoryFolder.__identifier__)
        for brain in repo_brains:
            brain.getObject()

    def _warmup_medium(self):
        # Fetch brains for all common types
        for type_iface in COMMON_TYPES:
            count = 0
            brains = self.catalog.unrestrictedSearchResults(
                object_provides=type_iface.__identifier__)
            for brain in brains:
                count += 1
            log.info('Fetched {} brains for type {}'.format(
                count, type_iface.__identifier__))

        self._warmup_minimal()

    def _warmup_catalog(self):
        log.info('')
        log.info('Stats before warmup (absolute):')
        self.cache_stats.display_current_stats()

        self.warmup_catalog()
        self.cache_stats.display_summary()

        log.info('')
        log.info('Stats after warmup (absolute):')
        self.cache_stats.display_current_stats()
        self.cache_stats.dump_warmup_stats()

    @CacheStats.track
    def warmup_catalog(self):
        log.info('Loading metadata...')
        self.warmup_metadata()

        log.info('Loading indexes...')
        for index_name in self.catalog.indexes():
            index = self.catalog._catalog.indexes[index_name]
            if not self.settings['zctext_indexes'] and isinstance(index, ZCTextIndex):
                continue
            log.info('Loading index %r...' % index_name)
            self.warmup_index(index)

        log.info('Done warming up catalog.')

    @CacheStats.track
    def warmup_metadata(self):
        list(self.catalog._catalog.data.items())

    @CacheStats.track
    def warmup_index(self, index):
        """Load internal index data structures by iterating over contents of
        *BTrees and *TreeSets, causing their respective buckets to be loaded.
        """
        if isinstance(index, (FieldIndex, DateIndex, KeywordIndex)):
            # value -> set of docids
            for key, value in index._index.items():
                list(value)

            if self.settings['unindexes']:
                # docid -> field value
                list(index._unindex.items())

        elif isinstance(index, ZCTextIndex):
            # wid -> doc2score
            list(index.index._wordinfo.items())
            # docid -> docweight
            list(index.index._docweight.items())
            # docid -> encoded wordids
            list(index.index._docwords.items())

            if self.settings['lexicons']:
                self.warmup_lexicon(index)

        elif isinstance(index, BooleanIndex):
            list(index._index)
            if self.settings['unindexes']:
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

            if self.settings['unindexes']:
                # docid -> path
                for docid, path in index._unindex.items():
                    assert isinstance(docid, int)

        elif isinstance(index, DateRangeIndex):
            if self.settings['unindexes']:
                # no forward index
                for docid, date_range in index._unindex.items():
                    assert isinstance(docid, int)

        elif isinstance(index, GopipIndex):
            # not a real index
            return

        elif isinstance(index, UUIDIndex):
            list(index._index.items())
            if self.settings['unindexes']:
                list(index._unindex.items())

        else:
            log.warn('Unexpected index type %r for index %r, skipping.' %
                     (index.__class__, index.id))

    @CacheStats.track
    def warmup_lexicon(self, zc_text_index):
        lexicon = aq_base(zc_text_index.index._lexicon)
        log.info('Loading lexicon %r for index %r...' % (
            lexicon.id, zc_text_index.id))

        # word -> wid
        list(lexicon._wids.items())

        # wid -> word
        list(lexicon._words.items())


def get_eventlog_path():
    """Determine the path of the instance's eventlog.
    """
    zconf = getConfiguration()
    eventlog = getattr(zconf, 'eventlog', None)
    if eventlog is None:
        log.warn(
            "Warmup: Couldn't find eventlog configuration in order "
            "to determine logfile location!")
        return None

    handler_factories = eventlog.handler_factories
    eventlog_path = handler_factories[0].section.path
    return eventlog_path
