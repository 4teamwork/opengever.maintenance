"""
Script that reports documents missing their archival file, and optionally
queues them for conversion by a nightly job.

Usage: archival_file_checker.py [-n] [report_missing | queue_missing]

The script takes one of two commmands:

report_missing
    Reports all documents missing their archival file.

queue_missing
    Reports the documents, and additionally queues them for conversion by
    a nightly job (by adding them to a persistent queue on the site root).

The script will display some basic console output, and automatically log
that ouput and some more detailed information to a logfile.
"""
from BTrees.IIBTree import IITreeSet
from BTrees.IOBTree import IOBTree
from collections import Counter
from collections import OrderedDict
from ftw.bumblebee.interfaces import IBumblebeeDocument
from opengever.document.archival_file import ArchivalFileConverter
from opengever.document.archival_file import STATE_FAILED_TEMPORARILY
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.nightly_archival_file_job import MISSING_ARCHIVAL_FILE_KEY
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from opengever.private.dossier import IPrivateDossier
from plone import api
from Products.CMFPlone.interfaces import IPloneSiteRoot
from zope.annotation import IAnnotations
from zope.component import getUtility
from zope.component.hooks import getSite
from zope.component.hooks import setSite
from zope.intid.interfaces import IIntIds
import argparse
import gc
import json
import logging
import os
import subprocess
import sys
import transaction


logger = logging.getLogger('archival_file_checker')
logger.setLevel(logging.INFO)


# Duplicating this here instead of importing from opengever.dossier.resolve
# in order to avoid hard dependency from opengever.maintenance
AFTER_RESOLVE_JOBS_PENDING_KEY = 'opengever.dossier.resolve.after_resolve_jobs_pending'


class ArchivalFileChecker(object):
    """Checks whether documents that should have an archival file actually
    do have one, and reports the ones that don't.

    With command 'queue_missing' also queues those documents for conversion
    via a nightly job.
    """

    def __init__(self, context, options, logger):
        self.context = context
        self.options = options
        self.logger = logger

        self.log = logger.log
        self.log_to_file = logger.log_to_file
        self.log_memory = logger.log_memory

        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)
        self.all_dossier_stats = None

        # Bookkeeping stats for memory usage
        self.rss_max = 0
        self.checked_docs_count = 0

    def run(self):
        assert IPloneSiteRoot.providedBy(self.context)

        missing_by_dossier = self.check()

        if self.options.cmd == 'queue_missing':
            self.queue_missing(missing_by_dossier)

        self.log("")
        self.log("Detailed log written to %s" % self.logger.logfile_path)
        self.log("Memory log written to %s" % self.logger.memory_logfile_path)

    def check(self):
        """For all candidate dossiers, check if the documents contained in
        them are missing their archival file.

        A candidate dossier is any dossier that:
        - is resolved
        - doesn't have any after-resolve jobs pending
        - isn't a subdossier (we'll check docs recursively)
        - isn't in the private area

        For each dossier we'll gather some stats that allow us to cross-check
        that the script is operating correctly, even though in the end we
        only really need a list of documents (per dossier) that are missing
        their archival file.

        Once done, we'll display (and log) a summary report. In addition,
        a detailed list of the documents missing their archival file is
        logged to the file (but not displayed).

        We'll also show the current queue length at the end.
        """
        path = '/'.join(self.context.getPhysicalPath())
        resolved_dossier_brains = self.catalog.unrestrictedSearchResults(
            path=path,
            is_subdossier=False,
            sort_on='path',
            object_provides=IDossierMarker.__identifier__,
            review_state='dossier-state-resolved')

        all_dossier_stats = OrderedDict()
        missing_by_dossier = []

        for brain in resolved_dossier_brains:
            dossier = brain.getObject()

            if IPrivateDossier.providedBy(dossier):
                # Documents in private dossiers don't need archival files
                continue

            if self.after_resolve_jobs_pending(dossier):
                # Nightly resolve job for this dossier hasn't run yet, so
                # it's archival files *can't* exist yet
                continue

            self.log_memstats()

            dossier_stats, docs_missing_archival_file = self._check_dossier(dossier)
            dossier_intid = self.intids.getId(dossier)
            all_dossier_stats[dossier_intid] = dossier_stats

            if docs_missing_archival_file:
                missing_by_dossier.append({
                    'dossier': dossier_intid,
                    'missing': docs_missing_archival_file})

        self.all_dossier_stats = all_dossier_stats

        # Display (and log) summary report
        self.log("Archival file report")
        self.log(78 * "=")
        self.log("")

        result = self.render_result_tables()
        for line in result.splitlines():
            self.log(line)

        # Log individual documents that are missing archival file (per dossier)
        self.log_to_file("")
        self.log_to_file("List of documents missing archival file")
        self.log_to_file("=" * 80)
        self.log_to_file("")

        for group in missing_by_dossier:
            dossier_intid = group['dossier']
            doc_intids = group['missing']
            self.log_to_file('Dossier (IntID): %s' % dossier_intid)
            for doc_intid in doc_intids:
                self.log_to_file("  Document (IntId): %r" % doc_intid)
            self.log_to_file("")

        # Display current queue length, just to be helpful
        assert IPloneSiteRoot.providedBy(self.context)
        ann = IAnnotations(self.context)
        queue = ann.get(MISSING_ARCHIVAL_FILE_KEY, {})

        queued_dossiers = len(queue)
        queued_docs = sum(map(len, queue.values()))

        self.log("")
        self.log("Current queue length")
        self.log("=" * 80)
        self.log("%s documents queued (from %s dossiers)" % (
            queued_docs, queued_dossiers))

        return missing_by_dossier

    def log_memstats(self):
        rss = self.get_rss() / 1024.0
        self.rss_max = max(self.rss_max, rss)
        memstats = {
            'items': self.checked_docs_count,
            'rss_current': rss,
            'rss_max': self.rss_max,
        }
        self.log_memory(json.dumps(memstats))

    def get_rss(self):
        """Get current memory usage (RSS) of this process.
        """
        out = subprocess.check_output(
            ["ps", "-p", "%s" % os.getpid(), "-o", "rss"])
        try:
            return int(out.splitlines()[-1].strip())
        except ValueError:
            return 0

    def collect_garbage(self, site):
        # In order to get rid of leaking references, the Plone site needs to be
        # re-set in regular intervals using the setSite() hook. This reassigns
        # it to the SiteInfo() module global in zope.component.hooks, and
        # therefore allows the Python garbage collector to cut loose references
        # it was previously holding on to.
        setSite(getSite())

        # Trigger garbage collection for the cPickleCache
        site._p_jar.cacheGC()

        # Also trigger Python garbage collection.
        gc.collect()

        # (These two don't seem to affect the memory high-water-mark a lot,
        # but result in a more stable / predictable growth over time.
        #
        # But should this cause problems at some point, it's safe
        # to remove these without affecting the max memory consumed too much.)

    def _check_dossier(self, dossier):
        """Check an individual dossier's documents for missing archival file.

        First, we determine if a document in question actually *should* have
        an archival file. If it should, but doesn't, it's tracked as 'missing'.
        """
        dossier_stats = Counter()
        dossier_stats['states'] = Counter()
        dossier_path = '/'.join(dossier.getPhysicalPath())

        contained_docs = self.catalog.unrestrictedSearchResults(
            path={'query': dossier_path},
            object_provides=IBaseDocument.__identifier__,
        )
        dossier_stats['total_docs_in_dossier'] = len(contained_docs)

        docs_missing_archival_file = []
        for doc_brain in contained_docs:
            doc = doc_brain.getObject()
            doc_intid = self.intids.getId(doc)

            # GC every 500 items proved a happy medium between memory
            # high watermark and slowdown in runtime
            if self.checked_docs_count % 500 == 0:
                # Trigger GC to keep memory usage in check
                self.collect_garbage(self.context)

            # Determine if this document should have an archival file
            should_have_archival_file = self.should_have_archival_file(doc)

            if should_have_archival_file:
                dossier_stats['should_have_archival_file'] += 1

            # Check if an archival file is present
            if getattr(doc, 'archival_file', None) is not None:
                dossier_stats['with'] += 1
            else:
                dossier_stats['without'] += 1

                if should_have_archival_file:
                    dossier_stats['missing'] += 1
                    docs_missing_archival_file.append(doc_intid)

            self.checked_docs_count += 1

        return dossier_stats, docs_missing_archival_file

    def after_resolve_jobs_pending(self, dossier):
        ann = IAnnotations(dossier)
        return ann.get(AFTER_RESOLVE_JOBS_PENDING_KEY, False)

    def should_have_archival_file(self, doc):
        """Determine whether an IBaseDocument should have an archival file.
        """
        if doc.is_mail:
            return False

        if doc.title.startswith(u'Dossier Journal '):
            return False

        bdoc = IBumblebeeDocument(doc)
        if not bdoc.is_convertable():
            return False

        conversion_state = ArchivalFileConverter(doc).get_state()
        if conversion_state == STATE_FAILED_TEMPORARILY:
            # FAILED_TEMPORARILY really is quite permanent in most cases.
            # Currently it rather means that Bumblebee did post back an
            # explicit error.
            return False

        return True

    def render_result_tables(self):
        """Return a multiline string representation of the result tables.
        """
        all_dossier_stats = self.all_dossier_stats
        assert all_dossier_stats is not None

        totals = Counter()

        dossier_table = TextTable()
        dossier_table.add_row((
            'dossier_intid',
            'total_docs_in_dossier',
            'should_have_archival_file',
            'with',
            'without',
            'missing',
        ))

        totals['total_resolved_dossiers'] = len(all_dossier_stats)

        for dossier_path, dossier_stats in all_dossier_stats.items():
            dossier_table.add_row((
                dossier_path,
                dossier_stats['total_docs_in_dossier'],
                dossier_stats['should_have_archival_file'],
                dossier_stats['with'],
                dossier_stats['without'],
                dossier_stats['missing'],
            ))

            totals['total_docs'] += dossier_stats['total_docs_in_dossier']
            totals['total_should_have_archival_file'] += dossier_stats['should_have_archival_file']
            totals['total_missing'] += dossier_stats['missing']

        output = ''
        output += dossier_table.generate_output()
        output += '\n\n'

        totals_table = TextTable()
        totals_table.add_row((
            'total_resolved_dossiers',
            'total_docs',
            'total_should_have_archival_file',
            'total_missing',
        ))
        totals_table.add_row((
            totals['total_resolved_dossiers'],
            totals['total_docs'],
            totals['total_should_have_archival_file'],
            totals['total_missing'],
        ))

        output += totals_table.generate_output()

        return output

    def queue_missing(self, missing_by_dossier):
        """Queue conversion job for documents missing their archival file.

        This method takes a list of dicts, one per dossier that contains at
        least one document with a missing archival file.
        """
        self.log("")
        self.log("Queueing missing archival files")
        self.log("=" * 80)

        total_missing = sum([len(group['missing']) for group in missing_by_dossier])

        self.log("Queueing archival file conversion for %s total documents" % total_missing)

        assert IPloneSiteRoot.providedBy(self.context)
        ann = IAnnotations(self.context)
        if MISSING_ARCHIVAL_FILE_KEY not in ann:
            ann[MISSING_ARCHIVAL_FILE_KEY] = IOBTree()
        queue = ann[MISSING_ARCHIVAL_FILE_KEY]

        for group in missing_by_dossier:
            dossier_intid = group['dossier']
            missing = group['missing']
            self.log("  Queueing %s documents for Dossier %r" % (len(missing), dossier_intid))

            if dossier_intid in queue:
                self.log('  (Replacing already queued documents for Dossier %r)' % dossier_intid)

            queue[dossier_intid] = IITreeSet()
            for doc_intid in missing:
                queue[dossier_intid].add(doc_intid)
                self.log("    Queued Document: IntId %s" % (doc_intid))

        self.log("Done. Queued %s documents for conversion" % total_missing)


class Logger(LogFilePathFinder):
    """Quick & dirty logging facility that allows us to display and log
    messages at the same time, but also exclusively log to the file for
    details that would clutter the console output.
    """

    def __init__(self, filename_basis):
        super(Logger, self).__init__()
        self.logfile_path = self.get_logfile_path(filename_basis)
        self.memory_logfile_path = self.get_logfile_path(filename_basis + '-memory')

    def __enter__(self):
        self.logfile = open(self.logfile_path, 'w')
        self.memory_logfile = open(self.memory_logfile_path, 'w')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.memory_logfile.close()
        self.logfile.close()

    def log(self, line):
        if not line.endswith('\n'):
            line += '\n'
        sys.stdout.write(line)
        self.log_to_file(line)

    def log_to_file(self, line):
        if not line.endswith('\n'):
            line += '\n'
        self.logfile.write(line)

    def log_memory(self, line):
        if not line.endswith('\n'):
            line += '\n'
        self.memory_logfile.write(line)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', choices=['report_missing', 'queue_missing'],
                        help='Command')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-n', dest='dryrun', default=False, help='Dryrun')

    options = parser.parse_args(sys.argv[3:])

    # report_missing should always be readonly
    if options.cmd == 'report_missing':
        options.dryrun = True

    app = setup_app()
    portal = setup_plone(app, options)

    # Set pickle cache size to zero to avoid unbounded memory growth
    portal._p_jar._cache.cache_size = 0

    with Logger('archival-file-checker') as logger:
        if options.dryrun:
            transaction.doom()

        checker = ArchivalFileChecker(portal, options, logger)
        checker.run()

        if not options.dryrun:
            transaction.commit()


if __name__ == '__main__':
    main()
