"""
CMI Export

Exports GEVER data as CSV files for the CMI migration, as specified in
export_feature/migration_concept.md. Exports all registered types by
default; use --export to restrict to a subset. Document blob files are
only exported when --export-blobs is given.

Usage:

Help: bin/instance run export_gever_koeniz.py -h
"""
from collections import OrderedDict
from zope.globalrequest import setRequest
from opengever.maintenance import dm
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.comment import CommentExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.contact import ContactExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.document import DocumentExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.dossier import DossierExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.keyword import KeywordExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.meeting import MeetingExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.member import MemberExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.participation import ParticipationExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.proposal import ProposalExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.repository import RepositoryExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.task import TaskExporter
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.user import UserExporter
import argparse
import logging
import os
import sys
import time
import transaction


logger = logging.getLogger('opengever.maintenance.export_gever_koeniz')
logging.root.addHandler(logging.StreamHandler(stream=sys.stdout))
logging.root.setLevel(logging.INFO)


EXPORTER_REGISTRY = OrderedDict([
    ('repository', RepositoryExporter),
    ('dossiers', DossierExporter),
    ('documents', DocumentExporter),
    ('tasks', TaskExporter),
    ('keywords', KeywordExporter),
    ('comments', CommentExporter),
    ('participations', ParticipationExporter),
    ('members', MemberExporter),
    ('meetings', MeetingExporter),
    ('proposals', ProposalExporter),
    ('users', UserExporter),
    ('contacts', ContactExporter),
])


class GeverKoenizExporter(object):
    """Orchestrates the CMI CSV / blob export."""

    def __init__(self, portal, export_base_dir, export_types, export_blobs):
        self.portal = portal
        self.export_types = export_types
        self.export_blobs = export_blobs
        self.export_dir = self._create_export_dir(export_base_dir)

    def _create_export_dir(self, base_dir):
        name = 'export_{}'.format(int(time.time()))
        path = os.path.join(base_dir, name)
        os.makedirs(path)
        return path

    def run(self):
        stats = OrderedDict()
        exporters = OrderedDict()
        for key in self.export_types:
            exporter = EXPORTER_REGISTRY[key](self.portal)
            stats[exporter.label] = exporter.export(self.export_dir)
            exporters[key] = exporter

        if self.export_blobs:
            EXPORTER_REGISTRY['documents'](self.portal).export_blobs(self.export_dir)

        self._validate_references(exporters)
        self._log_stats(stats)

    def _validate_references(self, exporters):
        logger.info(u'Validating references...')
        ok = missing = skipped = 0
        for key, exporter in exporters.items():
            for target_key, refs in exporter.referenced_ids.items():
                target_label = EXPORTER_REGISTRY[target_key].label
                if target_key not in exporters:
                    logger.warning(
                        u'  %s -> %s: SKIPPED (%s not exported in this run)',
                        exporter.label, target_label, target_label)
                    skipped += 1
                    continue

                known_ids = exporters[target_key].exported_ids
                missing_ids = sorted(refs - known_ids)
                if missing_ids:
                    sample = u', '.join(missing_ids[:20])
                    if len(missing_ids) > 20:
                        sample += u', ... and {} more'.format(len(missing_ids) - 20)
                    logger.warning(
                        u'  %s -> %s: %d/%d references missing: %s',
                        exporter.label, target_label, len(missing_ids), len(refs), sample)
                    missing += 1
                else:
                    logger.info(
                        u'  %s -> %s: OK (%d references)',
                        exporter.label, target_label, len(refs))
                    ok += 1

        logger.info(
            u'Reference validation: %d OK, %d missing, %d skipped', ok, missing, skipped)

    def _log_stats(self, stats):
        logger.info(
            u'Export finished (%s): %s',
            self.export_dir,
            u', '.join(u'{}: {}'.format(label, count)
                       for label, count in stats.items()))


if __name__ == '__main__':
    dm()  # Sets up the `plone` global, using the sole Plone site.
    setRequest(plone.REQUEST)
    transaction.doom()  # This export is read-only.

    parser = argparse.ArgumentParser(
        description='Export GEVER data as CSV for CMI migrations.')
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--export', nargs='+', choices=EXPORTER_REGISTRY.keys(), metavar='TYPE',
        help='Restrict export to given type(s). Possible values: {}.'.format(
            ', '.join(EXPORTER_REGISTRY.keys())))
    group.add_argument(
        '--export-all', action='store_true', default=False,
        help='Export all registered types (does not include blobs). This is '
             'also the default when neither --export nor --export-all is given.')
    parser.add_argument(
        '--export-blobs', action='store_true', default=False,
        help='Also export document files into documents/<UID>/<Filename>.<ext>. '
             'Default: off.')
    parser.add_argument(
        '--export-path', dest='export_path', default='/tmp/',
        help='Base directory the export_<timestamp>/ folder is created in. '
             'Default: /tmp/.')
    options = parser.parse_args(sys.argv[3:])

    export_types = options.export if options.export else list(EXPORTER_REGISTRY.keys())
    GeverKoenizExporter(plone, options.export_path, export_types, options.export_blobs).run()
