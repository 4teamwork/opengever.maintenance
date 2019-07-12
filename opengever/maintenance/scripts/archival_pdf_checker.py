from collections import Counter
from collections import OrderedDict
from ftw.bumblebee.interfaces import IBumblebeeDocument
from opengever.document.archival_file import ArchivalFileConverter
from opengever.document.archival_file import STATE_CONVERTED
from opengever.document.archival_file import STATE_CONVERTING
from opengever.document.archival_file import STATE_FAILED_PERMANENTLY
from opengever.document.archival_file import STATE_FAILED_TEMPORARILY
from opengever.document.archival_file import STATE_MANUALLY_PROVIDED
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import TextTable
from plone import api
from pprint import pprint
import argparse
import logging
import sys
import transaction


logger = logging.getLogger('archival_file_checker')
logger.setLevel(logging.INFO)


STATES = {
    STATE_CONVERTING: 'STATE_CONVERTING',
    STATE_CONVERTED: 'STATE_CONVERTED',
    STATE_MANUALLY_PROVIDED: 'STATE_MANUALLY_PROVIDED',
    STATE_FAILED_TEMPORARILY: 'STATE_FAILED_TEMPORARILY',
    STATE_FAILED_PERMANENTLY: 'STATE_FAILED_PERMANENTLY',
    None: 'STATE_NONE',
}


class ArchivalPDFChecker(object):

    def __init__(self, context):
        self.context = context
        self.all_dossier_stats = None
        self.dossiers_with_missing_pdf = None
        self.total_resolved_dossiers = 0

    def run(self):
        catalog = api.portal.get_tool('portal_catalog')

        path = '/'.join(self.context.getPhysicalPath())
        resolved_dossier_brains = catalog.unrestrictedSearchResults(
            path=path,
            is_subdossier=False,
            sort_on='path',
            object_provides=IDossierMarker.__identifier__,
            review_state='dossier-state-resolved')

        self.total_resolved_dossiers = len(resolved_dossier_brains)
        all_dossier_stats = OrderedDict()
        dossiers_with_missing_pdf = []

        for brain in resolved_dossier_brains:
            dossier = brain.getObject()

            dossier_stats = Counter()
            dossier_stats['states'] = Counter()
            dossier_path = brain.getPath()

            contained_docs = catalog.unrestrictedSearchResults(
                path={'query': dossier_path},
                object_provides=IBaseDocument.__identifier__,
            )
            dossier_stats['total_docs'] = len(contained_docs)

            docs_with_missing_pdf = []
            for doc_brain in contained_docs:
                doc = doc_brain.getObject()

                # Determine if this document should have an archival PDF
                should_have_pdf = self.should_have_pdf(doc)

                if should_have_pdf:
                    dossier_stats['should_have_pdf'] += 1

                # Check if an archival PDF is present
                if getattr(doc, 'archival_file', None) is not None:
                    dossier_stats['with_pdf'] += 1
                else:
                    dossier_stats['without_pdf'] += 1
                    if should_have_pdf:
                        dossier_stats['missing'] += 1

                        # Document should be triggered for archival file
                        # conversion
                        docs_with_missing_pdf.append(doc)

                # Record conversion state
                converter = ArchivalFileConverter(doc)
                conversion_state = converter.get_state()
                assert conversion_state in STATES
                dossier_stats['states'][conversion_state] += 1

            all_dossier_stats[dossier_path] = dossier_stats

            # If the dossier contain documents with missing archival files
            # add it to the
            if docs_with_missing_pdf:
                dossiers_with_missing_pdf.append({
                    'dossier': dossier,
                    'missing': docs_with_missing_pdf})

        self.all_dossier_stats = all_dossier_stats
        self.dossiers_with_missing_pdf = dossiers_with_missing_pdf

    def should_have_pdf(self, doc):
        if doc.portal_type == 'ftw.mail.mail':
            return False

        if doc.title.startswith(u'Dossier Journal '):
            print "Skipping journal PDF"
            return False

        bdoc = IBumblebeeDocument(doc)
        if not bdoc.is_convertable():
            return False

        return True

    def render_result_tables(self):
        all_dossier_stats = self.all_dossier_stats
        assert all_dossier_stats is not None

        totals = Counter()

        dossier_table = TextTable()
        dossier_table.add_row((
            'path',
            'total_docs',
            'should_have_pdf',
            'with_pdf',
            'without_pdf',
            'missing',
        ))

        totals['total_resolved_dossiers'] = self.total_resolved_dossiers
        totals['total_candidate_dossiers'] = len(all_dossier_stats)

        for dossier_path, dossier_stats in all_dossier_stats.items():
            dossier_table.add_row((
                dossier_path,
                dossier_stats['total_docs'],
                dossier_stats['should_have_pdf'],
                dossier_stats['with_pdf'],
                dossier_stats['without_pdf'],
                dossier_stats['missing'],
            ))

            totals['total_docs'] += dossier_stats['total_docs']
            totals['total_should_have_pdf'] += dossier_stats['should_have_pdf']
            totals['total_missing'] += dossier_stats['missing']

        output = ''
        output += dossier_table.generate_output()
        output += '\n\n'

        totals_table = TextTable()
        totals_table.add_row((
            'total_resolved_dossiers',
            'total_candidate_dossiers',
            'total_docs',
            'total_should_have_pdf',
            'total_missing',
        ))
        totals_table.add_row((
            totals['total_resolved_dossiers'],
            totals['total_candidate_dossiers'],
            totals['total_docs'],
            totals['total_should_have_pdf'],
            totals['total_missing'],
        ))

        output += totals_table.generate_output()

        return output

    def detailed_report(self):
        return self.dossiers_with_missing_pdf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['report_table', 'detailed_report'],
                        help='solr-maintenance mode')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-n', dest='dryrun', default=False, help='Dryrun')

    options = parser.parse_args(sys.argv[3:])
    app = setup_app()

    portal = setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    checker = ArchivalPDFChecker(portal)
    checker.run()

    if options.mode == 'report_table':
        print checker.render_result_tables()

    if options.mode == 'detailed_report':
        print "Overview table:"
        print 50 * "="
        print ""
        print checker.render_result_tables()

        print ""
        print ""
        print "Dossiers with missing archival files"
        print 50 * "="
        print ""
        print ""
        pprint(checker.detailed_report())

    if not options.dryrun:
        transaction.commit()


if __name__ == '__main__':
    main()
