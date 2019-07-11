from collections import Counter
from collections import OrderedDict
from DateTime import DateTime
from ftw.bumblebee.interfaces import IBumblebeeDocument
from opengever.document.archival_file import ArchivalFileConverter
from opengever.document.archival_file import STATE_CONVERTED
from opengever.document.archival_file import STATE_CONVERTING
from opengever.document.archival_file import STATE_FAILED_PERMANENTLY
from opengever.document.archival_file import STATE_FAILED_TEMPORARILY
from opengever.document.archival_file import STATE_MANUALLY_PROVIDED
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.utils import TextTable
from plone import api


STATES = {
    STATE_CONVERTING: 'STATE_CONVERTING',
    STATE_CONVERTED: 'STATE_CONVERTED',
    STATE_MANUALLY_PROVIDED: 'STATE_MANUALLY_PROVIDED',
    STATE_FAILED_TEMPORARILY: 'STATE_FAILED_TEMPORARILY',
    STATE_FAILED_PERMANENTLY: 'STATE_FAILED_PERMANENTLY',
    None: 'STATE_NONE',
}

FEATURE_ACTIVATION_DATE = DateTime('2017/03/31 16:52:24.962264 GMT+2')


class ArchivalPDFChecker(object):

    def __init__(self, context):
        self.context = context
        self.all_dossier_stats = None
        self.resolved_before_feature = 0
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

        for brain in resolved_dossier_brains:
            dossier = brain.getObject()
            wftool = api.portal.get_tool('portal_workflow')
            history = wftool.getInfoFor(dossier, "review_history")
            resolve_date = None
            for action_info in history:
                if action_info.get('action') == 'dossier-transition-resolve':
                    resolve_date = action_info.get('time')

            if FEATURE_ACTIVATION_DATE > resolve_date:
                print "Skipping non-candidate dossier"
                self.resolved_before_feature += 1
                continue

            dossier_stats = Counter()
            dossier_stats['states'] = Counter()
            dossier_path = brain.getPath()

            contained_docs = catalog.unrestrictedSearchResults(
                path={'query': dossier_path},
                object_provides=IBaseDocument.__identifier__,
            )
            dossier_stats['total_docs'] = len(contained_docs)
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

                # Record conversion state
                converter = ArchivalFileConverter(doc)
                conversion_state = converter.get_state()
                assert conversion_state in STATES
                dossier_stats['states'][conversion_state] += 1

            all_dossier_stats[dossier_path] = dossier_stats

        self.all_dossier_stats = all_dossier_stats

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
        totals['total_resolved_before_feature'] = self.resolved_before_feature
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
            'total_resolved_before_feature',
            'total_candidate_dossiers',
            'total_docs',
            'total_should_have_pdf',
            'total_missing',
        ))
        totals_table.add_row((
            totals['total_resolved_dossiers'],
            totals['total_resolved_before_feature'],
            totals['total_candidate_dossiers'],
            totals['total_docs'],
            totals['total_should_have_pdf'],
            totals['total_missing'],
        ))

        output += totals_table.generate_output()

        return output
