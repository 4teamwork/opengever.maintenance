# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class ProposalExporter(BaseExporter):
    """Exports agenda items / proposals (Traktanden) as proposals.csv."""

    key = 'proposals'
    label = u'Traktanden'
    filename = 'proposals.csv'
    headers = [
        u'Traktandum UID',
        u'Traktandum Nr.',
        u'Beschlussnummer',
        u'Titel',
        u'Beschreibung',
        u'Dossier UID',
        u'Sitzung UID',
        u'Auftraggeber UID',
        u'Antragsdokument UID',
        u'Status',
        u'Beilagen UID',
        u'Entkoppelte Beilagen UID',
        u'Protokollauszug UID',
    ]
