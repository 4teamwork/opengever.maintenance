# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class DossierExporter(BaseExporter):
    """Exports dossiers as dossiers.csv."""

    key = 'dossiers'
    label = u'Dossiers'
    filename = 'dossiers.csv'
    headers = [
        u'Dossier UID',
        u'Dossier ID',
        u'Status',
        u'Titel',
        u'Aktenzeichen Nr.',
        u'Ordnungssystem - Ordnungsposition UID',
        u'Übergeordnete Dossier UID',
        u'Beschreibung',
        u'Federführend - Benutzer UID',
        u'Beginn',
        u'Ende',
        u'Schlagwörter UID',
        u'Verwandte Dossiers - Dossier UID',
    ]
