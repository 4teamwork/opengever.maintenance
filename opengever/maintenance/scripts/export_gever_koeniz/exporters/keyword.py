# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class KeywordExporter(BaseExporter):
    """Exports keywords (Schlagwörter) as keywords.csv."""

    key = 'keywords'
    label = u'Schlagwörter'
    filename = 'keywords.csv'
    headers = [
        u'Schlagwort UID',
        u'Schlagwort Bezeichnung',
    ]
