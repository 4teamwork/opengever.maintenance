# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class RepositoryExporter(BaseExporter):
    """Exports the Ordnungssystem (repository) as repository.csv."""

    key = 'repository'
    label = u'Ordnungssystem'
    filename = 'repository.csv'
    headers = [
        u'Ordnungspositionsnummer',
        u'UID',
        u'Pfad',
        u'Titel der Ordnungsposition',
        u'Titel der Ordnungsposition (französisch)',
        u'Titel der Ordnungsposition (englisch)',
        u'Beschreibung (optional)',
        u'Klassifikation',
        u'Datenschutz',
        u'Öffentlichkeitsstatus',
        u'Aufbewahrungsdauer (Jahre)',
        u'Kommentar zur Aufbewahrungsdauer',
        u'Archivwürdigkeit',
        u'Kommentar zur Archivwürdigkeit',
        u'Archivische Schutzfrist (Jahre)',
        u'Gültig ab',
        u'Gültig bis',
    ]
