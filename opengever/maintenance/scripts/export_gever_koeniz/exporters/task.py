# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class TaskExporter(BaseExporter):
    """Exports (open) tasks as tasks.csv."""

    key = 'tasks'
    label = u'Aufgaben'
    filename = 'tasks.csv'
    headers = [
        u'Aufgabe UID',
        u'Aufgabe-ID',
        u'Dossier',
        u'Übergeordnete Aufgabe',
        u'Status',
        u'Titel',
        u'Beschreibung',
        u'Zu Erledigen bis',
        u'Erinnerung',
        u'Auftragnehmer',
        u'Auftragnehmer UID',
        u'Auftraggeber',
        u'Auftraggeber UID',
        u'Auftragstyp',
        u'Persönliche Aufgabe',
        u'Informierte Beteiligte',
        u'Informierte Beteiligte UID',
        u'Dokumente UID',
    ]
