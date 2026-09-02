# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class ParticipationExporter(BaseExporter):
    """Exports participations (Beteiligungen) as participations.csv."""

    key = 'participations'
    label = u'Beteiligungen'
    filename = 'participations.csv'
    headers = [
        u'Dossier - UID',
        u'Benutzer - UID',
        u'Kontakt - UID',
        u'Rollen',
    ]
