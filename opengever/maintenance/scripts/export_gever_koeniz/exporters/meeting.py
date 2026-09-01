# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class MeetingExporter(BaseExporter):
    """Exports meetings (Sitzungen) as meetings.csv."""

    key = 'meetings'
    label = u'Sitzungen'
    filename = 'meetings.csv'
    headers = [
        u'Sitzung UID',
        u'Gremium',
        u'Sitzungstitel',
        u'Status',
        u'Beginn',
        u'Ende',
        u'Vorsitz',
        u'Protokollführung',
        u'Ort',
        u'Sitzungsdossier UID',
        u'Sitzungsdokumente UID',
        u'Teilnehmende',
        u'Teilnehmende UID',
    ]
