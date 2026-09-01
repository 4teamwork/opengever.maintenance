# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class UserExporter(BaseExporter):
    """Exports users (Benutzer) as users.csv."""

    key = 'users'
    label = u'Benutzer'
    filename = 'users.csv'
    headers = [
        u'Benutzer UID',
        u'Status',
        u'Name',
        u'Vorname',
        u'E-Mail',
    ]
