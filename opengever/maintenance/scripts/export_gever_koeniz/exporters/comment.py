# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class CommentExporter(BaseExporter):
    """Exports comments (Kommentare) as comments.csv."""

    key = 'comments'
    label = u'Kommentare'
    filename = 'comments.csv'
    headers = [
        u'Datum',
        u'Text',
        u'Dossier - UID',
        u'Antrag - UID',
        u'Aufgabe - UID',
        u'Benutzer - UID',
    ]
