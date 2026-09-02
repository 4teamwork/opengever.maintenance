# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
import os


class DocumentExporter(BaseExporter):
    """Exports documents as documents.csv, and optionally their blob files
    into documents/<UID>/<Filename>.<ext>.
    """

    key = 'documents'
    label = u'Dokumente'
    filename = 'documents.csv'
    id_column = u'Dokument UID'
    headers = [
        u'Dokument UID',
        u'Dokument-ID',
        u'Übergeordnetes Dossier',
        u'Übergeordnete Aufgabe',
        u'Titel',
        u'Dokumentennummer',
        u'Datei',
        u'Beschreibung',
        u'Dokumentdatum',
        u'Eingangsdatum',
        u'Ausgangsdatum',
        u'Dokumenttyp',
        u'Autor',
        u'In Papierform aufbewahrt',
    ]

    def export_blobs(self, export_dir):
        """Create the documents/ folder that will hold one subfolder per
        document UID, containing the document's blob file.
        """
        blobs_dir = os.path.join(export_dir, 'documents')
        os.makedirs(blobs_dir)
