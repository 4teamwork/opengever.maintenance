# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class ContactExporter(BaseExporter):
    """Exports contacts (Kontakte) as contacts.csv."""

    key = 'contacts'
    label = u'Kontakte'
    filename = 'contacts.csv'
    headers = [
        u'Kontakt UID',
        u'Anrede',
        u'Titel',
        u'Vorname',
        u'Nachname',
        u'Funktion',
        u'Abteilung',
        u'Firma',
        u'Telefon Arbeit',
        u'E-Mail 1',
        u'Telefon Mobile',
        u'E-Mail 2',
        u'Telefon Privat',
        u'URL',
        u'Fax Arbeit',
        u'Adresse (Strasse / Nr.)',
        u'Adresszusatz',
        u'PLZ',
        u'Ort',
        u'Land',
        u'Beschreibung',
    ]
