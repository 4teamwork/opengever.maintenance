# -*- coding: utf-8 -*-
from opengever.contact.service import ContactService
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter


class ContactExporter(BaseExporter):
    """Exports contacts (Kontakte) as contacts.csv."""

    key = 'contacts'
    label = u'Kontakte'
    filename = 'contacts.csv'
    id_column = u'Kontakt UID'
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

    def get_items(self):
        brains = ContactService().all_contact_brains()
        contacts = [brain.getObject() for brain in brains]
        return sorted(contacts, key=lambda c: (c.lastname or u'', c.firstname or u''))

    def row_for_item(self, item):
        return [
            unicode(item.UID()),
            item.salutation or u'',
            item.academic_title or u'',
            item.firstname or u'',
            item.lastname or u'',
            item.function or u'',
            item.department or u'',
            item.company or u'',
            item.phone_office or u'',
            item.email or u'',
            item.phone_mobile or u'',
            item.email2 or u'',
            item.phone_home or u'',
            item.url or u'',
            item.phone_fax or u'',
            item.address1 or u'',
            item.address2 or u'',
            item.zip_code or u'',
            item.city or u'',
            item.country or u'',
            item.description or u'',
        ]
