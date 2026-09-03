# -*- coding: utf-8 -*-
from Acquisition import aq_inner
from Acquisition import aq_parent
from collections import OrderedDict
from opengever.base.interfaces import IReferenceNumberFormatter
from opengever.base.interfaces import IReferenceNumberSettings
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from zope.component import queryAdapter
from zope.i18n import translate


class RepositoryExporter(BaseExporter):
    """Exports the Ordnungssystem (repository) as repository.csv."""

    key = 'repository'
    label = u'Ordnungssystem'
    filename = 'repository.csv'
    id_column = u'Ordnungsposition UID'
    headers = [
        u'Ordnungspositionsnummer',
        u'Ordnungsposition UID',
        u'Übergeordnete Ordnungsposition - UID',
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
    reference_columns = OrderedDict([
        (u'Übergeordnete Ordnungsposition - UID', 'repository'),
    ])

    def get_items(self):
        active_formatter = api.portal.get_registry_record(
            name='formatter', interface=IReferenceNumberSettings)
        formatter = queryAdapter(
            self.portal, IReferenceNumberFormatter, name=active_formatter)
        return sorted(
            api.content.find(
                self.portal, object_provides=IRepositoryFolder.__identifier__),
            key=formatter.sorter,
        )

    def row_for_item(self, brain):
        item = brain.getObject()
        return [
            item.get_repository_number(),
            unicode(item.UID()),
            self._parent_uid(item),
            item.title_de or u'',
            item.title_fr or u'',
            item.title_en or u'',
            item.description or u'',
            self._translate(item.classification),
            self._translate(item.privacy_layer),
            self._translate(item.public_trial),
            unicode(item.get_retention_period()),
            item.get_retention_period_annotation() or u'',
            self._translate(item.get_archival_value()),
            item.get_archival_value_annotation() or u'',
            unicode(item.get_custody_period()),
            self._format_date(item.valid_from),
            self._format_date(item.valid_until),
        ]

    def _parent_uid(self, folder):
        parent = aq_parent(aq_inner(folder))
        if not IRepositoryFolder.providedBy(parent):
            return u''
        return unicode(parent.UID())

    def _translate(self, value):
        if not value:
            return u''
        return translate(value, domain='opengever.base', target_language='de')

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))
