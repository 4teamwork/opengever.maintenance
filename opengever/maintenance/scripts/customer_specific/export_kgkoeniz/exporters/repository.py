# -*- coding: utf-8 -*-
from Acquisition import aq_inner
from Acquisition import aq_parent
from collections import OrderedDict
from opengever.base.interfaces import IReferenceNumberFormatter
from opengever.base.interfaces import IReferenceNumberSettings
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from zope.component import queryAdapter


class RepositoryExporter(BaseExporter):
    """Exports the Ordnungssystem (repository) as repository.csv."""

    key = 'repository'
    label = u'Ordnungssystem'
    filename = 'repository.csv'
    id_column = u'Ordnungsposition UID'
    headers = [
        u'Ordnungspositionsnummer',
        u'Ordnungsposition UID',
        u'Pfad zum Objekt',
        u'Übergeordnete Ordnungsposition - UID',
        u'Titel der Ordnungsposition',
        u'Beschreibung (optional)',
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
        catalog = api.portal.get_tool('portal_catalog')
        return sorted(
            catalog.unrestrictedSearchResults(
                object_provides=IRepositoryFolder.__identifier__),
            key=formatter.sorter,
        )

    def row_for_item(self, brain):
        item = brain.getObject()
        return [
            item.get_repository_number(),
            unicode(item.UID()),
            self._physical_path(item),
            self._parent_uid(item),
            item.title_de or u'',
            item.description or u'',
            self._format_date(item.valid_from),
            self._format_date(item.valid_until),
        ]

    def _parent_uid(self, folder):
        parent = aq_parent(aq_inner(folder))
        if not IRepositoryFolder.providedBy(parent):
            return u''
        return unicode(parent.UID())

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))
