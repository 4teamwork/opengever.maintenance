# -*- coding: utf-8 -*-
from opengever.dossier.behaviors.dossier import IDossier
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.utils import get_containing_repository_folder
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
from opengever.maintenance.scripts.export_gever_koeniz.exporters.keyword import generate_keyword_uid
from plone import api
from zope.i18n import translate


class DossierExporter(BaseExporter):
    """Exports dossiers as dossiers.csv."""

    key = 'dossiers'
    label = u'Dossiers'
    filename = 'dossiers.csv'
    headers = [
        u'Dossier UID',
        u'Dossier ID',
        u'Status',
        u'Titel',
        u'Aktenzeichen Nr.',
        u'Ordnungssystem - Ordnungsposition UID',
        u'Übergeordnete Dossier UID',
        u'Beschreibung',
        u'Federführend - Benutzer UID',
        u'Beginn',
        u'Ende',
        u'Schlagwörter UID',
        u'Verwandte Dossiers - Dossier UID',
    ]

    def get_items(self):
        brains = api.content.find(
            self.portal, object_provides=IDossierMarker.__identifier__)
        dossiers = [brain.getObject() for brain in brains]
        return sorted(dossiers, key=lambda d: d.get_sequence_number())

    def row_for_item(self, item):
        return [
            unicode(item.UID()),
            unicode(item.get_sequence_number()),
            self._translate_state(item),
            item.title or u'',
            item.get_reference_number() or u'',
            self._repository_folder_uid(item),
            self._parent_dossier_uid(item),
            item.description or u'',
            IDossier(item).responsible or u'',
            self._format_date(IDossier(item).start),
            self._format_date(IDossier(item).end),
            self._keyword_uids(item),
            self._related_dossier_uids(item),
        ]

    def _translate_state(self, item):
        state = api.content.get_state(item)
        if not state:
            return u''
        return translate(state, domain='plone', target_language='de')

    def _repository_folder_uid(self, item):
        folder = get_containing_repository_folder(item)
        if folder is None:
            return u''
        return unicode(folder.UID())

    def _parent_dossier_uid(self, item):
        parent = item.get_parent_dossier()
        if parent is None:
            return u''
        return unicode(parent.UID())

    def _keyword_uids(self, item):
        keywords = IDossier(item).keywords
        return u'|'.join(generate_keyword_uid(keyword) for keyword in keywords)

    def _related_dossier_uids(self, item):
        related = [rel.to_object for rel in IDossier(item).relatedDossier
                   if rel.to_object]
        return u'|'.join(unicode(obj.UID()) for obj in related)

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))
