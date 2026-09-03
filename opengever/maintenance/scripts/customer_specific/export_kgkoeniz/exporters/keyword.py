# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from plone import api
import hashlib


def generate_keyword_uid(title):
    """Deterministic UID for a keyword, derived from its title."""
    return unicode(hashlib.md5(title.encode('utf-8')).hexdigest())


class KeywordExporter(BaseExporter):
    """Exports keywords (Schlagwörter) as keywords.csv."""

    key = 'keywords'
    label = u'Schlagwörter'
    filename = 'keywords.csv'
    id_column = u'Schlagwort UID'
    headers = [
        u'Schlagwort UID',
        u'Schlagwort Bezeichnung',
    ]

    def get_items(self):
        catalog = api.portal.get_tool('portal_catalog')
        keywords = [kw for kw in catalog.uniqueValuesFor('Subject') if kw]
        return sorted(keywords)

    def row_for_item(self, item):
        return [
            generate_keyword_uid(item),
            item,
        ]
