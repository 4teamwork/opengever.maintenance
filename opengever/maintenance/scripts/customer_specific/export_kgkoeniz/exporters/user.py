# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.ogds.models.service import ogds_service


class UserExporter(BaseExporter):
    """Exports users (Benutzer) as users.csv."""

    key = 'users'
    label = u'Benutzer'
    filename = 'users.csv'
    id_column = u'Benutzer UID'
    headers = [
        u'Benutzer UID',
        u'Status',
        u'Name',
        u'Vorname',
        u'E-Mail',
    ]

    def get_items(self):
        return sorted(ogds_service().all_users(), key=lambda user: user.userid)

    def row_for_item(self, item):
        return [
            unicode(item.userid),
            u'Aktiv' if item.active else u'Inaktiv',
            item.lastname or u'',
            item.firstname or u'',
            item.email or u'',
        ]
