# -*- coding: utf-8 -*-
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.meeting.model.member import Member


class MemberExporter(BaseExporter):
    """Exports committee members (Sitzungsmitglieder) as members.csv."""

    key = 'members'
    label = u'Sitzungsmitglieder'
    filename = 'members.csv'
    id_column = u'Sitzungsmitglied-ID'
    headers = [
        u'Sitzungsmitglied-ID',
        u'Vorname',
        u'Nachname',
        u'E-Mail',
    ]

    def get_items(self):
        return Member.query.order_by(Member.lastname, Member.firstname).all()

    def row_for_item(self, item):
        return [
            unicode(item.member_id),
            item.firstname or u'',
            item.lastname or u'',
            item.email or u'',
        ]
