# -*- coding: utf-8 -*-
from collections import OrderedDict
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.meeting.model.meeting import Meeting
from pytz import timezone
from zope.i18n import translate


class MeetingExporter(BaseExporter):
    """Exports meetings (Sitzungen) as meetings.csv."""

    key = 'meetings'
    label = u'Sitzungen'
    filename = 'meetings.csv'
    id_column = u'Sitzung-ID'
    headers = [
        u'Sitzung-ID',
        u'Gremium',
        u'Sitzungstitel',
        u'Status',
        u'Beginn',
        u'Ende',
        u'Vorsitz - ID',
        u'Protokollführung - UID',
        u'Ort',
        u'Sitzungsdossier - UID',
        u'Protokoll - UID',
        u'Traktandenliste - UID',
        u'Teilnehmende - ID',
    ]
    reference_columns = OrderedDict([
        (u'Vorsitz - ID', 'members'),
        (u'Protokollführung - UID', 'users'),
        (u'Sitzungsdossier - UID', 'dossiers'),
        (u'Protokoll - UID', 'documents'),
        (u'Traktandenliste - UID', 'documents'),
        (u'Teilnehmende - ID', 'members'),
    ])

    def get_items(self):
        return Meeting.query.order_by(Meeting.start).all()

    def row_for_item(self, item):
        return [
            unicode(item.meeting_id),
            item.committee.title or u'',
            item.title or u'',
            self._translate_state(item),
            self._format_datetime(item.start),
            self._format_datetime(item.end),
            unicode(item.presidency.member_id) if item.presidency else u'',
            unicode(item.secretary.userid) if item.secretary else u'',
            item.location or u'',
            unicode(item.get_dossier().UID()),
            self._document_uid(item.protocol_document),
            self._document_uid(item.agendaitem_list_document),
            self._participant_ids(item),
        ]

    def _translate_state(self, item):
        return translate(
            item.get_state().title, domain='opengever.meeting', target_language='de')

    def _document_uid(self, generated_document):
        if generated_document is None:
            return u''
        document = generated_document.resolve_document()
        if document is None:
            return u''
        return unicode(document.UID())

    def _participant_ids(self, item):
        return u'|'.join(unicode(member.member_id) for member in item.participants)

    def _format_datetime(self, value):
        if not value:
            return u''
        local = value.astimezone(timezone('Europe/Zurich'))
        return unicode(local.strftime('%d.%m.%Y %H:%M'))
