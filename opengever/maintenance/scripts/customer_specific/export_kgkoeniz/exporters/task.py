# -*- coding: utf-8 -*-
from Acquisition import aq_inner
from Acquisition import aq_parent
from collections import OrderedDict
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.utils import get_containing_dossier
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.task import OPEN_TASK_STATES
from opengever.task.task import ITask
from plone import api
from Products.CMFPlone.utils import safe_unicode
from zope.i18n import translate


class TaskExporter(BaseExporter):
    """Exports (open) tasks as tasks.csv."""

    key = 'tasks'
    label = u'Aufgaben'
    filename = 'tasks.csv'
    id_column = u'Aufgabe UID'
    headers = [
        u'Aufgabe UID',
        u'Pfad zum Objekt',
        u'Aufgabe-ID',
        u'Dossier - UID',
        u'Übergeordnete Aufgabe - UID',
        u'Status',
        u'Titel',
        u'Beschreibung',
        u'Zu Erledigen bis',
        u'Erinnerung',
        u'Auftragnehmer - UID',
        u'Auftraggeber - UID',
        u'Auftragstyp',
        u'Persönliche Aufgabe',
        u'Informierte Beteiligte - UID',
        u'Dokumente - UID',
    ]
    reference_columns = OrderedDict([
        (u'Dossier - UID', 'dossiers'),
        (u'Übergeordnete Aufgabe - UID', 'tasks'),
        (u'Auftragnehmer - UID', 'users'),
        (u'Auftraggeber - UID', 'users'),
        (u'Informierte Beteiligte - UID', 'users'),
        (u'Dokumente - UID', 'documents'),
    ])

    def get_items(self):
        catalog = api.portal.get_tool('portal_catalog')
        return catalog.unrestrictedSearchResults(
            object_provides=ITask.__identifier__,
            review_state=OPEN_TASK_STATES)

    def row_for_item(self, brain):
        item = brain.getObject()
        return [
            unicode(item.UID()),
            self._physical_path(item),
            unicode(brain.id),
            self._dossier_uid(item),
            self._parent_task_uid(item),
            self._translate_state(item),
            item.title or u'',
            self._description(item),
            self._format_date(item.deadline),
            self._reminder_date(item),
            unicode(item.responsible) if item.responsible else u'',
            unicode(item.issuer) if item.issuer else u'',
            item.get_task_type_label(language=u'de') or u'',
            u'Ja' if item.is_private else u'Nein',
            self._informed_principal_ids(item),
            self._document_uids(item),
        ]

    def _dossier_uid(self, item):
        dossier = get_containing_dossier(item)
        if dossier is None or not IDossierMarker.providedBy(dossier):
            return u''
        return unicode(dossier.UID())

    def _parent_task_uid(self, item):
        parent = aq_parent(aq_inner(item))
        if not ITask.providedBy(parent):
            return u''
        return unicode(parent.UID())

    def _translate_state(self, item):
        state = api.content.get_state(item)
        if not state:
            return u''
        return translate(state, domain='plone', target_language='de')

    def _description(self, item):
        if not item.text:
            return u''
        plain = api.portal.get_tool(name='portal_transforms').convertTo(
            'text/plain', item.text.output, mimetype='text/html').getData()
        return u' '.join(safe_unicode(plain).split())

    def _reminder_date(self, item):
        """The reminder date shown is the one calculated for whichever
        potential responsible has the lowest userid - reminders are set
        per-representative, so there is no single canonical reminder for
        a task with multiple (e.g. team) representatives.
        """
        reminders = item.get_reminders_of_potential_responsibles()
        if not reminders:
            return u''
        user_id = sorted(reminders.keys())[0]
        reminder = reminders[user_id]
        return self._format_date(reminder.calculate_trigger_date(item.deadline))

    def _informed_principal_ids(self, item):
        return u'|'.join(unicode(principal) for principal in item.informed_principals)

    def _document_uids(self, item):
        related = [rel.to_object for rel in item.relatedItems if rel.to_object]
        return u'|'.join(unicode(obj.UID()) for obj in related)

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))
