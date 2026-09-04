# -*- coding: utf-8 -*-
from collections import OrderedDict
from opengever.base.response import COMMENT_RESPONSE_TYPE
from opengever.base.response import IResponseContainer
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.scripts.customer_specific.export_kgkoeniz.exporters.base import BaseExporter
from opengever.meeting.proposal import IProposal
from opengever.task import OPEN_TASK_STATES
from opengever.task.task import ITask
from plone import api


def _uid(obj):
    if obj is None:
        return u''
    return unicode(obj.UID())


class CommentExporter(BaseExporter):
    """Exports comments (Kommentare) as comments.csv.

    Comments are `Response` objects (response_type 'comment') stored on
    the commented-on object itself, so they are collected by iterating
    Dossiers, (open) Aufgaben and Anträge, rather than queried directly.
    Deleted comments are removed from the response container by the
    delete action itself, so no filtering for that is needed here.
    """

    key = 'comments'
    label = u'Kommentare'
    filename = 'comments.csv'
    headers = [
        u'Datum',
        u'Text',
        u'Dossier - UID',
        u'Antrag - UID',
        u'Aufgabe - UID',
        u'Benutzer - UID',
    ]
    reference_columns = OrderedDict([
        (u'Dossier - UID', 'dossiers'),
        (u'Antrag - UID', 'proposals'),
        (u'Aufgabe - UID', 'tasks'),
        (u'Benutzer - UID', 'users'),
    ])

    def get_items(self):
        items = []
        items.extend(self._dossier_comments())
        items.extend(self._task_comments())
        items.extend(self._proposal_comments())
        return items

    def row_for_item(self, item):
        response, dossier_uid, antrag_uid, aufgabe_uid = item
        return [
            self._format_date(response.created),
            response.text or u'',
            dossier_uid,
            antrag_uid,
            aufgabe_uid,
            unicode(response.creator) if response.creator else u'',
        ]

    def _dossier_comments(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__, sort_on='UID')
        items = []
        for brain in brains:
            dossier = brain.getObject()
            for response in self._comment_responses(dossier):
                items.append((response, _uid(dossier), u'', u''))
        return items

    def _task_comments(self):
        # Restricted to open tasks, like TaskExporter - so every Aufgabe -
        # UID referenced here is also present in tasks.csv.
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=ITask.__identifier__,
            review_state=OPEN_TASK_STATES)
        items = []
        for brain in brains:
            task = brain.getObject()
            for response in self._comment_responses(task):
                items.append((response, u'', u'', _uid(task)))
        return items

    def _proposal_comments(self):
        # A Proposal (Antrag) and its SubmittedProposal (Eingereichter
        # Antrag) are separate physical objects, each with their own
        # comments - see ProposalExporter. Both are attributed to the
        # canonical "Traktandum UID" used there.
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IProposal.__identifier__)
        items = []
        for brain in brains:
            proposal = brain.getObject()
            model = proposal.load_model()
            assert model is not None, u'missing db-model for {}'.format(proposal)
            antrag_uid = self._proposal_uid(model)
            for obj in (model.resolve_proposal(), model.resolve_submitted_proposal()):
                if obj is None:
                    continue
                for response in self._comment_responses(obj):
                    items.append((response, u'', antrag_uid, u''))
        return items

    def _proposal_uid(self, model):
        uid = _uid(model.resolve_proposal())
        if uid:
            return uid
        return _uid(model.resolve_submitted_proposal())

    def _comment_responses(self, obj):
        # ResponseContainer.list() returns storage.values() off an LOBTree
        # keyed by response_id, so responses already come back in
        # chronological (response_id-ascending) order.
        return [response for response in IResponseContainer(obj).list()
                if response.response_type == COMMENT_RESPONSE_TYPE]

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))
