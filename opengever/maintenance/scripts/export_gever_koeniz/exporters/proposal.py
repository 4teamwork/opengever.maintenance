# -*- coding: utf-8 -*-
from collections import OrderedDict
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
from opengever.meeting.model import SubmittedDocument
from opengever.meeting.proposal import IProposal
from plone import api
from plone.locking.interfaces import ILockable
from zope.i18n import translate


def _uid(obj):
    if obj is None:
        return u''
    return unicode(obj.UID())


class _AntragFields(object):
    """Field extraction for a proposal that has not (or no longer, i.e.
    cancelled) been submitted to a committee (Antrag).
    """

    def __init__(self, model):
        self.model = model
        self.proposal = model.resolve_proposal()

    @property
    def title(self):
        return self.model.title or u''

    @property
    def description(self):
        return self.model.description or u''

    def dossier_uid(self):
        if self.proposal is None:
            return u''
        return _uid(self.proposal.get_containing_dossier())

    def proposal_document_uid(self):
        if self.proposal is None:
            return u''
        return _uid(self.proposal.get_proposal_document())

    def attachment_uids(self):
        """Returns (beilagen_uids, entkoppelte_beilagen_uids)."""
        if self.proposal is None:
            return [], []
        beilagen, entkoppelt = [], []
        for document in self.proposal.get_documents():
            if document is None:
                continue
            submitted_document = SubmittedDocument.query.get_by_source(
                self.proposal, document)
            if not submitted_document or ILockable(
                    submitted_document.resolve_submitted()).locked():
                beilagen.append(_uid(document))
            else:
                entkoppelt.append(_uid(document))
        return beilagen, entkoppelt


class _EingereichterAntragFields(object):
    """Field extraction for a proposal that has been submitted to a
    committee (Eingereichter Antrag; workflow states: submitted,
    scheduled, decided).
    """

    def __init__(self, model):
        self.model = model
        self.submitted_proposal = model.resolve_submitted_proposal()

    @property
    def title(self):
        return self.model.submitted_title or self.model.title or u''

    @property
    def description(self):
        return (self.model.submitted_description
                or self.model.description or u'')

    def dossier_uid(self):
        if self.submitted_proposal is None:
            return u''
        return _uid(self.submitted_proposal.get_containing_dossier())

    def proposal_document_uid(self):
        if self.submitted_proposal is None:
            return u''
        return _uid(self.submitted_proposal.get_proposal_document())

    def attachment_uids(self):
        if self.submitted_proposal is None:
            return [], []
        beilagen, entkoppelt = [], []
        for document in self.submitted_proposal.get_documents():
            if ILockable(document).locked():
                beilagen.append(_uid(document))
            else:
                entkoppelt.append(_uid(document))
        return beilagen, entkoppelt


class ProposalExporter(BaseExporter):
    """Exports Anträge and Eingereichte Anträge as proposals.csv.

    Each row is one `Proposal` SQL model row, which may back an Antrag,
    an Eingereichter Antrag, or - once submitted - both at once as two
    separate physical Zope objects; see `_register_known_ids()`.
    """

    key = 'proposals'
    label = u'Anträge'
    filename = 'proposals.csv'
    id_column = u'Traktandum UID'
    headers = [
        u'Traktandum UID',
        u'Traktandum Nr.',
        u'Beschlussnummer',
        u'Titel',
        u'Beschreibung',
        u'Dossier - UID',
        u'Sitzung - ID',
        u'Auftraggeber - UID',
        u'Antragsdokument - UID',
        u'Status',
        u'Beilagen - UID',
        u'Entkoppelte Beilagen - UID',
        u'Protokollauszug - UID',
    ]
    reference_columns = OrderedDict([
        (u'Dossier - UID', 'dossiers'),
        (u'Sitzung - ID', 'meetings'),
        (u'Auftraggeber - UID', 'users'),
        (u'Antragsdokument - UID', 'documents'),
        (u'Beilagen - UID', 'documents'),
        (u'Entkoppelte Beilagen - UID', 'documents'),
        (u'Protokollauszug - UID', 'documents'),
    ])

    def get_items(self):
        # Every Eingereichter Antrag originates from an Antrag, which is
        # never deleted - so enumerating Anträge also covers every
        # Eingereichter Antrag, without querying the meeting database
        # directly (which is not scoped to this Plone site).
        brains = api.content.find(
            self.portal, object_provides=IProposal.__identifier__)
        return sorted(brains, key=lambda brain: brain.getPath())

    def row_for_item(self, brain):
        item = self._load_model(brain)
        self._register_known_ids(item)
        fields = self._fields_for(item)
        beilagen, entkoppelt = fields.attachment_uids()
        return [
            self._proposal_uid(item),
            self._item_number(item),
            self._decision_number(item),
            fields.title,
            fields.description,
            fields.dossier_uid(),
            self._meeting_id(item),
            unicode(item.issuer) if item.issuer else u'',
            fields.proposal_document_uid(),
            self._translate_state(item),
            u'|'.join(beilagen),
            u'|'.join(entkoppelt),
            self._excerpt_uids(item),
        ]

    def _load_model(self, brain):
        proposal = brain.getObject()
        model = proposal.load_model()
        assert model is not None, u'missing db-model for {}'.format(proposal)
        return model

    def _register_known_ids(self, item):
        # DocumentExporter can attribute a document to either the Antrag
        # or the Eingereichter Antrag, so both UIDs must be known even
        # though only the Antrag's UID is shown as "Traktandum UID".
        for obj in (item.resolve_proposal(), item.resolve_submitted_proposal()):
            uid = _uid(obj)
            if uid:
                self.exported_ids.add(uid)

    def _is_submitted(self, item):
        # submitted_int_id is cleared by reject(), which also deletes the
        # SubmittedProposal - so this reflects "currently submitted",
        # independent of whether the object is resolvable right now.
        return item.submitted_int_id is not None

    def _fields_for(self, item):
        if self._is_submitted(item):
            return _EingereichterAntragFields(item)
        return _AntragFields(item)

    def _proposal_uid(self, item):
        uid = _uid(item.resolve_proposal())
        if uid:
            return uid
        return _uid(item.resolve_submitted_proposal())

    def _item_number(self, item):
        if item.agenda_item and item.agenda_item.item_number:
            return unicode(item.agenda_item.item_number)
        return u''

    def _decision_number(self, item):
        number = item.get_decision_number()
        return unicode(number) if number else u''

    def _meeting_id(self, item):
        meeting = item.get_meeting()
        return unicode(meeting.meeting_id) if meeting is not None else u''

    def _translate_state(self, item):
        return translate(
            item.get_state().title, domain='opengever.meeting',
            target_language='de')

    def _excerpt_uids(self, item):
        uids = []
        for document in (item.resolve_excerpt_document(),
                          item.resolve_submitted_excerpt_document()):
            uid = _uid(document)
            if uid and uid not in uids:
                uids.append(uid)
        return u'|'.join(uids)
