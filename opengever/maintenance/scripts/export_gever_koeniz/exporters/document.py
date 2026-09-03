# -*- coding: utf-8 -*-
from Acquisition import aq_inner
from Acquisition import aq_parent
from collections import OrderedDict
from ftw.upgrade import ProgressLogger
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import ISequenceNumber
from opengever.base.solr.fields import translate_document_type
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
from opengever.meeting.committeecontainer import ICommitteeContainer
from opengever.meeting.proposal import IBaseProposal
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task import CLOSED_TASK_STATES
from opengever.task.task import ITask
from plone import api
from zope.component import getUtility
import os


class DocumentExporter(BaseExporter):
    """Exports documents as documents.csv, and optionally their blob files
    into documents/<UID>/<Filename>.<ext>.
    """

    key = 'documents'
    label = u'Dokumente'
    filename = 'documents.csv'
    id_column = u'Dokument UID'
    headers = [
        u'Dokument UID',
        u'Pfad zum Objekt',
        u'Dokument-ID',
        u'Übergeordnetes Dossier - UID',
        u'Übergeordnete Aufgabe - UID',
        u'Übergeordneter Antrag - UID',
        u'Titel',
        u'Dokumentennummer',
        u'Datei',
        u'Dateipfad',
        u'Beschreibung',
        u'Dokumentdatum',
        u'Eingangsdatum',
        u'Ausgangsdatum',
        u'Dokumenttyp',
        u'Autor',
        u'In Papierform aufbewahrt',
    ]
    reference_columns = OrderedDict([
        (u'Übergeordnetes Dossier - UID', 'dossiers'),
        (u'Übergeordnete Aufgabe - UID', 'tasks'),
        (u'Übergeordneter Antrag - UID', 'proposals'),
    ])

    def get_items(self):
        brains = []
        for root in self._get_search_roots():
            brains.extend(api.content.find(
                root, object_provides=IBaseDocument.__identifier__))
        return sorted(brains, key=lambda brain: brain.getPath())

    def _get_search_roots(self):
        """Documents live either under the repository root (Ordnungssystem)
        or, once submitted as a proposal attachment, physically parented
        under a SubmittedProposal in a committee container (Sitzungen) -
        a separate top-level tree, not a descendant of the repository root.
        """
        roots = [self._get_repository_root()]
        committee_container_brains = api.content.find(
            self.portal, object_provides=ICommitteeContainer.__identifier__)
        roots.extend(brain.getObject() for brain in committee_container_brains)
        return roots

    def _get_repository_root(self):
        brains = api.content.find(
            self.portal, object_provides=IRepositoryRoot.__identifier__)
        if len(brains) != 1:
            raise ValueError(
                u'Expected exactly one repository root, found {}'.format(
                    len(brains)))
        return brains[0].getObject()

    def row_for_item(self, brain):
        item = brain.getObject()
        parent = aq_parent(aq_inner(item))
        if self._parent_task_is_closed(parent):
            return None
        dossier_uid, task_uid, proposal_uid = self._parent_uids(item, parent)
        return [
            unicode(item.UID()),
            self._physical_path(item),
            unicode(self._sequence_number(item)),
            dossier_uid,
            task_uid,
            proposal_uid,
            item.title or u'',
            self._reference_number(item),
            item.get_filename() or u'',
            self._blob_path(item),
            item.description or u'',
            self._format_date(item.document_date),
            self._format_date(item.receipt_date),
            self._format_date(item.delivery_date),
            translate_document_type(item.document_type) if item.document_type else u'',
            item.document_author or u'',
            u'Ja' if item.preserved_as_paper else u'Nein',
        ]

    def _sequence_number(self, item):
        return getUtility(ISequenceNumber).get_number(item)

    def _reference_number(self, item):
        return IReferenceNumber(item).get_number() or u''

    def _blob_path(self, item):
        """Path to the item's exported blob file, relative to the export
        directory. Matches the layout created by `export_blobs`.
        """
        if not item.has_file():
            return u''
        return u'/'.join((u'documents', item.UID(), item.get_filename()))

    def _parent_task_is_closed(self, parent):
        """Documents whose immediate parent is a task in a closed state
        (e.g. cancelled or tested-and-closed) are excluded from the export,
        analogous to TaskExporter only exporting open tasks.
        """
        if not ITask.providedBy(parent):
            return False
        return api.content.get_state(parent) in CLOSED_TASK_STATES

    def _parent_uids(self, item, parent):
        """Return (dossier_uid, task_uid, proposal_uid), with exactly one of
        them populated depending on the type of the document's immediate
        parent. Raises if the parent is of an unexpected type.
        """
        if IDossierMarker.providedBy(parent):
            return unicode(parent.UID()), u'', u''
        if ITask.providedBy(parent):
            return u'', unicode(parent.UID()), u''
        if IBaseProposal.providedBy(parent):
            return u'', u'', unicode(parent.UID())
        raise ValueError(
            u'Document {} has an unknown parent type: {}'.format(
                item.UID(), getattr(parent, 'portal_type', repr(parent))))

    def _format_date(self, value):
        if not value:
            return u''
        return unicode(value.strftime('%d.%m.%Y'))

    def export_blobs(self, export_dir):
        """Create the documents/ folder that holds one subfolder per
        document UID, containing the document's blob file with its
        original filename.
        """
        blobs_dir = os.path.join(export_dir, 'documents')
        os.makedirs(blobs_dir)
        for brain in ProgressLogger(u'Dokumente (Dateien)', self.get_items()):
            item = brain.getObject()
            if self._parent_task_is_closed(aq_parent(aq_inner(item))):
                continue
            blob_path = self._blob_path(item)
            if not blob_path:
                continue
            file_path = os.path.join(export_dir, *blob_path.split(u'/'))
            os.makedirs(os.path.dirname(file_path))
            with open(file_path, 'wb') as blob_file:
                blob_file.write(item.get_file().data)
