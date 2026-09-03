# -*- coding: utf-8 -*-
from collections import OrderedDict
from opengever.contact.service import CONTACT_TYPE
from opengever.dossier.behaviors.participation import IParticipationAware
from opengever.dossier.behaviors.participation import IParticipationAwareMarker
from opengever.maintenance.scripts.export_gever_koeniz.exporters.base import BaseExporter
from opengever.ogds.base.actor import ActorLookup
from plone import api


# German labels as defined in participation_roles.vdex. The vdex vocabulary
# resolves langstrings via ambient language negotiation, which isn't
# reliable when running as a script without a request, so roles are mapped
# explicitly here instead.
ROLE_LABELS = {
    'participation': u'Mitwirkung',
    'final-drawing': u'Schlusszeichnung',
    'regard': u'Kenntnisnahme',
}


class ParticipationExporter(BaseExporter):
    """Exports participations (Beteiligungen) as participations.csv."""

    key = 'participations'
    label = u'Beteiligungen'
    filename = 'participations.csv'
    headers = [
        u'Dossier - UID',
        u'Benutzer - UID',
        u'Kontakt - UID',
        u'Rollen',
    ]
    reference_columns = OrderedDict([
        (u'Dossier - UID', 'dossiers'),
        (u'Benutzer - UID', 'users'),
        (u'Kontakt - UID', 'contacts'),
    ])

    def get_items(self):
        brains = api.content.find(
            self.portal, object_provides=IParticipationAwareMarker.__identifier__)
        items = []
        for brain in sorted(brains, key=lambda brain: brain.UID):
            dossier = brain.getObject()
            participations = IParticipationAware(dossier).get_participations()
            for participation in sorted(participations, key=lambda p: p.contact):
                items.append((dossier, participation))
        return items

    def row_for_item(self, item):
        dossier, participation = item
        user_uid, contact_uid = self._resolve_participant(participation.contact)
        return [
            unicode(dossier.UID()),
            user_uid,
            contact_uid,
            self._role_labels(participation.roles),
        ]

    def _resolve_participant(self, participant_id):
        if ActorLookup(participant_id).is_contact():
            return u'', self._contact_uid(participant_id)
        return unicode(participant_id), u''

    def _contact_uid(self, participant_id):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            portal_type=CONTACT_TYPE, contactid=participant_id)
        if not brains:
            return u''
        return unicode(brains[0].UID)

    def _role_labels(self, roles):
        return u'|'.join(ROLE_LABELS.get(role, role) for role in roles)
