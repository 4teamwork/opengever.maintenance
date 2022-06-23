"""
This script is used to export sql contacts in to bundle and removes contact
references in manual journal entries.

bin/instance run ./scripts/sql_contact_migration.py

optional arguments:
  -p : skip the migration of dossier participations
  -t : skip the removal of contact references from manual journal entries.
  -n : dry-run.
"""

from ftw.journal.config import JOURNAL_ENTRIES_ANNOTATIONS_KEY
from ftw.journal.interfaces import IAnnotationsJournalizable
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.contact.models.org_role import OrgRole
from opengever.contact.models.organization import Organization
from opengever.contact.models.participation import ContactParticipation
from opengever.contact.models.participation import OgdsUserParticipation
from opengever.contact.models.participation import OrgRoleParticipation
from opengever.contact.models.person import Person
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.participations import KuBParticipationHandler
from opengever.dossier.participations import SQLParticipationHandler
from opengever.journal.entry import MANUAL_JOURNAL_ENTRY
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from persistent.list import PersistentList
from plone import api
from uuid import uuid4
from zope.annotation.interfaces import IAnnotations
import json
import logging
import os
import sys
import time
import transaction

logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


class SqlContactExporter(object):

    def __init__(self, bundle_directory):
        self.bundle_directory = bundle_directory
        self.contact_mapping = {}
        self.org_role_mapping = {}

    def run(self, skip_participations=False, skip_journal_cleanup=False):
        os.mkdir(self.bundle_directory)

        self.export()
        if not skip_participations:
            self.migrate_participations()

        if not skip_journal_cleanup:
            self.cleanup_journal_entries()

    def export(self):
        persons = list(self.get_persons())
        self.export_json('people.json', persons)

        organizations = list(self.get_organizations())
        self.export_json('organizations.json', organizations)

        org_roles = list(self.get_org_roles())
        self.export_json('memberships.json', org_roles)

    def migrate_participations(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__)
        brains = ProgressLogger('Migrate dossier participations.', brains)
        for brain in brains:
            dossier = brain.getObject()
            self.migrate_participation(dossier)

    def migrate_participation(self, dossier):
        sql_handler = SQLParticipationHandler(dossier)
        kub_handler = KuBParticipationHandler(dossier)
        participations = sql_handler.get_participations()

        for participation in participations:
            # To avoid reindexing the dosiser after each participation we
            # add the participation manually
            if isinstance(participation, OgdsUserParticipation):
                participant_id = participation.ogds_userid

            elif isinstance(participation, ContactParticipation):
                if participation.contact.contact_type == 'person':
                    participant_id = u'person:{}'.format(
                        self.contact_mapping[participation.contact_id])
                else:
                    participant_id = u'organization:{}'.format(
                        self.contact_mapping[participation.contact_id])

            elif isinstance(participation, OrgRoleParticipation):
                participant_id = u'membership:{}'.format(
                    self.org_role_mapping[participation.org_role_id])
            else:
                raise Exception(
                    u'Not supported participation type: {}'.format(
                        participation.participation_type))

            kub_participation = kub_handler.create_participation(
                participant_id=participant_id,
                roles=[role.role for role in participation.roles])
            kub_handler.append_participation(kub_participation)

        if participations:
            dossier.reindexObject(idxs=["participations", "UID"])

    def cleanup_journal_entries(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IAnnotationsJournalizable.__identifier__)
        brains = ProgressLogger(
            'Remove contact references from journal entries.', brains)
        for brain in brains:
            self.reset_contact_references(brain.getObject())

    def reset_contact_references(self, obj):
        annotations = IAnnotations(obj)
        entries = annotations.get(JOURNAL_ENTRIES_ANNOTATIONS_KEY, [])
        for entry in entries:
            if entry.get('action', {}).get('type') == MANUAL_JOURNAL_ENTRY:
                entry['action']['contacts'] = PersistentList()

    def get_persons(self):
        for person in Person.query:
            kub_uid = str(uuid4())
            self.contact_mapping[person.contact_id] = kub_uid

            yield {
                'id': kub_uid,
                'third_party_id': person.former_contact_id,
                'first_name': person.firstname,
                'last_name': person.lastname,
                'official_name': ' '.join((person.firstname, person.lastname)),
            }

    def get_organizations(self):
        for organization in Organization.query:
            kub_uid = str(uuid4())
            self.contact_mapping[organization.contact_id] = kub_uid
            yield {
                'id': kub_uid,
                'third_party_id': organization.former_contact_id,
                'name': organization.name}

    def get_org_roles(self):
        for org_role in OrgRole.query:
            kub_uid = str(uuid4())
            self.org_role_mapping[org_role.org_role_id] = kub_uid
            yield {
                'id': kub_uid,
                'third_party_id': org_role.org_role_id,
                'person': self.contact_mapping[org_role.person_id],
                'organization': self.contact_mapping[org_role.organization_id],
                'role': org_role.function}

    def export_json(self, filename, items):
        with open('/'.join((self.bundle_directory, filename)), 'w') as outfile:
            json.dump(items, outfile, indent=4)


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    parser.add_option("-p", "--skip-participations",
                      action="store_true",
                      dest="skip_participations", default=False)
    parser.add_option("-j", "--skip-journal-cleanup",
                      action="store_true",
                      dest="skip_journal", default=False)
    (options, args) = parser.parse_args()

    if options.dryrun:
        logger.info("Dry run, dooming transaction")
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    bundle_directory = u'var/kub-bundle-{}'.format(time.strftime('%d%m%Y-%H%M%S'))
    exporter = SqlContactExporter(bundle_directory)
    exporter.run(skip_participations=options.skip_participations,
                 skip_journal_cleanup=options.skip_journal)

    if not options.dryrun:
        transaction.commit()
        logger.info("Transaction committed")

if __name__ == '__main__':
    main()
