"""
This script is used to export sql contacts in to bundle and removes contact
references in manual journal entries.

bin/instance run ./scripts/sql_contact_migration.py
"""

from opengever.contact.models.org_role import OrgRole
from opengever.contact.models.organization import Organization
from opengever.contact.models.person import Person
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from uuid import uuid4
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

    def run(self):
        os.mkdir(self.bundle_directory)

        self.export()

    def export(self):
        persons = list(self.get_persons())
        self.export_json('people.json', persons)

        organizations = list(self.get_organizations())
        self.export_json('organizations.json', organizations)

        org_roles = list(self.get_org_roles())
        self.export_json('memberships.json', org_roles)

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
                'organization': self.contact_mapping[org_role.organization_id]}

    def export_json(self, filename, items):
        with open('/'.join((self.bundle_directory, filename)), 'w') as outfile:
            json.dump(items, outfile, indent=4)


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if options.dryrun:
        logger.info("Dry run, dooming transaction")
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    bundle_directory = u'var/kub-bundle-{}'.format(time.strftime('%d%m%Y-%H%M%S'))
    exporter = SqlContactExporter(bundle_directory)
    exporter.run()

    if not options.dryrun:
        transaction.commit()
        logger.info("Transaction committed")

if __name__ == '__main__':
    main()
