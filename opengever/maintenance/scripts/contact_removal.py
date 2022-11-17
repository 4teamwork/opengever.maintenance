"""
This script is used to remove contacts and related dossier participations.

bin/instance run ./scripts/contact_removal.py

optional arguments:
  -n : dry-run.
  -s : site root (used if multiple plone sites exists).
"""

from ftw.upgrade.progresslogger import ProgressLogger
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.participations import PloneParticipationHandler
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import argparse
import logging
import sys
import transaction

logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


class ContactRemover(object):

    def run(self):
        self.remove_contacts()
        self.remove_contact_participations()

    def remove_contacts(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            portal_type='opengever.contact.contact')
        contacts = [brain.getObject() for brain in brains]

        logger.info("Found {} contacts to remove".format(len(contacts)))
        api.content.delete(objects=contacts)

    def remove_contact_participations(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__)
        for brain in ProgressLogger(
                'Remove dossier contact participations.', brains):
            handler = PloneParticipationHandler(brain.getObject())
            participations = handler.get_participations()
            for participation in participations:
                participant_id = participation.contact
                if participant_id.startswith(u'contact:'):
                    handler.remove_participation(participant_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument("-n", "--dry-run", action="store_true",
                        dest="dryrun", default=False)

    options = parser.parse_args(sys.argv[3:])

    if options.dryrun:
        logger.info("Dry run, dooming transaction")
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    remover = ContactRemover()
    remover.run()

    logger.info(
        "All contacts and related participations successfully removed.")

    if not options.dryrun:
        transaction.commit()
        logger.info("Transaction committed")


if __name__ == '__main__':
    main()
