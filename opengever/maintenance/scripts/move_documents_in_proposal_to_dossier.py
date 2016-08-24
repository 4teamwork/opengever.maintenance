from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import elevated_privileges
from plone import api
import logging
import sys
import transaction


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)

SEPARATOR = '-' * 78


def move_documents_in_proposal_to_dossier(options):
    """Move documents placed in a proposal to their parent dossier.

    """

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.meeting.proposal')

    with elevated_privileges():
        for brain in brains:
            proposal = brain.getObject()
            document_brains = catalog.unrestrictedSearchResults(
                path=brain.getPath(),
                portal_type='opengever.document.document')
            for document_brain in document_brains:
                document = document_brain.getObject()
                dossier = proposal.get_containing_dossier()
                logger.info("moving document {} to dossier {}".format(
                    document_brain.getPath(),
                    "/".join(dossier.getPhysicalPath())))
                api.content.move(source=document, target=dossier)

    if not options.dry_run:
        logger.info("committing...")
        transaction.commit()
    logger.info("done.")


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    logger.info(SEPARATOR)
    logger.info("Date: {}".format(datetime.now().isoformat()))
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        logger.info("DRY-RUN")

    move_documents_in_proposal_to_dossier(options)


if __name__ == '__main__':
    main()
