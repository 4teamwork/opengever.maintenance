"""
Script to set `preserved_as_paper` to False for all `IDocumentMetadata`.

    bin/instance run set_preserved_as_paper_to_false.py

"""
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import logging
import transaction


logger = logging.getLogger("set_preserved_as_paper_to_false")
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def set_preserved_as_paper_to_false():
    catalog = api.portal.get_tool("portal_catalog")
    logger.info("Setting preserved_as_paper to False")
    query = {
        "object_provides": IDocumentMetadata.__identifier__,
    }
    brains = catalog.unrestrictedSearchResults(query)
    total = len(brains)

    for i, brain in enumerate(brains):
        obj = brain.getObject()
        meta = IDocumentMetadata(obj)
        meta.preserved_as_paper = False

        if i % 100 == 0:
            logger.info("Progress: %s of %s objects\n" % (i, total))

    logger.info("Done")


if __name__ == "__main__":
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        logger.info("DRY-RUN")

    set_preserved_as_paper_to_false()

    if not options.dry_run:
        transaction.commit()
