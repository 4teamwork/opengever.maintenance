#!/usr/bin/env python
"""
This script finds all active dossiers in a given repository folder (i.e.
those with review_state == 'dossier-state-active') and resolves (closes) them.
It uses basic logging and commits after processing each dossier.
"""

from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.interfaces import IDossierResolver
from zope.component import getAdapter
from plone import api
import traceback
from opengever.dossier.exceptions import PreconditionsViolated

from opengever.maintenance.debughelpers import setup_app, setup_option_parser, setup_plone

import logging
import sys
import transaction

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def resolve_active_dossiers(context, dry_run=False):
    catalog = api.portal.get_tool("portal_catalog")
    query = {
        "path": context.absolute_url_path(),
        "object_provides": IDossierMarker.__identifier__,
        "review_state": "dossier-state-active"
    }
    active_dossiers = catalog.unrestrictedSearchResults(**query)

    if not active_dossiers:
        logger.info("No active dossiers found.")
        return

    logger.info("Found %d active dossier(s).", len(active_dossiers))

    for brain in active_dossiers:
        dossier = brain.getObject()
        resolver = getAdapter(dossier, IDossierResolver, name="lenient")
        try:
            resolver.raise_on_failed_preconditions()
            resolver.resolve()
            dossier.reindexObject()
            logger.info("Resolved dossier: %s", dossier.absolute_url_path())

            if dry_run:
                logger.info("Dry run enabled. Changes for dossier not committed.")
                transaction.doom()
            else:
                transaction.commit()
                logger.info("Committed changes for dossier: %s", dossier.absolute_url_path())
        except Exception as e:
            logger.error("Error resolving dossier %s: %s", dossier.absolute_url_path(), e)
            logger.error(traceback.format_exc())
            continue


def main():
    logger.info("START: Script started, initializing Plone environment.")
    app = setup_app()
    setup_plone(app)

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dry_run", default=False,
                      help="Perform a dry run (do not commit changes)")
    (options, args) = parser.parse_args()

    if len(args) != 1:
        logger.error("ERROR: Missing path argument! Please provide the repository path as the first argument.")
        print("Bro, you need a path argument as first argument")
        sys.exit(1)

    repository_path = args[0]
    logger.info("Repository path provided: %s", repository_path)

    try:
        context = app.unrestrictedTraverse(repository_path)
    except Exception as e:
        logger.error("Cannot traverse to repository at '%s': %s", repository_path, e)
        sys.exit(1)

    logger.info("DONE: Starting resolution of active dossiers in repository: %s", repository_path)
    try:
        resolve_active_dossiers(context, dry_run=options.dry_run)
    except PreconditionsViolated as e:
        logger.error("ERROR:  Preconditions violated while resolving dossiers:")
        for error in e.errors:
            logger.error(error)
    logger.info("SUCCESS: Completed resolving active dossiers.")


if __name__ == '__main__':
    main()
