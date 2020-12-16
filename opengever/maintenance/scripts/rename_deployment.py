"""
Allows to rename various aspects of a deployment.

Example Usage:
    bin/instance run rename_deployment.py --new-deployment-title="DI AGG" --new-au-title="DI AGG" --new-ou-title="DI AGG" --new-au-abbr="DI AGG"
"""
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.dossiertemplate.behaviors import IDossierTemplateMarker
from opengever.globalindex.handlers.task import sync_task
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.task.task import ITask
from plone import api
import logging
import sys
import transaction


logger = logging.getLogger('opengever.maintenance')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def rename_deployment(plone, options):
    print '\n' * 4
    logger.info('Renaming deployment %r' % plone.id)

    reindexing_needed = False

    if options.dryrun:
        logger.info('(DRY-RUN)')

    if options.new_deployment_title:
        set_new_deployment_title(plone, options)

    if options.new_au_title:
        set_new_admin_unit_title(plone, options)

    if options.new_ou_title:
        set_new_org_unit_title(plone, options)

    if options.new_au_abbr:
        set_new_admin_unit_abbreviation(plone, options)
        reindexing_needed = True

    if reindexing_needed:
        reindex_objects(plone, options)


def set_new_deployment_title(plone, options):
    new_title = options.new_deployment_title.decode('utf-8')

    logger.info('Existing site title: %s' % plone.title)
    logger.info('Setting new site title: %s\n' % new_title)

    logger.info('Existing email_from_name: %s' % plone.email_from_name)
    logger.info('Setting new email_from_name: %s\n' % new_title)

    if not options.dryrun:
        plone.manage_changeProperties(
            {'title': new_title,
             'email_from_name': new_title})


def set_new_admin_unit_title(plone, options):
    new_au_title = options.new_au_title.decode('utf-8')

    admin_unit = get_current_admin_unit()

    logger.info('Existing AdminUnit title: %s' % admin_unit.title)
    logger.info('Setting AdminUnit title: %s\n' % new_au_title)

    if not options.dryrun:
        admin_unit.title = new_au_title


def set_new_org_unit_title(plone, options):
    new_ou_title = options.new_ou_title.decode('utf-8')

    admin_unit = get_current_admin_unit()

    if not len(admin_unit.org_units) == 1:
        logger.error(
            'Use of --new-ou-title is only supported for deployments with '
            'exactly one OrgUnit, aborting.')
        sys.exit(1)

    org_unit = admin_unit.org_units[0]

    logger.info('Existing OrgUnit title: %s' % org_unit.title)
    logger.info('Setting OrgUnit title: %s\n' % new_ou_title)

    if not options.dryrun:
        org_unit.title = new_ou_title


def set_new_admin_unit_abbreviation(plone, options):
    new_au_abbr = options.new_au_abbr.decode('utf-8')

    admin_unit = get_current_admin_unit()

    logger.info('Existing AdminUnit abbreviation: %s' % admin_unit.abbreviation)
    logger.info('Setting AdminUnit abbreviation: %s\n' % new_au_abbr)

    if not options.dryrun:
        admin_unit.abbreviation = new_au_abbr


def reindex_objects(plone, options):

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults()

    for brain in brains:
        obj = brain.getObject()
        attrs_to_index = []

        if options.new_au_abbr:
            # Reindex data derived from AdminUnit abbreviation:
            # - Catalog indexes / metadata
            # - Solr fields
            # - SQL (for tasks)

            # (Almost) everything has at least a reference number
            attrs_to_index.append('reference')

            # Update the 'reference' column in SQL
            if ITask.providedBy(obj):
                logger.info('Syncing task %r to SQL' % obj)
                if not options.dryrun:
                    sync_task(obj, None)

            if IDossierMarker.providedBy(obj) or IDossierTemplateMarker.providedBy(obj):
                # These have an IDynamicTextIndexExtender that includes the
                # reference number.
                #
                # Document has one too, but it won't be used if Solr is in play.
                # Documents have their searchable metadata in the Solr-only
                # 'metadata' field handled below.
                #
                # Tasks have an IDynamicTextIndexExtender too, but it doesn't
                # include the reference number.
                attrs_to_index.append('SearchableText')

            elif IBaseDocument:
                # 'metadata' is the Solr-only field that also gets queried in
                # full text searches. Only IBaseDocument has an indexer for it.
                attrs_to_index.append('metadata')

        if attrs_to_index:
            logger.info('Reindexing %r for %r' % (attrs_to_index, obj))
            if not options.dryrun:
                obj.reindexObject(idxs=attrs_to_index)


def parse_options():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)

    parser.add_option("--new-deployment-title")
    parser.add_option("--new-au-title")
    parser.add_option("--new-ou-title")
    parser.add_option("--new-au-abbr")
    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':
    app = setup_app()

    options, args = parse_options()

    if options.dryrun:
        transaction.doom()

    plone = setup_plone(app, options)

    rename_deployment(plone, options)

    if not options.dryrun:
        logger.info('Committing transaction...')
        transaction.commit()
        logger.info('Done.')
