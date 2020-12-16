"""
Allows to rename various aspects of a deployment.

Example Usage:
    bin/instance run rename_deployment.py --new-deployment-title="DI AGG" --new-au-title="DI AGG" --new-ou-title="DI AGG"
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
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

    if options.dryrun:
        logger.info('(DRY-RUN)')

    if options.new_deployment_title:
        set_new_deployment_title(plone, options)

    if options.new_au_title:
        set_new_admin_unit_title(plone, options)

    if options.new_ou_title:
        set_new_org_unit_title(plone, options)


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


def parse_options():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)

    parser.add_option("--new-deployment-title")
    parser.add_option("--new-au-title")
    parser.add_option("--new-ou-title")
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
