"""
Script to fix broken task-template responsible.

    bin/instance run fix_task_template_responsibles.py

Fixes: https://4teamwork.atlassian.net/browse/CA-1997

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import logging
import sys
import transaction


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


def fix_task_template_responsibles(catalog):
    broken_brains = catalog(responsible="interactive_actor:None")
    logger.info('fixing {} broken objects'.format(len(broken_brains)))

    for brain in broken_brains:
        logger.info('fixing {}'.format(brain.getURL()))
        obj = brain.getObject()
        obj.responsible = None
        obj.reindexObject(idxs=['responsible'])

    logger.info('fixed {}'.format(brain.getURL()))


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    options, args = parser.parse_args()

    if options.dry_run:
        logger.warn('transaction doomed because we are in dry-mode.')
        transaction.doom()

    setup_plone(app, options)
    catalog = api.portal.get_tool('portal_catalog')
    fix_task_template_responsibles(catalog)
    if options.dry_run:
        logger.warn('skipping commit because we are in dry-mode.')
    else:
        transaction.commit()
        logger.info('done.')


if __name__ == '__main__':
    main()
