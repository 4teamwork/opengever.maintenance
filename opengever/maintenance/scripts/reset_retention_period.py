from opengever.base.behaviors.lifecycle import ILifeCycleMarker
from opengever.base.behaviors.lifecycle import ILifeCycle
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


class RetentionPeriodResetter(object):
    """Resets the retention periods
    """

    def __init__(self, plone, options):
        self.plone = plone
        self.options = options
        self.portal_setup = api.portal.get_tool('portal_setup')
        self.catalog = api.portal.get_tool('portal_catalog')

    def run(self):
        for item in self.catalog.unrestrictedSearchResults(
                object_provides=[ILifeCycleMarker.__identifier__]):
            obj = item.getObject()

            ILifeCycle(obj).retention_period = 0


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    options, args = parser.parse_args()

    if options.dry_run:
        logger.warn('transaction doomed because we are in dry-mode.')
        transaction.doom()

    plone = setup_plone(app, options)
    RetentionPeriodResetter(plone, options).run()
    if options.dry_run:
        logger.warn('skipping commit because we are in dry-mode.')
    else:
        transaction.commit()
        logger.info('done.')


if __name__ == '__main__':
    main()
