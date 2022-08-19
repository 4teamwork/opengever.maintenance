"""
This script is used to prepare deletion of the meetings from
Gever and use RIS in its stead.
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import logging
import transaction

logger = logging.getLogger('opengever.maintenance')
logging.root.setLevel(logging.INFO)


class MeetingsContentMigrator(object):

    def __init__(self):
        self.catalog = api.portal.get_tool("portal_catalog")


def main():
    app = setup_app()
    setup_plone(app)

    parser = setup_option_parser()
    parser.add_option("-n", dest="dryrun", action="store_true", default=False)
    (options, args) = parser.parse_args()

    if options.dryrun:
        logger.info('Performing dryrun!\n')
        transaction.doom()

    migrator = MeetingsContentMigrator()

    if not options.dryrun:
        transaction.commit()


if __name__ == '__main__':
    main()
