from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from StringIO import StringIO
from zope.component import queryMultiAdapter
import argparse
import logging
import sys


logger = logging.getLogger('solr_maintenance')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['reindex', 'sync', 'diff', 'clear'],
                        help='solr-maintenance mode')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()

    portal = setup_plone(app, options)
    portal.REQUEST.response.stdout = StringIO()

    logger.info('Start solr maintenance mode `{}`'.format(options.mode))
    solr_maintenance = queryMultiAdapter(
        (portal, portal.REQUEST), name=u'solr-maintenance')
    getattr(solr_maintenance, options.mode)()

    if options.mode in ['reindex', 'sync']:
        solr_maintenance.optimize()


if __name__ == '__main__':
    main()
