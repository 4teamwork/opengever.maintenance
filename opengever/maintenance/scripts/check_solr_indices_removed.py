from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
import argparse
import logging
import sys


INDEXES_TO_REMOVE = [
    'Description',
    'document_author',
    'SearchableText',
]

logger = logging.getLogger('check_solr_indexes')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()
    portal = setup_plone(app, options)

    catalog = getToolByName(portal, 'portal_catalog')
    indexes = catalog.indexes()
    unexpectedly_found = []
    for index_name in INDEXES_TO_REMOVE:
        if index_name in indexes:
            unexpectedly_found.append(index_name)

    if unexpectedly_found:
        logger.info('Unexpectedly found indexes: {}.'.format(
            ', '.join(unexpectedly_found))
        )
    else:
        logger.info('Indexes removed correctly.')


if __name__ == '__main__':
    main()
