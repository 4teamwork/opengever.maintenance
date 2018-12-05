from opengever.base.interfaces import ISearchSettings
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone.registry.interfaces import IRegistry
from Products.CMFCore.utils import getToolByName
from StringIO import StringIO
from zope.component import getUtility
from zope.component import queryMultiAdapter

import argparse
import logging
import sys
import transaction


INDEXES_TO_REMOVE = [
    'SearchableText',
    'Description',
    'document_author',
]

logger = logging.getLogger('activate_solr')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('--keep-indexes', dest='keep_indexes', default=False,
                        action="store_true", help='Keep catalog indexes')
    parser.add_argument('--no-indexing', dest='no_indexing', default=False,
                        action="store_true", help='Do not reindex in Solr')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()

    portal = setup_plone(app, options)
    portal.REQUEST.response.stdout = StringIO()
    if not options.no_indexing:
        sync(portal)

    # Abort current transaction after Solr reindexing because we didn't modify
    # any ZODB data and we don't want to retry the transaction because of a
    # read conflict.
    transaction.abort()
    transaction.begin()

    enable_solr()
    if not options.keep_indexes:
        remove_catalog_indexes(portal)

    trx = transaction.get()
    trx.note('Activate Solr')
    trx.commit()


def sync(portal):
    logger.info('Indexing Solr using sync mode')
    solr_maintenance = queryMultiAdapter(
        (portal, portal.REQUEST), name=u'solr-maintenance')
    solr_maintenance.sync()
    solr_maintenance.optimize()


def enable_solr():
    registry = getUtility(IRegistry)
    settings = registry.forInterface(ISearchSettings)
    settings.use_solr = True
    logger.info('Solr enabled')


def remove_catalog_indexes(portal):
    catalog = getToolByName(portal, 'portal_catalog')
    indexes = catalog.indexes()
    for index in INDEXES_TO_REMOVE:
        if index in indexes:
            catalog.delIndex(index)
            logger.info('Removed catalog index %s.', index)

    catalog.clearIndex('Title')
    lexicon = catalog['plone_lexicon']
    lexicon.clear()
    logger.info('plone_lexicon cleared. Reindexing Title...')
    title_index = catalog._catalog.getIndex('Title')
    items = catalog.unrestrictedSearchResults()
    for item in items:
        title_index.index_object(item.getRID(), item)


if __name__ == '__main__':
    main()
