from ftw.solr.browser.maintenance import checkpoint_iterator
from ftw.solr.browser.maintenance import timer
from ftw.solr.interfaces import ISolrConnectionManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import queryMultiAdapter
from zope.component import queryUtility
import argparse
import logging
import sys



logger = logging.getLogger('solr')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['remove', 'reindex'],
                        help='solr-maintenance mode')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()
    portal = setup_plone(app, options)

    if options.mode == 'remove':
        remove_filesize(portal)

    elif options.mode == 'reindex':
        reindex(portal)


def remove_filesize(portal):
    sm = queryUtility(ISolrConnectionManager)

    # Copied intermediate commit handling from solr maintenance view
    zodb_conn = portal._p_jar
    processed = 0
    lap = timer()

    def commit():
        conn = sm.connection
        conn.commit(after_commit=False)
        zodb_conn.cacheGC()
        logger.info(
            'Intermediate commit (%d items processed, last batch in %s)',
            processed, lap.next())

    res = sm.connection.search(
        {'query':'*:*',
         'limit': 1000000,
         'params': {'fl':['UID']}})

    cpi = checkpoint_iterator(commit, interval=1000)

    for solr_doc in res.docs:
        sm.connection.add({"UID": solr_doc['UID'], "filesize": {"set": None}})
        processed += 1
        cpi.next()

    commit()


def reindex(portal):
    portal = api.portal.get()
    solr_maintenance = queryMultiAdapter(
        (portal, portal.REQUEST), name=u'solr-maintenance')

    solr_maintenance.reindex(idxs=['filesize', 'changed'], doom=False)


if __name__ == '__main__':
    main()
