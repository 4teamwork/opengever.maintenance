"""
Reindexes 'path_depth' in Solr for all objects that are missing it.

    bin/instance run reindex_path_depth_in_solr.py

"""
from ftw.solr.interfaces import ISolrConnectionManager
from ftw.solr.interfaces import ISolrIndexHandler
from ftw.solr.interfaces import ISolrSearch
from opengever.base.solr import OGSolrContentListingObject
from opengever.base.solr import OGSolrDocument
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from zope.component import getMultiAdapter
from zope.component import getUtility

# Turns out batching here is not implemented optimally, and may skip
# objects. Just set BATCH_SIZE to include all results in one batch, or
# execute the script repeatedly to make sure all objects have been fixed.

BATCH_SIZE = 1000
HARD_LIMIT = 1000000


def reindex_path_depth(plone):
    solr = getUtility(ISolrSearch)
    manager = getUtility(ISolrConnectionManager)

    i = 0
    start = 0

    while True and i < HARD_LIMIT:
        query = dict(
            query=u'-path_depth:[1 TO 999]',
            rows=BATCH_SIZE,
            start=start
        )
        results = solr.search(**query)

        total = results.num_found
        for solr_doc in results.docs:
            clobj = OGSolrContentListingObject(OGSolrDocument(solr_doc))
            obj = clobj.getObject()

            if obj is None:
                continue

            print "Reindexing path_depth for %r" % obj
            handler = getMultiAdapter((obj, manager), ISolrIndexHandler)
            handler.add(['path_depth'])
            i += 1

        print "Intermediate commit (%s/%s)" % (i, total)
        manager.connection.commit(soft_commit=False, extract_after_commit=False)

        if len(results.docs) == 0:
            break

        start += BATCH_SIZE

    manager.connection.commit(soft_commit=False, extract_after_commit=False)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    reindex_path_depth(plone)
