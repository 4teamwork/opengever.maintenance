"""
Reindexes 'is_subtask' in Solr for all objects that are missing it.

    bin/instance run reindex_is_subtask_in_solr.py

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


BATCH_SIZE = 1000
HARD_LIMIT = 1000000


def reindex_path_depth(plone):
    solr = getUtility(ISolrSearch)
    manager = getUtility(ISolrConnectionManager)

    i = 0

    while True and i < HARD_LIMIT:
        query = dict(
            query=u'object_provides:opengever.task.task.ITask -is_subtask:[0 TO 1]',
            rows=BATCH_SIZE,
        )

        results = solr.search(**query)
        if len(results.docs) == 0:
            break

        remaining = results.num_found
        for solr_doc in results.docs:
            clobj = OGSolrContentListingObject(OGSolrDocument(solr_doc))
            obj = clobj.getObject()

            if obj is None:
                continue

            print "Reindexing is_subtask for %r" % obj
            handler = getMultiAdapter((obj, manager), ISolrIndexHandler)
            handler.add(['is_subtask'])
            i += 1

        print "Intermediate commit (done %s, remaining %s)" % (i, remaining)
        manager.connection.commit(soft_commit=False, after_commit=False)

    manager.connection.commit(soft_commit=False, after_commit=False)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    reindex_path_depth(plone)
