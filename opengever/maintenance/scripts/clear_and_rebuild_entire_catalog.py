"""
Script to clear and rebuild catalog.

    bin/instance run clear_and_rebuild_entire_catalog.py
"""


from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFPlone.utils import base_hasattr
from Products.CMFPlone.utils import safe_callable
import transaction


class Rebuilder(object):

    def __init__(self, plone, options):
        self.plone = plone
        self.catalog = api.portal.get_tool('portal_catalog')

    def run(self):
        print "Clear and rebuild..."
        self._register_after_commit_hook()
        self.clear_find_and_rebuild()
        print "Done."

    def clear_find_and_rebuild(self):
        """Based on CatalogTool.clearFindAndRebuild(), but with some rough
        progress logging.
        """
        # Empties catalog, then finds all contentish objects (i.e. objects
        # with an indexObject method), and reindexes them.
        # This may take a long time.

        stats = {
            'num_indexed': 0,
            'num_total': len(self.catalog._catalog.uids)
        }

        def index_object(obj, path, stats=stats):
            if stats['num_indexed'] % 10 == 0:
                percent = stats['num_indexed'] / float(stats['num_total']) * 100
                print "Progress (estimate): %.2f%% (%s / %s)" % (
                    percent, stats['num_indexed'], stats['num_total'])

            print "  Reindexing %s" % path
            __traceback_info__ = path
            if (base_hasattr(obj, 'indexObject') and
                    safe_callable(obj.indexObject)):
                try:
                    obj.indexObject()
                    stats['num_indexed'] += 1
                except TypeError:
                    # Catalogs have 'indexObject' as well, but they
                    # take different args, and will fail
                    pass
        self.catalog.manage_catalogClear()
        self.plone.ZopeFindAndApply(self.plone, search_sub=True,
                                    apply_func=index_object)

    def _register_after_commit_hook(self):

        def notification_hook(success, *args, **kwargs):
            result = success and 'committed' or 'aborted'
            print 'Transaction has been %s.' % result

        txn = transaction.get()
        txn.addAfterCommitHook(notification_hook)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        print "Dry-run"
        transaction.doom()

    rebuilder = Rebuilder(plone, options)
    rebuilder.run()

    if not options.dryrun:
        transaction.commit()
