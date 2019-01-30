"""
This script uncatalogs orphaned paths from the ZCatalog.
(Paths that refer to objects that don't exist at that path any more).

Usage:

    bin/instance run uncatalog_orphaned_paths.py -n
"""

from collections import defaultdict
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


class OrphanedPathUncataloger(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.catalog = api.portal.get_tool('portal_catalog')

        self.seen_uids = set()
        self.duplicate_uids = set()
        self.paths_by_uid = defaultdict(list)

    def run(self):
        print "Collecting all brains..."
        all_brains = self.catalog.unrestrictedSearchResults()
        print "Done collecting brains.\n"

        print "Checking brains for duplicate UIDs..."
        total = len(all_brains)
        for i, brain in enumerate(all_brains):
            uid = brain.UID

            if i % 10000 == 0:
                print "%.1f%% (%s/%s)" % (float(i) / total * 100, i, total)

            # Collect all paths for this UID.
            # Exactly one for intact objects, two (or more) for orphaned paths
            self.paths_by_uid[uid].append(brain.getPath())

            if uid in self.seen_uids:
                # If we encountered this UID more than once, flag it
                self.duplicate_uids.add(uid)

            self.seen_uids.add(uid)

        print "Done checking for duplicate UIDs."
        print "Found %s duplicate UIDs:" % len(self.duplicate_uids)
        for dup_uid in self.duplicate_uids:
            print dup_uid
        print

        print "Building list of questionable paths..."
        questionable_paths = []
        for dup_uid in self.duplicate_uids:
            # Build list of paths that were encounteded where more than one
            # path was pointing to the same UID. These are the ones we need
            # to check. One is the canonical, correct path for the object,
            # the other ones will result in KeyErrors and need to be uncataloged
            questionable_paths.extend(self.paths_by_uid[dup_uid])
        print "Done.\n"

        print "Checking questionable paths..."
        paths_to_uncatalog = []
        objs_to_reindex = []

        for path in questionable_paths:
            try:
                obj = self.portal.unrestrictedTraverse(path)
                # Canonical object - reindex it to be safe
                print "  Obj should be reindexed: %r" % obj
                objs_to_reindex.append(obj)
            except KeyError:
                # Orphaned brain, uncatalog this path
                print "  Path should be uncataloged: %s" % path
                paths_to_uncatalog.append(path)

        print "Done. Found %s paths to uncatalog (%s to reindex)\n" % (
            len(paths_to_uncatalog), len(objs_to_reindex))

        if not self.options.dryrun:
            print "Reindexing objects..."
            for obj in objs_to_reindex:
                obj.reindexObject()

            print "Uncataloging orphaned paths..."
            for path_to_uncatalog in paths_to_uncatalog:
                print "Uncataloging %s" % path_to_uncatalog
                self.catalog.uncatalog_object(path_to_uncatalog)
            print

            print "All done."
        else:
            print "DRY-RUN, nothing done."


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    uncataloger = OrphanedPathUncataloger(plone, options)
    uncataloger.run()

    if not options.dryrun:
        transaction.commit()
        print "Transaction committed."
