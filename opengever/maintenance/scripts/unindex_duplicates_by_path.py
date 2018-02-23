from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import sys
import transaction


USAGE = """
Usage: bin/instance run unindex_duplicates_by_path.py <path> [-n]
"""


def unindex_duplicates(plone, path):
    catalog = api.portal.get_tool('portal_catalog')

    obj = plone.restrictedTraverse(path)
    if not obj:
        print "Could not find object at {}".format(path)
        sys.exit(1)

    if path not in catalog._catalog.uids:
        print "No rid in catalog uids for {}".format(path)
        sys.exit(1)

    correct_rid = catalog._catalog.uids[path]
    all_rids = [rid for rid, uid in catalog._catalog.paths.items()
                if uid == path]

    if (len(all_rids) == 1) and (all_rids[0] == correct_rid):
        print "All seems well. done."
        return

    pre_uids_length = len(catalog._catalog.uids)
    pre_paths_length = len(catalog._catalog.paths)
    pre_btree_length = catalog._catalog._length.value
    print "Entries in catalog uids: {}".format(pre_uids_length)
    print "Entries in catalog paths: {}".format(pre_paths_length)
    print "Catalog btree length {}".format(pre_btree_length)
    print ""

    invalid_rids = [each for each in all_rids if each != correct_rid]
    if len(invalid_rids) != 1:
        # we could loop, of course, but currently we have only observed
        # one invalid entry. so better have a close look again should
        # we ever have multiple invalid entries.
        print "Can only handle one incorrect rid, got {}".format(invalid_rids)
        sys.exit(1)

    invalid_rid = invalid_rids[0]
    print "Uncataloging invalid rid {}".format(invalid_rid)

    indexes = catalog._catalog.indexes.keys()
    for name in indexes:
        x = catalog._catalog.getIndex(name)
        if hasattr(x, 'unindex_object'):
            print "Processing index {}".format(name)
            x.unindex_object(invalid_rid)
    print "Removed rid from all indexes"

    if invalid_rid in catalog._catalog.data:
        del catalog._catalog.data[invalid_rid]
    if invalid_rid in catalog._catalog.paths:
        del catalog._catalog.paths[invalid_rid]

    post_uids_length = len(catalog._catalog.uids)
    post_paths_length = len(catalog._catalog.paths)
    print ""
    print "Entries in catalog uids: {}".format(post_uids_length)
    print "Entries in catalog paths: {}".format(post_paths_length)

    if post_uids_length != pre_uids_length:
        print "Something went wrong, should not have changed uids"
        sys.exit(1)

    if post_paths_length != (pre_paths_length - 1):
        print "Something went wrong, should removed one entry from paths"
        sys.exit(1)

    if post_uids_length != post_paths_length:
        print "Something went wrong, catalog path and rid mapping inconsistent"
        sys.exit(1)

    correct_btree_lenth = post_uids_length  # uids and paths length is equal
    if pre_btree_length != correct_btree_lenth:
        # btree length needs fixing
        if (pre_btree_length - 1) != correct_btree_lenth:
            print "Something went wrong, expected btree to shrink by one"
            sys.exit(1)

        catalog._catalog._length.change(-1)

    print "Catalog btree length {}".format(catalog._catalog._length.value)


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print USAGE
        print "Error: expecting exactly one path"
        sys.exit(1)

    if options.dryrun:
        transaction.doom()

    path = args[0]
    plone = setup_plone(app, options)
    unindex_duplicates(plone, path)

    if not options.dryrun:
        transaction.commit()
    print "Done"


if __name__ == '__main__':
    main()
