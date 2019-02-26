"""
Usage:

    bin/instance run find_catalog_inconsistencies.py
"""

from collections import defaultdict
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import TextTable
from plone import api
import transaction


class CatalogInconsistencyFinder(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.catalog = api.portal.get_tool('portal_catalog')

    def find_duplicate_uids_from_brains(self):
        all_brains = self.catalog.unrestrictedSearchResults()
        print "Checking all brains for duplicate UIDs..."
        total = len(all_brains)
        duplicate_uids = set()
        rids_and_paths_by_uid = defaultdict(list)

        for i, brain in enumerate(all_brains):
            uid = brain.UID

            if i % 10000 == 0:
                print "  %.1f%% (%s/%s)" % (float(i) / total * 100, i, total)

            if uid in rids_and_paths_by_uid:
                # If we encountered this UID more than once, flag it
                duplicate_uids.add(uid)

            meta = {'rid': brain.getRID(), 'path': brain.getPath()}
            rids_and_paths_by_uid[uid].append(meta)

        duplicate_uids_with_metadata = {}
        for dup_uid in duplicate_uids:
            metas = rids_and_paths_by_uid[dup_uid]
            duplicate_uids_with_metadata[dup_uid] = metas[:]

        return duplicate_uids_with_metadata

    def run(self):
        print "Looking for catalog inconsistencies..."

        duplicate_uids_with_metadata = self.find_duplicate_uids_from_brains()
        print "Done checking for duplicate UIDs.\n"

        print "Duplicate UIDs from brains: %r" % len(duplicate_uids_with_metadata)
        print "(UIDs related to more than one path or RID)"

        print
        dup_uids_table = TextTable()
        dup_uids_table.add_row(("UID", "RID", "path"))
        for uid, metas in duplicate_uids_with_metadata.items():
            for meta in metas:
                dup_uids_table.add_row((uid, meta['rid'], meta['path']))
        print dup_uids_table.generate_output()
        print

        """UID index structure:
        _index:   UID -> RID
        _unindex: RID -> UID
        """

        uid_index = self.catalog._catalog.indexes['UID']
        self.diff_inverted_indexes(
            index_title='UID Index',
            index=uid_index._index,
            unindex=uid_index._unindex,
            fw_key_label='UID',
            fw_value_label='RID')

        """Mapping structure:
        _catalog.uids:  uid (path) -> RID
        _catalog.paths: RID -> uid (path)
        """

        self.diff_inverted_indexes(
            index_title='uids/paths mapping',
            index=self.catalog._catalog.uids,
            unindex=self.catalog._catalog.paths,
            fw_key_label='UID',
            fw_value_label='RID')

    def diff_inverted_indexes(self, index_title, index, unindex, fw_key_label, fw_value_label):
        print "=" * 80
        print index_title
        print "=" * 80
        print

        index_length = len(index)
        unindex_length = len(unindex)
        # assert len(keys) == len(values) == len(idx)

        length_difference = abs(index_length - unindex_length)

        print "%s index length: %s" % (index_title, index_length)
        print "%s unindex length: %s" % (index_title, unindex_length)
        print "%s length difference: %s" % (index_title, length_difference)

        if length_difference:

            extra_keys_in_index = set(index.keys()) - set(unindex.values())
            print "Keys (%ss) in index, but not in values of unindex: %r" % (fw_key_label, len(extra_keys_in_index))
            table = TextTable()
            table.add_row((fw_key_label, fw_value_label))
            for fw_key in extra_keys_in_index:
                fw_value = index.get(fw_key)
                table.add_row((fw_key, fw_value))
            print
            print table.generate_output()
            print

            extra_values_in_index = set(index.values()) - set(unindex.keys())
            print "Values (%ss) in index, but not in keys of unindex: %r" % (fw_value_label, len(extra_values_in_index))
            table = TextTable()
            table.add_row((fw_value_label, fw_key_label))
            for fw_value in extra_values_in_index:
                fw_key = unindex.get(fw_value)
                table.add_row((fw_value, fw_key))
            print
            print table.generate_output()
            print

            extra_keys_in_unindex = set(unindex.keys()) - set(index.values())
            print "Keys (%ss) in unindex, but not in values of index: %r" % (fw_value_label, len(extra_keys_in_unindex))
            table = TextTable()
            table.add_row((fw_value_label, fw_key_label))
            for fw_value in extra_keys_in_unindex:
                fw_key = unindex.get(fw_value)
                table.add_row((fw_value, fw_key))
            print
            print table.generate_output()
            print

            extra_values_in_unindex = set(unindex.values()) - set(index.keys())
            print "Values (%ss) in unindex, but not in keys of index: %r" % (fw_key_label, len(extra_values_in_unindex))
            table = TextTable()
            table.add_row((fw_key_label, fw_value_label))
            for fw_key in extra_values_in_unindex:
                fw_value = index.get(fw_key)
                table.add_row((fw_key, fw_value))
            print
            print table.generate_output()
            print

        print


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    finder = CatalogInconsistencyFinder(plone, options)
    finder.run()
