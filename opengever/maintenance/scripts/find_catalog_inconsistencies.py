"""
Usage:

    bin/instance run find_catalog_inconsistencies.py
"""

from collections import defaultdict
from collective.indexing.queue import processQueue
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import TextTable
from plone import api
from plone.app.folder.nogopip import GopipIndex
from Products.ExtendedPathIndex.ExtendedPathIndex import ExtendedPathIndex
from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex
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
        claimed_length = uid_index._length
        self.diff_inverted_indexes(
            index_title='UID Index',
            index=uid_index._index,
            unindex=uid_index._unindex,
            fw_key_label='UID',
            fw_value_label='RID',
            claimed_length=claimed_length)

        """Mapping structure:
        _catalog.uids:  uid (path) -> RID
        _catalog.paths: RID -> uid (path)
        """

        claimed_length = self.catalog._catalog._length
        diff = self.diff_inverted_indexes(
            index_title='uids/paths mapping',
            index=self.catalog._catalog.uids,
            unindex=self.catalog._catalog.paths,
            fw_key_label='UID',
            fw_value_label='RID',
            claimed_length=claimed_length)

        extra_uids_in_uids = diff.get('extra_keys_in_index', set())
        extra_rids_in_uids = diff.get('extra_values_in_index', set())
        extra_rids_in_paths = diff.get('extra_keys_in_unindex', set())
        extra_uids_in_paths = diff.get('extra_values_in_unindex', set())

        for rid in extra_rids_in_paths:
            potential_path = self.catalog._catalog.paths[rid]
            self.force_uncatalog_object_via_rid(rid)
            # TODO: Keep track of any and all paths we encounter that are
            # involved in something that needed fixing. Build a list of
            # "sketchy paths", and at the very end, iterate over all of them,
            # try to fetch the object (except KeyError), and if present,
            # reindex that object.

        # TODO: Fix the other 3 issues in a similar fashion

    def force_uncatalog_object_via_rid(self, rid):
        _catalog = self.catalog._catalog
        data = _catalog.data
        paths = _catalog.paths
        indexes = _catalog.indexes.keys()

        for name in indexes:
            idx = _catalog.getIndex(name)
            if not hasattr(idx, 'unindex_object'):
                raise Exception('WTF')

            if hasattr(idx, 'unindex_object'):

                if isinstance(idx, GopipIndex):
                    # Not a real index
                    continue

                print "Unindexing %r from %r (%s)" % (rid, idx, name)

                if isinstance(idx, (ZCTextIndex, DateRangeIndex, BooleanIndex)):
                    # These are more complex index types, that we don't handle
                    # on a low level. We have to hope .unindex_object is able
                    # to uncatalog the object and doesn't raise a KeyError.
                    idx.unindex_object(rid)
                    continue

                if not isinstance(idx, (DateIndex, FieldIndex, KeywordIndex, ExtendedPathIndex, UUIDIndex)):
                    raise Exception('Unhandled index type: %r' % idx)

                if rid in idx._index.values():
                    # Not quite sure yet if this actually *can* happen
                    entries_pointing_to_rid = [key for key in idx._index.keys() if idx._index[key] == rid]
                    assert len(entries_pointing_to_rid) == 1
                    entry = entries_pointing_to_rid[0]
                    del idx._index[entry]

                if rid in idx._unindex:
                    del idx._unindex[rid]

                # This should eventually converge to
                # len(_index) == len(_unindex) == _length.value
                actual_min_length = min(len(idx._index), len(idx._unindex))
                length_delta = idx._length.value - actual_min_length
                idx._length.change(length_delta)

        if rid in data:
            del data[rid]

        if rid in paths:
            del paths[rid]

        _catalog._length.change(-1)

    def catalog_object(self, potential_path):
        try:
            obj = self.portal.unrestrictedTraverse(potential_path)
        except KeyError:
            print "No obj found at path: %r" % potential_path
            return
        obj.reindexObject()
        processQueue()

    def diff_inverted_indexes(self, index_title, index, unindex, fw_key_label, fw_value_label, claimed_length=None):
        """Checks two indexes (BTrees) against each other that are supposed to
        be inverted copies of each other, one being the forward index and the
        other the inverted index.

        In other words: The keys of the forward index are supposed to be
        values in the inverted index, and vice versa.

        index_title:    The title used for labelling this index in output
        index:          The forward index
        unindex:        The inverted index
        fw_key_label:   The label for keys in the forward index. This will in
                        turn also be the label for values in the inverted index
        fw_value_label: The label for values in the forward index. This will
                        in turn also be the label for keys in the inverted index.
        """
        diff = {}
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
        print "claimed length (according to BTrees.Length obj): %r" % claimed_length.value

        if length_difference:

            extra_keys_in_index = set(index.keys()) - set(unindex.values())
            diff['extra_keys_in_index'] = extra_keys_in_index

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
            diff['extra_values_in_index'] = extra_values_in_index

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
            diff['extra_keys_in_unindex'] = extra_keys_in_unindex

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
            diff['extra_values_in_unindex'] = extra_values_in_unindex

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
        return diff

    def assert_clean(self):
        _catalog = self.catalog._catalog
        indexes = _catalog.indexes.keys()

        for name in indexes:
            idx = _catalog.getIndex(name)

            if isinstance(idx, GopipIndex):
                # Not a real index
                continue

            print "Checking %r (%s)" % (idx, name)

            # Note: Not all index types are symmetric in the sense that
            # their forward and reverse index must have the same length.
            # Nothing we can do for these.

            if isinstance(idx, (ZCTextIndex, DateRangeIndex, BooleanIndex)):
                # These are more complex index types, that we don't handle
                # on a low level. Can't check these.
                continue

            if isinstance(idx, (DateIndex, )):
                # Checking <DateIndex at /ai/portal_catalog/changed> (changed)
                # Index length: 133541
                # UnIndex length: 377619
                # Claimed length: 133541
                continue

            if name == 'total_comments':
                # Checking <FieldIndex at /ai/portal_catalog/total_comments> (total_comments)
                # Index length: 1
                # UnIndex length: 377619
                # Claimed length: 1
                continue

            if isinstance(idx, (DateIndex, FieldIndex, KeywordIndex, ExtendedPathIndex, UUIDIndex)):
                print "Index length: %s" % len(idx._index)
                print "UnIndex length: %s" % len(idx._unindex)
                print "Claimed length: %s" % idx._length.value
                assert len(idx._index) == len(idx._unindex) == idx._length.value
                continue

            raise Exception('Unknown index type: %r' % idx)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    finder = CatalogInconsistencyFinder(plone, options)
    finder.run()

    print
    print "Second run to verify"
    print '\n'

    finder = CatalogInconsistencyFinder(plone, options)
    finder.run()

    finder.assert_clean()
