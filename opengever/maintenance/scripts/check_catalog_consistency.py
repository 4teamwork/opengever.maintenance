"""
This script attempts to discover inconsistencies in the internal data
structures of the catalog.
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api


SEPARATOR = '-' * 78


class CatalogConsistencyChecker(object):

    def __init__(self):
        self.catalog = api.portal.get_tool('portal_catalog')

    def check(self):
        brains = self.catalog.unrestrictedSearchResults()

        for brain in brains:
            # Trigger RID key errors
            self._get_index_data_for_brain(brain)

            # Trigger key errors in _wordinfo mapping
            self._check_wordinfo_consistency(brain)

        print "All checks done."

    def _get_index_data_for_brain(self, brain):
        """Based on Products.ZCatalog.Catalog.Catalog.getIndexDataForRID

        Logs exceptions (most likely KeyErrors) when fetching a record by RID
        from a particular index fails.

        This check attempts to discover inconsistencies between the Plone
        Lexicon and ZCTextIndexes.

        It should detect cases where a ZCTextIndex references a word (by an
        integer word ID (wid)) that doesn't exist in the lexicon (any more).
        It does this by trying to fetch all indexed data, for all indexes,
        for the given brain.

        In the case of ZCTextIndexes, this will cause `getEntryForObject` to
        look up a list of word IDs in the lexicon, and trigger a KeyError if
        that word isn't in the lexicon.

        It seems that the only reliable way to fix this situation is to first
        CLEAR all the affected indexes, and then rebuild them.
        """
        rid = brain.getRID()
        _catalog = self.catalog._catalog
        result = {}

        for name in _catalog.indexes.keys():
            idx = _catalog.getIndex(name)
            try:
                result[name] = idx.getEntryForObject(rid, "")
            except Exception, e:
                msg = "Index {}: Fetching RID {} failed with {!r}. Path: {}"
                print msg.format(name, rid, e, brain.getPath())
        return result

    def _check_wordinfo_consistency(self, brain):
        """During a massive rename operation, we saw KeyErrors during
        unIndexing of objects from the attempt to delete doc2score[rid].

        This check tries to force that kind of KeyError, by accessing
        idx.index._wordinfo[wid][rid] for each indexed word in each index
        with a lexicon (usually ZCTextIndexes).

        The error seemed to be of a transient nature, so it's unclear
        whether this is actually a persistent problem - but if it is, this
        check should uncover it.
        """
        rid = brain.getRID()
        _catalog = self.catalog._catalog
        entry = self.catalog.getIndexDataForRID(rid)

        for idx_name, words in entry.items():
            idx = _catalog.getIndex(idx_name)

            try:
                lexicon = idx.getLexicon()
            except AttributeError:
                # No getLexicon() method - probably not a ZCTextIndex
                continue

            for word in words:
                wid = lexicon._wids[word]
                doc2score = idx.index._wordinfo[wid]
                try:
                    # Access score to potentially trigger a KeyError
                    doc2score[rid]
                except Exception, e:
                    msg = "Index {}: Fetching score for word {!r}, " \
                          "RID {} failed with {!r}. Path: {}"
                    print msg.format(idx_name, word, rid, e, brain.getPath())


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    setup_plone(app, options)

    CatalogConsistencyChecker().check()


if __name__ == '__main__':
    main()
