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


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    setup_plone(app, options)

    CatalogConsistencyChecker().check()


if __name__ == '__main__':
    main()
