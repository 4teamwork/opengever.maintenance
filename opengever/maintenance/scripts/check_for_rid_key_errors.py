"""
This script attempts to discover inconsistencies between the Plone Lexicon
and ZCTextIndexes.

It should detect cases where a ZCTextIndex references a word (by an integer
word ID (wid)) that doesn't exist in the lexicon (any more). It does this
by trying to fetch all indexed data, for all indexes, for all brains.

In the case of ZCTextIndexes, this will cause `getEntryForObject` to look up
a list of word IDs in the lexicon, and trigger a KeyError if that word isn't
in the lexicon.

It seems that the only reliable way to fix this situation is to first CLEAR
all the affected indexes, and then rebuild them.
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api


SEPARATOR = '-' * 78


def getIndexDataForRID(portal_catalog, rid, brain):
    """Based on Products.ZCatalog.Catalog.Catalog.getIndexDataForRID

    Logs exceptions (most likely KeyErrors) when fetching a record by RID
    from a particular index fails.
    """
    _catalog = portal_catalog._catalog
    result = {}
    for name in _catalog.indexes.keys():
        try:
            result[name] = _catalog.getIndex(name).getEntryForObject(rid, "")
        except Exception, e:
            msg = "Index {}: Fetching RID {} failed with {!r}. Path: {}"
            print msg.format(name, rid, e, brain.getPath())
    return result


def check_for_rid_key_errors(portal, options):
    """Check for RID KeyErrors in catalog
    """
    portal_catalog = api.portal.get_tool('portal_catalog')
    brains = portal_catalog.unrestrictedSearchResults()

    for brain in brains:
        rid = brain.getRID()
        getIndexDataForRID(portal_catalog, rid, brain)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    check_for_rid_key_errors(plone, options)


if __name__ == '__main__':
    main()
