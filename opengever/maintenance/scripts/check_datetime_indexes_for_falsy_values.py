"""
A bin/instance run script which reports catalog datetime indexes
which contain falsy values.

A datetime index must not contain falsy values, otherwise the affected
brains will be removed from a result set when ordered by this index.

bin/instance0 run check_datetime_indexes_for_falsy_values.py


Background:
Sorting by a DateTime index containing falsy values is problematic:
the index has not enough infos to decide whether the falsy entries should
be ordered on top or on bottom of the list.
The index simply removes the falsy values.

ftw.table's catalog source contains a workaround which prevents the catalog
from sorting by date indexes in general and does this by hand afterwards.
This is a bad solution.

The correct solution is to extend the indexers of date indexes so that they
always return a datetime. The indexer must decide whether this is a very-early
or very-late datetime object.

With this script we prove that our indexes are correct, so that we can fix this
by removing ftw.table workaround in GEVER.
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex


def check_datetime_indexes(plone):
    catalog = plone.portal_catalog
    for index in catalog._catalog.indexes.values():
        if not isinstance(index, DateIndex):
            continue

        check_index(index)


def check_index(index):
    falsy = filter(lambda value: not value, index.uniqueValues())
    if falsy:
        print 'Index {!r} has falsy values {!r}'.format(index, falsy)


def main():
    plone = setup_plone(setup_app())
    check_datetime_indexes(plone)

if __name__ == '__main__':
    main()
