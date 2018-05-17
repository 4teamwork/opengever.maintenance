"""
Script to count number of checked out documents per user.

    bin/instance run count_checked_out_docs.py

"""
from collections import Counter
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction
from pprint import pprint


def median(numbers):
    numbers = sorted(numbers)
    center = len(numbers) / 2
    if len(numbers) % 2 == 0:
        return sum(numbers[center - 1:center + 1]) / 2.0
    else:
        return numbers[center]


def count_checked_out_documents(plone, options):
    counts = Counter()

    catalog = api.portal.get_tool('portal_catalog')
    index = catalog._catalog.indexes['checked_out']

    for value in index.uniqueValues():
        if value == '':
            continue
        brains = catalog.unrestrictedSearchResults(
            portal_type='opengever.document.document',
            checked_out=value,
        )
        counts[value] = len(brains)

    stats = dict()
    stats['counts'] = dict(counts)
    stats['min'] = min(counts.values())
    stats['max'] = max(counts.values())
    stats['med'] = median(counts.values())
    stats['avg'] = float(sum(counts.values())) / len(counts)

    pprint(stats)

if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    count_checked_out_documents(plone, options)
