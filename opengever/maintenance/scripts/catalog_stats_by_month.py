"""
Script that dumps a JSON with statistics about counts for common
GEVER content types grouped by months.

Example usage: bin/instance run catalog_stats_by_month.py 2015-01 2016-03
"""

from collections import namedtuple
from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.stats.catalog import get_contenttype_stats
import json


YearMonth = namedtuple('YearMonth', ['year', 'month'])


def itermonths(start, end):
    assert end > start
    start = YearMonth(*start)
    end = YearMonth(*end)
    current = start
    while not current > end:
        yield current
        if current.month == 12:
            current = YearMonth(current.year + 1, 1)
        else:
            current = YearMonth(current.year, current.month + 1)


def itermonthrange(start, end):
    months = itermonths(start, end)
    prev = next(months)
    for month in months:
        rng = (prev, month)
        prev = month
        yield rng


def get_catalog_stats_per_month(plone, date_from, date_to):
    stats_by_month = {}
    date_from = datetime.strptime(date_from, '%Y-%m').date()
    date_to = datetime.strptime(date_to, '%Y-%m').date()

    date_from = YearMonth(date_from.year, date_from.month)
    date_to = YearMonth(date_to.year, date_to.month)

    for rng_start, rng_end in itermonthrange(date_from, date_to):
        daterange = (
            datetime(rng_start.year, rng_start.month, 1, 0, 0),
            datetime(rng_end.year, rng_end.month, 1, 0, 0),
        )

        month_key = '%s-%s' % (rng_start.year, rng_start.month)
        print "Querying contenttype stats for %s..." % month_key
        catalog_stats = get_contenttype_stats(
            plone, daterange=daterange)

        stats_by_month[month_key] = catalog_stats

    return stats_by_month


def get_stats(plone, options, args):
    stats = {plone.id: {}}
    start, end = args

    catalog_stats = get_catalog_stats_per_month(plone, start, end)
    stats[plone.id].update(catalog_stats)

    total_stats = get_contenttype_stats(plone)
    stats[plone.id]['current_totals'] = total_stats
    return stats


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    stats = get_stats(plone, options, args)
    print json.dumps(stats)


if __name__ == '__main__':
    main()
