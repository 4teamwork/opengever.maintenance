"""
Script that dumps a JSON with statistics about counts for common
GEVER content types and total number of ZODB objects.
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.stats.catalog import get_contenttype_stats
from opengever.maintenance.stats.zodb import get_object_count
import json


def get_zodb_stats(plone):
    db = plone._p_jar.db()
    obj_count = get_object_count(db)
    return {'zodb_objects': obj_count}


def get_catalog_stats(plone):
    catalog_stats = get_contenttype_stats(plone)
    return catalog_stats


def get_stats(plone):
    stats = {plone.id: {}}

    catalog_stats = get_catalog_stats(plone)
    zodb_stats = get_zodb_stats(plone)

    stats[plone.id].update(catalog_stats)
    stats[plone.id].update(zodb_stats)
    return stats


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    stats = get_stats(plone)
    print json.dumps(stats)


if __name__ == '__main__':
    main()
