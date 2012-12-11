from collections import Counter
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone


SEPARATOR = "-" * 46


def count_objects(portal, options):
    counter = Counter()
    catalog = portal.portal_catalog
    portal_types = catalog.uniqueValuesFor('portal_type')

    for portal_type in portal_types:
        brains = catalog(portal_type=portal_type)
        counter[portal_type] = len(brains)

    for pt, count in counter.items():
        print "%s %s" % (pt.ljust(40), str(count).rjust(5))

    total = sum(counter.values())
    print SEPARATOR
    print "Total: %s objects" % total


def main():
    app = setup_app()
    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    plone = setup_plone(app, options)

    print SEPARATOR
    count_objects(plone, options)


if __name__ == '__main__':
    main()