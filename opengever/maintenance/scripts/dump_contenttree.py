"""
Script that dumps a content tree.

    bin/instance run dump_contenttree.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


def dump_contenttree():
    catalog = api.portal.get_tool('portal_catalog')
    all_brains = catalog.unrestrictedSearchResults(sort_on='path')

    nodes = []
    for brain in all_brains:
        node = {'path': brain.getPath(),
                'portal_type': brain.portal_type}
        nodes.append(node)

    for node in nodes:
        print node

if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    setup_plone(app, options)
    transaction.doom()

    dump_contenttree()
