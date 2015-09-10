from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
from plone import api


SEPARATOR = '-' * 78

OUTPUT_ENCODING = 'utf-8'


def find_long_task_titles(portal, options):
    """Find all tasks whose title is too long
    """
    admin_unit_id = portal.id
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.task.task', 'opengever.inbox.forwarding'])

    for brain in brains:
        task = brain.getObject()
        if len(task.title) > 255:
            admin_unit = get_current_admin_unit()
            path = '/'.join(task.getPhysicalPath()[2:]).decode('ascii')
            url = u'/'.join((admin_unit.public_url, path.decode('ascii')))
            title = task.title

            csv_line = u';'.join(
                u'"{}"'.format(v) for v in (admin_unit_id, url, title))
            print csv_line.encode(OUTPUT_ENCODING)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    find_long_task_titles(plone, options)


if __name__ == '__main__':
    main()
