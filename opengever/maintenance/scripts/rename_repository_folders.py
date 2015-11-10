"""
Script to rename the id of repository folders, when the id does no longer
correlate with the title.

USAGE: bin/instance run <path_to>/rename_repository_folders.py
OPTIONS:
 -v verbose
 -n dry run
 -s site root
"""

from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.task import ITask
from plone import api
from plone.app.content.interfaces import INameFromTitle
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import queryUtility
import transaction


SEPARATOR = '-' * 78


def get_id_for_obj(obj):
    nameFromTitle = INameFromTitle(obj, None)
    if nameFromTitle is None:
        raise AttributeError

    name = nameFromTitle.title
    util = queryUtility(IURLNormalizer)
    return util.normalize(name, locale='de')


def is_equal(current, new):
    current = normalize(current)
    new = normalize(new)
    return current == new or new in current

def normalize(value):
    value.replace('ue', 'u')
    value.replace('ae', 'a')
    value.replace('oe', 'o')
    return value


def check_ids():
    to_fix = []
    catalog = api.portal.get_tool('portal_catalog')
    repository_folders = catalog(portal_type='opengever.repository.repositoryfolder')

    for brain in repository_folders:
        repo = brain.getObject()
        current_id = repo.getId()
        new_id = get_id_for_obj(repo)

        if current_id != new_id:
            to_fix.append({'obj': repo, 'new_id': new_id, 'current_id': current_id})

    return to_fix


def rename(repo, correct_id):
    return api.content.rename(repo, new_id=correct_id, safe_id=True)


def reindex_task_path(repo):
    catalog = api.portal.get_tool('portal_catalog')
    tasks = catalog(object_provides=ITask.__identifier__,
                                 path='/'.join(repo.getPhysicalPath()))

    for task in tasks:
        task = task.getObject()
        sql_task = task.get_sql_object()
        sql_task.physical_path = task.get_physical_path()


def rename_repositories(options):
    to_fix = check_ids()
    for item in to_fix:
        rename(item.get('obj'), item.get('new_id'))
        reindex_task_path(item.get('obj'))

        if options.verbose:
            print 'Renamed: {}'.format(item)

        if not options.dry_run:
            transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    print SEPARATOR
    print "Date: {}".format(datetime.now().isoformat())
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    rename_repositories(options)

    print "Done."
    print SEPARATOR
    print SEPARATOR


if __name__ == '__main__':
    main()
