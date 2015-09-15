from opengever.globalindex.handlers.task import TaskSqlSyncer
from opengever.globalindex.model.task import Task
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from pprint import pprint as pp
import transaction


SEPARATOR = '-' * 78
TASK_ATTRIBUTES = ['title', 'text', 'breadcrumb_title', 'physical_path',
                   'review_state', 'icon', 'responsible', 'issuer', 'deadline',
                   'completed', 'modified', 'task_type', 'is_subtask',
                   'sequence_number', 'reference_number', 'containing_dossier',
                   'dossier_sequence_number', 'assigned_org_unit',
                   'principals', 'predecessor', 'containing_subdossier']


def check_is_update_to_date(task, sql_task):
    incorrect_attributes = []
    copy = Task(sql_task.int_id, sql_task.admin_unit_id)
    copy.sync_with(task)

    for attr in TASK_ATTRIBUTES:
        if getattr(copy, attr) != getattr(sql_task, attr):
            incorrect_attributes.append(attr)

    return incorrect_attributes


def find_incorrectly_synched_tasks(portal, options):
    """Check SQL synchronisation for all tasks and returns a list of
    tuples `(object, deviation description)` for all tasks with incorrect
    reflection in the globalindex.
    """

    incorrectly_synched = []
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=['opengever.task.task', 'opengever.inbox.forwarding'])

    for brain in brains:
        task = brain.getObject()
        sql_task = task.get_sql_object()

        if not sql_task:
            incorrectly_synched.append((task, 'not existing'))
            continue

        incorrect_attributes = check_is_update_to_date(task, sql_task)
        if incorrect_attributes:
            incorrectly_synched.append((task, str(incorrect_attributes)))

    return incorrectly_synched


def reindex_tasks(incorrectly_synched):
    for task, msg in incorrectly_synched:
        TaskSqlSyncer(task, None).sync()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-r", dest="reindex", action="store_true", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    incorrectly_synched = find_incorrectly_synched_tasks(plone, options)

    print SEPARATOR
    print SEPARATOR

    if incorrectly_synched:
        print '{} incorrectly_synched tasks detected.'.format(
            len(incorrectly_synched))
        print SEPARATOR
        pp(incorrectly_synched)

        if options.reindex:
            reindex_tasks(incorrectly_synched)
            transaction.commit()
            print SEPARATOR
            print 'All incorrectly synched tasks has been reindexed.'

    else:
        print 'Everything is fine.'

    print SEPARATOR
    print SEPARATOR

if __name__ == '__main__':
    main()
