from opengever.document.behaviors import IBaseDocument
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.adapters import IResponseContainer
from opengever.task.task import ITask
from plone import api
import transaction


SEPARATOR = '-' * 78


def get_tasks_with_non_transition_responses(portal, options):
    """Find all tasks with responses.
    """
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        object_provides='opengever.task.task.ITask')

    response_counter = 0
    broken_responses = []
    broken_tasks = []


    for brain in brains:
        task = brain.getObject()
        responses = IResponseContainer(task)
        for response in responses:
            response_counter += 1
            if not response.transition:
                broken_tasks.append(task)
                broken_responses.append(response)
                fix_broken_response(response, task)
                print 'fixed {}'.format(task)

    print 'CHECKED {} tasks with {} responses'.format(len(brains), response_counter)
    print 'Found {} broken responses on {} tasks'.format(
        len(broken_responses), len(set(broken_tasks)))

    return broken_tasks

def fix_broken_response(broken_response, task):
    if broken_response.changes:
        change_ids = [change.get('id') for change in broken_response.changes]
        if change_ids == ['deadline']:
            broken_response.transition = 'task-transition-modify-deadline'
            print 'Set modify-deadline for: {}'.format(task.absolute_url())

    elif broken_response.added_object:
        if ITask.providedBy(broken_response.added_object.to_object):
            broken_response.transition = 'transition-add-subtask'
            print 'Set add-subtask for: {}'.format(task.absolute_url())
        elif IBaseDocument.providedBy(broken_response.added_object.to_object):
            broken_response.transition = 'transition-add-document'
            print 'Set add-document for: {}'.format(task.absolute_url())
        else:
            print 'No fix found for {}'.format(broken_response.__dict__)

    else:
        print 'No fix found for {}'.format(broken_response.__dict__)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    get_tasks_with_non_transition_responses(plone, options)

    raw_input('Press ENTER to commit')
    transaction.commit()

if __name__ == '__main__':
    main()
