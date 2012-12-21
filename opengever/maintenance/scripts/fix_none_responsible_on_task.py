from opengever.globalindex.handlers.task import index_task
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.interfaces import ISuccessorTaskController
from persistent.list import PersistentList
from opengever.task.adapters import IResponseContainer
import transaction


def fix_none_responsible(app):

    plone = setup_plone(app)

    print "Get brains of task..."
    brains = plone.portal_catalog(
        {'portal_type': 'opengever.task.task'})

    print "%s task found" % len(brains)

    print "check local roles on task ..."

    for brain in brains:
        task = brain.getObject()

        responsible = task.responsible

        if responsible.startswith('inbox:'):
            responsible = 'og_%s_eingangskorb' %(responsible[6:])

        if responsible == 'None':
            if task.predecessor:
                new_task = ISuccessorTaskController(task).get_predecessor()
            else:
                new_task = ISuccessorTaskController(task).get_successors()[0]

            new_responsible = new_task.responsible
            new_responsible_client = new_task.assigned_client

            print "None Responsible: Task %s should change to %s %s (%s:%s)" %(
                '/'.join(task.getPhysicalPath()),
                new_responsible, new_responsible_client,
                new_task.client_id, new_task.physical_path)

            task.responsible = new_responsible
            task.responsible_client = new_responsible_client

            index_task(task, None)

            last_response = IResponseContainer(task)[-1]

            new_changes = PersistentList()
            for change in last_response.changes:
                if change.get('id') in ['responsible', 'reponsible'] and change.get('after') == 'None':
                    # drop it
                    pass
                else:
                    new_changes.append(change)
            last_response.changes = new_changes

            transaction.commit()


def main():

    # check if we have a zope environment aka 'app'
    mod = __import__(__name__)
    if not ('app' in dir(mod) or 'app' in globals()):
        print "Must be run with 'zopectl run'."
        return

    fix_none_responsible(app)


if __name__ == '__main__':
    main()
