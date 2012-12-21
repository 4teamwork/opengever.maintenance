from opengever.maintenance.debughelpers import setup_plone
from opengever.task.localroles import LocalRolesSetter
import transaction


def update_local_roles_on_task(app):

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

        temp = {}
        for principal, roles in task.get_local_roles():
            temp[principal] = roles

        if responsible in temp.keys():
            if 'Editor' in temp[responsible]:
                continue
        print 'Task %s not be authorized on %s ' % (
            responsible,
            '/'.join(task.getPhysicalPath()))

        print 'local roles setter is working ...'
        LocalRolesSetter(task)(None)
        transaction.commit()
        print 50 * '-'

def main():

    # check if we have a zope environment aka 'app'
    mod = __import__(__name__)
    if not ('app' in dir(mod) or 'app' in globals()):
        print "Must be run with 'zopectl run'."
        return

    update_local_roles_on_task(app)


if __name__ == '__main__':
    main()
