"""
DELETES orphaned OGDS tasks.

OGDS tasks are considered orphaned if no Plone task exists at the
`physical_path` that the OGDS record points to.

These can occur as a leftover if the zope instance was killed during task
creation for some reason.

    bin/instance run delete_orphaned_ogds_tasks.py --i-know-what-im-doing -n

"""
from opengever.globalindex.model.task import Task
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import create_session
from opengever.ogds.base.utils import get_current_admin_unit
from plone import api
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
import transaction


def delete_task(session, ogds_task):
    print "Deleting task %r (%s)" % (ogds_task, ogds_task.physical_path)
    session.delete(ogds_task)


def delete_ogds_tasks_without_corresponding_plone_task(plone, options):
    session = create_session()
    catalog = api.portal.get_tool('portal_catalog')
    intids = getUtility(IIntIds)

    orphaned_ogds_tasks = []

    # Get tasks on current admin unit
    current_au_id = get_current_admin_unit().unit_id
    ogds_tasks = session.query(Task).filter_by(admin_unit_id=current_au_id)

    for ogds_task in ogds_tasks:
        assert ogds_task.admin_unit_id == plone.id
        path = str(ogds_task.physical_path)
        container_path = '/'.join(path.split('/')[:-1])

        # Attempt to traverse to the supposed task's physical_path. If it
        # can't be found, it is considered orphaned and should be safe to
        # delete from OGDS.
        try:
            plone.unrestrictedTraverse(path)
        except KeyError:
            # Corresponding Plone object could not be found. Perform a couple
            # of sanity checks, and if those pass, consider the task orphaned.

            # First safeguard: Attempt to traverse to the supposed container
            # first. If we can't find *that*, then a different problem exists
            # that needs to be investigated separately.
            try:
                plone.unrestrictedTraverse(container_path)
            except KeyError:
                print ("WARNING: Unable to find container %r for task at %r, "
                       "skipping." % (container_path, path))
                continue

            # Second safeguard: Task must not be referenced from catalog
            brains = catalog.unrestrictedSearchResults(
                path='%s/%s' % (plone.id, path))
            if len(brains) > 0:
                print ("WARNING: Task at %r still appears in catalog, "
                       "skipping." % path)
                continue

            # Third safeguard: Referenced IntId must not resolve to an obj
            obj = intids.queryObject(ogds_task.int_id)
            if obj is not None:
                print ("WARNING: IntId %r resolves to object %r, "
                       "skipping" % (ogds_task.int_id, obj))
                continue

            if ogds_task.is_subtask or ogds_task.predecessor is not None:
                print ("WARNING: Task at %r (%s) is either a subtask or has a "
                       "predecessor. This script only deals with simple "
                       "cases, for this orphaned task you may need to "
                       "manually fix it and take care of the remote side's "
                       "state" % (ogds_task, path))
                continue

            orphaned_ogds_tasks.append(ogds_task)

    # Display (dry-run) or delete the orphaned tasks
    for ot in orphaned_ogds_tasks:
        print "Orphaned task: %r (%s)" % (ot, ot.physical_path)
        if not options.dryrun:
            delete_task(session, ot)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()

    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)

    parser.add_option("--i-know-what-im-doing", action="store_true",
                      dest="user_knows_what_theyre_doing", default=False)

    (options, args) = parser.parse_args()
    assert options.user_knows_what_theyre_doing

    plone = setup_plone(app, options)

    if options.dryrun:
        print 'dryrun ...'
        transaction.doom()

    delete_ogds_tasks_without_corresponding_plone_task(plone, options)

    if not options.dryrun:
        transaction.get().note(
            "Delete OGDS tasks without corresponding Plone task")
        transaction.commit()
