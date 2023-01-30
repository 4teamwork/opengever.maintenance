from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.base.model import create_session
from opengever.globalindex.model.task import Task
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.task import ITask
from plone import api
from zope.component import getUtility
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
from zope.intid import IIntIds
import argparse
import sys


class OrphanedTaskFinder(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.request = getRequest()
        alsoProvides(self.request, IOpengeverBaseLayer)
        self._orphaned_plone_tasks = None

    def find_local_tasks_by_title(self, title):
        candidates = [t for t in self._orphaned_plone_tasks if t.title == title]
        return candidates

    def run(self):
        intids = getUtility(IIntIds)
        session = create_session()

        # Check Plone tasks first
        orphaned_plone_tasks = []
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog(object_provides=ITask.__identifier__)
        for brain in brains:
            plone_task = brain.getObject()
            plone_int_id = intids.queryId(plone_task)
            if plone_int_id is None:
                print('Task %r is missing intid' % plone_task)
            sql_task = plone_task.get_sql_object()
            if not sql_task:
                orphaned_plone_tasks.append(plone_task)

        self._orphaned_plone_tasks = orphaned_plone_tasks
        print('\nOrphaned Plone Tasks: %r' % len(orphaned_plone_tasks))
        print('=' * 80)
        for task in orphaned_plone_tasks:
            print('%r (Title: %r)' % (task, task.title))

        # Check SQL tasks
        orphaned_by_intid = {}
        orphaned_by_path = {}

        tasks = session.query(Task).filter_by(admin_unit_id='gdgs')
        for task in tasks:
            sql_int_id = task.int_id
            plone_obj = intids.queryObject(sql_int_id)
            if plone_obj is None:
                orphaned_by_intid[sql_int_id] = task

            relative_path = task.physical_path.encode('utf-8')
            try:
                plone_obj = self.portal.unrestrictedTraverse(relative_path)
            except KeyError:
                orphaned_by_path[relative_path] = task

        print('\nOrphaned by IntId: %r' % len(orphaned_by_intid))
        print('=' * 80)
        print
        for task in orphaned_by_intid.values():
            print('Task ID: %r (IntId: %r Title: %r)' % (task.id, task.int_id, task.title))
            candidates = self.find_local_tasks_by_title(task.title)
            for cand in candidates:
                path = '/'.join(cand.getPhysicalPath())
                candidate_intid = intids.queryId(cand)
                print('    Candidate: %s (IntId: %r)' % (path, candidate_intid))
            print

        print('\nOrphaned by path: %r' % len(orphaned_by_path))
        print('=' * 80)
        print
        for task in orphaned_by_path.values():
            print('Task ID %r (Path: %r Title: %r)' % (task.id, task.physical_path, task.title))
            candidates = self.find_local_tasks_by_title(task.title)
            for cand in candidates:
                path = '/'.join(cand.getPhysicalPath())
                candidate_intid = intids.queryId(cand)
                print('    Candidate: %s (IntId: %r)' % (path, candidate_intid))
            print


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')

    options = parser.parse_args(sys.argv[3:])

    plone = setup_plone(app, options)

    finder = OrphanedTaskFinder(plone, options)
    finder.run()
