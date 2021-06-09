from opengever.nightlyjobs.interfaces import INightlyJobProvider
from Products.CMFPlone.interfaces import IPloneSiteRoot
from zope.annotation import IAnnotations
from zope.component import adapter
from zope.interface import implementer
from zope.publisher.interfaces.browser import IBrowserRequest
import importlib
import logging
from persistent.list import PersistentList


NIGHTLY_MAINTENANCE_JOBS_KEY = 'NIGHTLY_MAINTENANCE_JOBS'


@implementer(INightlyJobProvider)
@adapter(IPloneSiteRoot, IBrowserRequest, logging.Logger)
class NightlyMaintenanceJobsProvider(object):
    """Trigger conversion of archival files for documents that have been put
    in the persistent queue (by the `ArchivalFileChecker`).
    """

    def __init__(self, context, request, logger):
        self.context = context
        self.request = request
        self.logger = logger

    def get_queue(self):
        ann = IAnnotations(self.context)
        queue = ann.get(NIGHTLY_MAINTENANCE_JOBS_KEY, [])
        return queue

    def add_to_queue(self, job):
        ann = IAnnotations(self.context)
        if NIGHTLY_MAINTENANCE_JOBS_KEY not in ann:
            ann[NIGHTLY_MAINTENANCE_JOBS_KEY] = PersistentList()
        queue = self.get_queue()
        queue.append(job)

    def __iter__(self):
        """Iterate over jobs, which as described above, are dossier IntIds.
        """
        queue = self.get_queue()
        # Avoid list size changing during iteration
        jobs = list(queue)

        for job in jobs:
            self.logger.info('executing maintenance job: %r' % job)
            yield job

    def __len__(self):
        return len(self.get_queue())

    def run_job(self, job, interrupt_if_necessary):
        """Run the job. A job should be a dict with at least a key "function_name"
        and "module_name" used to import the corresponding function.
        That function will then get imported and called with the job as argument.
        """
        module = importlib.import_module(job["module_name"])
        function = getattr(module, job["function_name"])
        function(job)
        self.get_queue().remove(job)
        import transaction
        transaction.commit()
