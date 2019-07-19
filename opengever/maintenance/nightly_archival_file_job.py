from opengever.document.archival_file import ArchivalFileConverter
from plone import api
from Products.CMFPlone.interfaces import IPloneSiteRoot
from time import sleep
from zope.annotation import IAnnotations
from zope.component import adapter
from zope.component import getUtility
from zope.interface import implementer
from zope.interface import Interface
from zope.intid.interfaces import IIntIds
from zope.publisher.interfaces.browser import IBrowserRequest
import logging

# Conditional import to not cause issues on og.core versions where
# opengever.nightlyjobs.interfaces doesn't exist yet
try:
    from opengever.nightlyjobs.interfaces import INightlyJobProvider
except ImportError:
    # Older og.core version - provide a dummy interface
    class INightlyJobProvider(Interface):
        pass


MISSING_ARCHIVAL_FILE_KEY = 'DOCS_WITH_MISSING_ARCHIVAL_FILE'
MAX_CONVERSION_REQUESTS_PER_NIGHT = 1000

# Track total number of conversion requests sent per nightly run
sent_conversion_requests = 0


@implementer(INightlyJobProvider)
@adapter(IPloneSiteRoot, IBrowserRequest, logging.Logger)
class NightlyArchivalFileConversion(object):
    """Trigger conversion of archival files for documents that have been put
    in the persistent queue (by the `ArchivalFileChecker`).
    """

    def __init__(self, context, request, logger):
        self.context = context
        self.request = request
        self.logger = logger

        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)

    def get_queue(self):
        """The queue is a BTree in the site root's annotations.

        It is a mapping of dossier IntIds to lists of document IntIds (the
        documents being the ones that are missing their archival file).

        It is grouped by dossier because this allows us to process the queue
        in more reasonably sized chunks, instead of every single document
        being considered a "job" (which would lead to a commit every time).

        So the unit of work for this job is a dossier, and during the
        execution of that job it will trigger the conversion for that dossiers
        missing archival files.
        """
        ann = IAnnotations(self.context)
        queue = ann.get(MISSING_ARCHIVAL_FILE_KEY, {})
        return queue

    def __iter__(self):
        """Iterate over jobs, which as described above, are dossier IntIds.
        """
        queue = self.get_queue()
        # Avoid list size changing during iteration
        jobs = list(queue.keys())

        for job in jobs:
            self.logger.info('sent_conversion_requests: %r' % sent_conversion_requests)
            if sent_conversion_requests >= MAX_CONVERSION_REQUESTS_PER_NIGHT:
                self.logger.warn(
                    "Reached MAX_CONVERSION_REQUESTS_PER_NIGHT "
                    "(%r) limit, stopping work for tonight." %
                    MAX_CONVERSION_REQUESTS_PER_NIGHT)
                raise StopIteration

            yield job

    def __len__(self):
        return len(self.get_queue())

    def trigger_conversion(self, doc):
        self.logger.info("Triggering conversion for %r" % doc)
        ArchivalFileConverter(doc).trigger_conversion()

    def run_job(self, job, interrupt_if_necessary):
        """Run the job for the dossier identified by the IntId `job`.

        This means getting all the document IntIds that this dossier IntIds
        points to in the queue, and triggering conversion for those documents.
        """
        global sent_conversion_requests

        dossier_intid = job
        dossier = self.intids.getObject(dossier_intid)
        self.logger.info("Triggering conversion jobs for documents in %r" % dossier)

        queue = self.get_queue()
        for doc_intid in queue[dossier_intid]:
            interrupt_if_necessary()
            doc = self.intids.getObject(doc_intid)
            self.trigger_conversion(doc)
            sent_conversion_requests += 1

            # Stagger conversion requests at least a little bit, in order to
            # avoid overloading Bumblebee. This likely will have to be tuned.
            sleep(1)

        queue.pop(dossier_intid)
