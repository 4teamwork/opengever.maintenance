"""
Script to delete journal entries from historical CMFEditions versions.

    bin/instance run delete_journal_from_historic_versions.py

"""

from opengever.base.archeologist import Archeologist
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from zope.annotation import IAnnotations
import transaction
from plone import api
import logging





class VersionedJournalDeleter(object):

    JOURNAL_KEY = 'ftw.journal.journal_entries_annotations_key'

    def __init__(self, plone, options):
        self.plone = plone
        self.options = options
        self.catalog = api.portal.get_tool('portal_catalog')
        self.repository = api.portal.get_tool('portal_repository')

    def run(self):
        query_path = self.options.path
        if not query_path:
            query_path = '/'

        interfaces = [
            'ftw.journal.interfaces.IAnnotationsJournalizable',
            'plone.app.versioningbehavior.behaviors.IVersioningSupport',
        ]
        brains = self.catalog.unrestrictedSearchResults(
            path=query_path, object_provides=interfaces)

        total = len(brains)
        for i, brain in enumerate(brains):
            try:
                obj = brain.getObject()
            except KeyError:
                print "KeyError"
                continue
            percent = (i / float(total)) * 100
            print "%s/%s (%.2f%%) Checking %r" % (i, total, percent, obj)
            self.remove_versioned_journals(obj)

    def remove_versioned_journals(self, obj):
        shadow_history = self.repository.getHistoryMetadata(obj)

        if not shadow_history:
            return

        if obj.portal_type == 'ftw.mail.mail':
            print "Skipping mail"
            return

        print "Removing versioned journals for %r" % obj
        numvers = len(shadow_history)
        print "(%s versions)" % numvers

        for version_number in range(len(shadow_history)):
            archeologist = Archeologist(
                obj, self.repository.retrieve(obj, selector=version_number))

            archived_obj = archeologist.excavate()
            archived_ann = IAnnotations(archived_obj)

            print "  Checking version %s..." % version_number
            log.info('Checking...')

            if self.JOURNAL_KEY in archived_ann:
                print "  Removing journal annotations from version %s" % version_number
                removed = archived_ann.pop(self.JOURNAL_KEY)
                print "    Removed: %r" % removed
            # Create a savepoint for performance reasons
            transaction.savepoint()


if __name__ == '__main__':
    app = setup_app()

    log = logging.getLogger('og')
    log.setLevel(logging.INFO)
    stream_handler = log.root.handlers[0]
    stream_handler.setLevel(logging.INFO)

    parser = setup_option_parser()

    parser.add_option("--no-reindex", action="store_true",
                      dest="no_reindex", default=False)
    parser.add_option("--path", default=None)
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        print "Dry-run"
        transaction.doom()

    deleter = VersionedJournalDeleter(plone, options)
    deleter.run()

    if not options.dryrun:
        print "Committing transaction..."
        transaction.commit()
        print "Transaction committed."
