"""
Script to update IDs (and therefore URLs) of one or more objects.

    bin/instance run update_object_ids.py [-n] <file_with_paths>

<file_with_paths> should be a file with one path per line containing the list
of objects that should be checked, and possibly updated.

Each of these objects' IDs will be checked, and if it isn't equal to the ID
that an object with that title would normally get, it will be updated
accordingly.

Notes:
 - IDs of parents or children won't be checked/updated - only the ID of the object
   directly addressed by the given path. So this script is NOT recursive.
 - Journaling will be disabled for any renames. Therefore there won't be
   any journal entries for this operation.
"""

from opengever.base.interfaces import IReferenceNumber
from opengever.journal import handlers as journal_handlers
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from plone.app.content.interfaces import INameFromTitle
from plone.i18n.normalizer.interfaces import IURLNormalizer
from zope.component import queryUtility
import argparse
import sys
import transaction


class DisabledJournaling(object):
    """Context manager that temporarily disables creation of journal entries.
    """

    def __enter__(self):
        self._orig_journal_factory = journal_handlers.journal_entry_factory
        journal_handlers.journal_entry_factory = self.dummy_journal_factory

    def __exit__(self, exc_type, exc_val, exc_tb):
        journal_handlers.journal_entry_factory = self._orig_journal_factory
        self._orig_journal_factory = None

    @staticmethod
    def dummy_journal_factory(*args, **kwargs):
        pass


class ObjectIDUpdater(object):

    def __init__(self, obj, options):
        self.obj = obj
        self.options = options

    def maybe_update_id(self):
        if self.needs_update():
            self.update_object_id()

    def needs_update(self):
        expected_id = self.get_expected_id_for_obj()
        return self.obj.id != expected_id

    def get_expected_id_for_obj(self):
        name_from_title = INameFromTitle(self.obj, None)
        if name_from_title is None:
            raise AttributeError

        name = name_from_title.title
        util = queryUtility(IURLNormalizer)
        return util.normalize(name, locale='de')

    def update_object_id(self):
        refnum_before = IReferenceNumber(self.obj).get_number()
        new_id = self.get_expected_id_for_obj()

        print "Renaming %r to %r" % (self.obj, new_id)

        if self.options.dry_run:
            # Solr isn't bound to our transaction manager, so a txn.doom()
            # wouldn't prevent reindexes during dry-run from ending up in Solr
            return

        with DisabledJournaling():
            obj = api.content.rename(self.obj, new_id)

        # Ensure that reference number didn't change
        refnum_after = IReferenceNumber(obj).get_number()
        assert refnum_before == refnum_after
        assert type(refnum_before) is type(refnum_after)


class ObjectIDFixer(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options

    def run(self):
        fn = self.options.file
        with open(fn) as infile:
            paths = infile.readlines()

        # Sort paths in reverse order, so that we rename children before
        # parents. Otherwise we would fail to look up later paths because
        # a parent in them has already been renamed.
        paths = sorted(
            [path.strip() for path in paths if path.strip()],
            reverse=True)

        for path in paths:
            try:
                obj = self.portal.unrestrictedTraverse(path)
            except KeyError:
                print "Not found: %r (probably already renamed)" % path
                continue

            # We must only attempt to update the ID for objects that actually
            # derive their ID from their title. For now, this is explicitly
            # limited to repository folders for that reason.
            if not IRepositoryFolder.providedBy(obj):
                print "Refused: %r (not a RepositoryFolder)" % path
                continue

            id_updater = ObjectIDUpdater(obj, options)
            id_updater.maybe_update_id()


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('file',
                        help='File with object paths')
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="Dry run")

    options = parser.parse_args(sys.argv[3:])

    if options.dry_run:
        print "DRY RUN"
        transaction.doom()

    plone = setup_plone(app, options)

    fixer = ObjectIDFixer(plone, options)
    fixer.run()

    if not options.dry_run:
        transaction.commit()
