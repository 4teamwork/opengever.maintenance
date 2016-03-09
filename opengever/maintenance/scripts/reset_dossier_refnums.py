from Acquisition import aq_inner
from Acquisition import aq_parent
from contextlib import contextmanager
from csv import DictWriter
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.base.adapters import CHILD_REF_KEY
from opengever.base.adapters import DOSSIER_KEY
from opengever.base.adapters import PREFIX_REF_KEY
from opengever.base.adapters import REPOSITORY_FOLDER_KEY
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberPrefix
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.browser.refnum_selfcheck import ReferenceNumberChecker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from persistent.dict import PersistentDict
from persistent.list import PersistentList
from Products.CMFCore.utils import getToolByName
from zope.annotation.interfaces import IAnnotations
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
import logging
import os.path
import sys
import transaction
import uuid


DOSSIER_TYPES = (
    'opengever.dossier.businesscasedossier',
    'opengever.dossier.templatedossier',
    'opengever.meeting.meetingdossier',
)

REPOSTORY_TYPES = (
    'opengever.repository.repositoryfolder',
)


class Abort(Exception):
    pass


handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


STATS_CSV_PATH = os.path.abspath(
    os.path.join(__file__,
                 '..', '..', '..', '..',
                 'reset_dossier_refnums_statistics.csv'))


BACKUPS_KEY = 'reference_numbers_backups'
LAST_BACKUP_ID_KEY = 'reference_numbers_last_backup_id'
BACKUP_ID = str(uuid.uuid4())


class DossierRefnumsResetter(object):

    def __init__(self, portal):
        self.portal = portal
        self.catalog = getToolByName(self.portal, 'portal_catalog')
        self.intids = getUtility(IIntIds)

    def __call__(self):
        transaction.get().note('reset_dossier_refnums')
        try:
            with RefnumsStatistics(self.catalog)():
                self.fix_repository_storages()
                self.reset_dossier_refnums()
                print ''
                print ''
                self.update_catalog()

            self.selfcheck()

        except Abort, exc:
            transaction.abort()
            print ''
            print 'ABORTING TRANSACTION'
            print 'Reason:', str(exc)

    def fix_repository_storages(self):
        brains = self.catalog(object_provides=IRepositoryFolder.__identifier__)
        brains = ProgressLogger('Making repository storages persistent.', brains)
        objects = tuple(brain.getObject() for brain in brains)

        changes = map(self._make_repo_storages_persistent, objects)
        print 'Changed refnum storages for {} repository folders.'.format(
            len(filter(None, changes)))
        print ''

        changes = map(self._put_in_parent_mappings, objects)
        print 'Put folder in parent mappings for {} repository folders.'.format(
            len(filter(None, changes)))
        print ''

    def _make_repo_storages_persistent(self, repo_folder):
        changed = False

        annotations = IAnnotations(repo_folder)
        if REPOSITORY_FOLDER_KEY not in annotations:
            return changed

        if type(annotations[REPOSITORY_FOLDER_KEY]) == dict:
            annotations[REPOSITORY_FOLDER_KEY] = PersistentDict(
                annotations[REPOSITORY_FOLDER_KEY])
            changed = True

        container = annotations[REPOSITORY_FOLDER_KEY]
        for key in (CHILD_REF_KEY, PREFIX_REF_KEY):
            if type(container.get(key)) != dict:
                continue

            container[key] = PersistentDict(container[key])
            changed = True

        return changed

    def _put_in_parent_mappings(self, repo_folder):
        changed = False
        parent = aq_parent(aq_inner(repo_folder))
        child = repo_folder
        child_number = IReferenceNumber(child).get_local_number()
        child_intid = self.intids.getId(child)
        ref = IReferenceNumberPrefix(parent)

        is_in_intid_mapping = child_intid in ref.get_prefix_mapping(child)
        if not is_in_intid_mapping:
            ref.set_number(child, number=child_number)
            changed = True

        return changed

    def reset_dossier_refnums(self):
        msg = 'Resetting dossier reference numbers now.'
        brains = self.catalog(object_provides=IDossierMarker.__identifier__,
                              sort_on='created')
        map(self.reset_dossier_by_brain, ProgressLogger(msg, brains))

    def reset_dossier_by_brain(self, brain):
        obj = brain.getObject()
        parent = aq_parent(aq_inner(obj))
        self.maybe_reset_storage(parent)
        IReferenceNumberPrefix(parent).set_number(obj)

    def maybe_reset_storage(self, parent):
        """Reset the reference number storage when we didn't do it already
        in this run.
        """
        annotations = IAnnotations(parent)
        if annotations.get(LAST_BACKUP_ID_KEY, '') == BACKUP_ID:
            # Already done.
            return

        if BACKUPS_KEY not in annotations:
            annotations[BACKUPS_KEY] = PersistentList()

        annotations[BACKUPS_KEY].append(PersistentDict({
            DOSSIER_KEY: annotations.get(DOSSIER_KEY, None),
            LAST_BACKUP_ID_KEY: annotations.get(LAST_BACKUP_ID_KEY, None),
        }))

        annotations[DOSSIER_KEY] = PersistentDict()
        annotations[LAST_BACKUP_ID_KEY] = BACKUP_ID

    def update_catalog(self):
        # We may have changed all reference numbers of dossiers.
        # In order to make sure that the catalog values are up to date,
        # we therefore must reindex all dossiers
        # and all documents within dossiers.
        # Since the repository may be the smaller part of the database,
        # we just reindex everything for simplicity.
        # It may take a little longer, but we really want to be sure that
        # the catalog is consistent.

        msg = 'Catalog: update "reference" index and all metadata.'
        for brain in ProgressLogger(msg, self.catalog()):
            obj = brain.getObject()
            obj.reindexObject(idxs=['reference'])

    def selfcheck(self):
        print ''
        print '=' * 30

        def print_logger(msg):
            print msg

        checker = ReferenceNumberChecker(print_logger, self.portal)
        results = checker.selfcheck()
        if set(results.values()) != {'PASSED'}:
            raise Abort('Selfcheck failed.')


def make_utf8(value):
    if isinstance(value, unicode):
        return value.encode('utf-8')

    if isinstance(value, str):
        return value

    if isinstance(value, list):
        return map(make_utf8, value)

    if isinstance(value, tuple):
        return tuple(map(make_utf8, value))

    if isinstance(value, dict):
        return dict(zip(*map(make_utf8, zip(*value.items()))))

    raise TypeError('make_utf8 does not suppot {}'.format(type(value)))


class RefnumsStatistics(object):

    def __init__(self, catalog):
        self.catalog = catalog

    @contextmanager
    def __call__(self):
        if os.path.exists(STATS_CSV_PATH):
            print 'Error: file exists:', STATS_CSV_PATH
            print 'Not going to overwrite.'
            sys.exit(1)

        stats = self.statistics('before', {})
        yield
        stats = self.statistics('after', stats)
        map(self.validate_stastics, stats.values())
        self.write_statistics(stats)
        not_ok = filter(lambda item: not item['status'].startswith('OK'),
                        stats.values())
        if len(not_ok):
            raise Abort('Having {} wrong items; aborting'.format(
                len(not_ok)))

    def statistics(self, key, stats):
        for brain in self.catalog(sort_on='path'):
            obj = brain.getObject()
            if self.should_be_skipped(obj):
                continue

            if brain.getPath() not in stats:
                stats[brain.getPath()] = {
                    'path': brain.getPath(),
                    'portal_type': brain.portal_type,
                    'title': brain.Title,
                }

            stats[brain.getPath()].update({
                key + '-catalog': brain.reference,
                key + '-obj': IReferenceNumber(obj).get_number(),
            })

        return stats

    def validate_stastics(self, item):
        keys = {'before-catalog', 'before-obj', 'after-catalog', 'after-obj'}
        item['status'] = 'OK: -'

        if keys - set(item):
            item['status'] = 'ERROR: Missing infos'

        if item['portal_type'] in DOSSIER_TYPES:
            if item['after-catalog'] != item['after-obj']:
                item['status'] = 'ERROR: catalog not up to date?'
            if item['after-obj'] != item['before-obj']:
                item['status'] = 'OK: changed'

        if item['portal_type'] in REPOSTORY_TYPES:
            unique_values = set([value for (key, value) in item.items()
                             if key in keys])
            if len(unique_values) > 1:
                item['status'] = ('ERROR: unpexpected changes in repository object')

    def should_be_skipped(self, obj):
        if IRepositoryRoot.providedBy(obj):
            return True

        return False

    def write_statistics(self, items):
        fieldnames = ['portal_type',
                      'status',
                      'before-catalog',
                      'before-obj',
                      'after-catalog',
                      'after-obj',
                      'title',
                      'path']

        print ''
        print ''
        print 'INFO: Writing statistics to', STATS_CSV_PATH

        with open(STATS_CSV_PATH, 'w+') as csvfile:
            writer = DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in items.values():
                if len(set(item) - set(fieldnames)):
                    raise Abort('Unexpected keys {} in {}'.format(
                        set(item) - set(fieldnames), item))
                writer.writerow(make_utf8(item))


if __name__ == '__main__':
    app = setup_app()
    plone = setup_plone(app, [])

    if True:
        print 'WARNING: transaction dommed because we are in dry-mode.'
        print ''
        transaction.doom()

    DossierRefnumsResetter(plone)()
    transaction.commit()
