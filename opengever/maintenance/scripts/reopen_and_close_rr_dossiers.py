"""
Script to automatically reopen dossiers and complete them at a later time.
"""
from AccessControl.SecurityManagement import getSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManagement import setSecurityManager
from contextlib import contextmanager
from datetime import datetime
from opengever.base.security import UnrestrictedUser
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.exceptions import PreconditionsViolated
from opengever.dossier.reactivate import Reactivator
from opengever.dossier.resolve import LockingResolveManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from persistent.list import PersistentList
from plone import api
from plone.app.uuid.utils import uuidToObject
from zope.annotation import IAnnotations
from zope.globalrequest import getRequest
import argparse
import logging
import sys
import transaction


INPUT_DATEFMT = "%d-%m-%Y"
ANNOTATIONS_KEY = 'rr-dossier-migration'


logger = logging.getLogger('rr-dossier-migration')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


@contextmanager
def elevated_privileges(user_id=None):
    """Copy of elevated_privileges (opengever.base.security) but also
    assign the `Publisher` role, because reactivating a dossier needs the
    publisher role.
    """
    old_manager = getSecurityManager()
    try:
        # Clone the current user and assign a new role.
        # Note that the username (getId()) is left in exception
        # tracebacks in the error_log,
        # so it is an important thing to store.
        if user_id is None:
            user_id = api.user.get_current().getId()

        tmp_user = UnrestrictedUser(user_id, '', ('manage', 'Manager', 'Publisher'), '')

        # Wrap the user in the acquisition context of the portal
        tmp_user = tmp_user.__of__(api.portal.get().acl_users)
        newSecurityManager(getRequest(), tmp_user)

        yield
    finally:
        # Restore the old security manager
        setSecurityManager(old_manager)


class DossierManager(object):

    def write_list_to_the_annotiations(self, uids):
        ann = IAnnotations(api.portal.get())
        if ANNOTATIONS_KEY not in ann:
            ann[ANNOTATIONS_KEY] = PersistentList(uids)
        else:
            uids = list(set(list(ann[ANNOTATIONS_KEY]) + uids))
            ann[ANNOTATIONS_KEY] = PersistentList(uids)

    def open_dossiers(self, start_date):
        catalog = api.portal.get_tool('portal_catalog')
        date_range = {'query': start_date, 'range': 'min'}

        brains = catalog.unrestrictedSearchResults(
            {'object_provides': IDossierMarker.__identifier__,
             'review_state': 'dossier-state-resolved',
             'start': date_range})

        uids = []
        for brain in brains:
            dossier = brain.getObject()
            with elevated_privileges():
                Reactivator(dossier).reactivate()

            uids.append(brain.UID)
            logger.info('{} reopend'.format(brain.getURL()))

        self.write_list_to_the_annotiations(uids)
        logger.info('{} dossiers reopend'.format(len(uids)))

    def close_dossiers(self):
        ann = IAnnotations(api.portal.get())
        if ANNOTATIONS_KEY not in ann:
            raise Exception('No reopened dossiers stored - nothing to do.')

        uids = ann[ANNOTATIONS_KEY]
        for uid in uids:
            dossier = uuidToObject(uid)
            with elevated_privileges():
                try:
                    LockingResolveManager(dossier).resolve()
                    logger.info('{} closed'.format(dossier.absolute_url()))
                    ann[ANNOTATIONS_KEY].remove(uid)
                except PreconditionsViolated:
                    logger.error('Dossier {} could not been resolved'.format(
                        dossier.absolute_url()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['reopen', 'close'])
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('-d', dest='start_date', default=None,
                        help='Start date in ?? format')
    parser.add_argument('-n', dest='dryrun', default=False, help='Dryrun')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()

    setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    if options.mode == 'reopen':
        if not options.start_date:
            raise Exception(
                'start_date ("%d-%m-%Y") is required in reopen mode')

        start_date = datetime.strptime(options.start_date, INPUT_DATEFMT)
        logger.info('Start reopening dossiers, with a start date since {}'.format(start_date))

        DossierManager().open_dossiers(start_date)

    elif options.mode == 'close':
        DossierManager().close_dossiers()

    if not options.dryrun:
        transaction.commit()



if __name__ == '__main__':
    main()
