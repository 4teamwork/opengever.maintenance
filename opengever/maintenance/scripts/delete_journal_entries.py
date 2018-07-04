from ftw.journal.config import JOURNAL_ENTRIES_ANNOTATIONS_KEY
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from persistent.list import PersistentList
from plone import api
from zope.annotation.interfaces import IAnnotations
import logging
import transaction


logger = logging.getLogger('delete_journal_entries')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)

def delete_journal_entries(portal, options):
    uid = options.uid
    if not uid:
        raise Exception('Missing UID parameter')

    catalog = api.portal.get_tool('portal_catalog')
    results = catalog(UID=uid)
    if not len(results):
        raise Exception('Object with the UID {} not found'.format(uid))

    obj = results[0].getObject()

    annotations = IAnnotations(obj)
    if not annotations.get(JOURNAL_ENTRIES_ANNOTATIONS_KEY):
        logger.info('No journal entries existing')
        return

    logger.info('Removing {} journal entries'.format(
        len(annotations[JOURNAL_ENTRIES_ANNOTATIONS_KEY])))

    annotations[JOURNAL_ENTRIES_ANNOTATIONS_KEY] = PersistentList()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-u", "--uid", dest="uid", help="UID for object")
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    delete_journal_entries(plone, options)

    if not options.dryrun:
        transaction.commit()

if __name__ == '__main__':
    main()
