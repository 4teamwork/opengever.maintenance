from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFPlone.CatalogTool import MAX_SORTABLE_TITLE
import logging
import re
import transaction

logger = logging.getLogger('opengever.maintenance')
SEPARATOR = '-' * 78

# Finds only string containing digits, but was not found by the regex of the
# plone upgrade (`plone.app.upgrade.v43.alphas.num_sort_regex`)
num_sort_regex = re.compile('\D+\d+')
six_digits_regex = re.compile('\d{6}')


def fix_broken_sortable_title_indexes(options):
    """Find brains which has wrongly not been reindexed by
    the plone.app.upgrade.v43.reindex_sortable_title, and reindex them.
    """

    # copied from plone.app.upgrade.v43.alphas.py
    catalog = api.portal.get_tool('portal_catalog')
    _catalog = catalog._catalog
    indexes = _catalog.indexes
    sort_title_index = indexes.get('sortable_title', None)
    if sort_title_index is None:
        logger.warn('Fix script cancelled, no sort_title_index found.')
        return

    from Products.PluginIndexes.FieldIndex import FieldIndex
    if not isinstance(sort_title_index, FieldIndex.FieldIndex):
        logger.warn('Fix script cancelled, sort_title_index is not a FieldIndex.')
        return

    change = []
    for i, (name, rids) in enumerate(sort_title_index._index.iteritems()):
        if len(name) > MAX_SORTABLE_TITLE or num_sort_regex.match(name):
            if not six_digits_regex.search(name):
                logger.warn('Ignoring: {}'.format(name))
                continue
            else:
                logger.warn('Scheduled for fixing: {}'.format(name))

            if hasattr(rids, 'keys'):
                change.extend(list(rids.keys()))
            else:
                change.append(rids)

    logger.warn('Analyzing finished.')
    logger.warn('{} objects affected.'.format(len(change)))

    update_metadata = 'sortable_title' in _catalog.schema
    logger.warn('start fixing, with update_metadata={}'.format(update_metadata))
    for i, rid in enumerate(change):
        brain = _catalog[rid]
        try:
            obj = brain.getObject()
        except AttributeError:
            continue
        if update_metadata:
            obj.reindexObject()
        else:
            obj.reindexObject(idxs=['sortable_title'])

    if not options.dry_run:
        transaction.commit()

def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    logger.warn("Date: {}".format(datetime.now().isoformat()))
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        logger.warn("DRY-RUN")

    fix_broken_sortable_title_indexes(options)


if __name__ == '__main__':
    main()
