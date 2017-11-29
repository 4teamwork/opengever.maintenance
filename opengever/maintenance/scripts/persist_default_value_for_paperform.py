from logging import Formatter
from logging import getLogger
from logging import INFO
from logging import StreamHandler
from opengever.base.default_values import object_has_value_for_field
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.document.behaviors.metadata import preserved_as_paper_default
from opengever.document.behaviors import IBaseDocument
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from time import time
import gc
import transaction


def fire_gc():
    start = time()
    logger.info('Firing garbage collection')
    api.portal.get()._p_jar.cacheGC()
    gc.collect()
    logger.info('Garbage collection done in %.3fs', time() - start)


def persist_default_value_for_paperform():
    catalog = api.portal.get_tool('portal_catalog')
    query_filter = {
        'object_provides': IBaseDocument.__identifier__,
        }

    results = catalog.unrestrictedSearchResults(**query_filter)
    total_count = len(results)

    field = IDocumentMetadata['preserved_as_paper']
    default_value = preserved_as_paper_default()

    logger.info('Persisting paperform default values on documents')

    for i, brain in enumerate(results):
        title = brain.Title

        # We can skip documents which had this value explicitly set
        document = brain.getObject()
        if not object_has_value_for_field(document, field):
            field.set(field.interface(document), default_value)
            action = 'Persisted'
        else:
            action = 'Skipped'

        logger.info(
            '%s %s %06d / %06d',
            action,
            title,
            i,
            total_count,
            )

        # Fire GC every 100 brain.getObject()
        if i > 0 and i % 100 == 0:
            fire_gc()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    options = parser.parse_args()[0]

    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    persist_default_value_for_paperform()

    if not options.dry_run:
        transaction.commit()


if __name__ == '__main__':
    logger = getLogger('persist-paperform-defaultvalues')
    logger.setLevel(INFO)

    stream_handler = StreamHandler()
    stream_handler.setLevel(INFO)
    log_formatter = Formatter(
        '%(asctime)s %(levelname)s %(message)s',
        '%Y-%m-%d %H:%M:%S',
        )
    stream_handler.setFormatter(log_formatter)

    logger.addHandler(stream_handler)

    main()
