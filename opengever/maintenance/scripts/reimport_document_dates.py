"""
    Script to reimport document_dates from a bundle document.json.
    See https://extranet.4teamwork.ch/support/gever-st-gallen/tracker/298/view

    bin/instance run reimport_document_dates.py -f ./documents.json

"""
from datetime import date
from ftw.upgrade import ProgressLogger
from opengever.bundle.sections.constructor import BUNDLE_GUID_KEY
from opengever.document.behaviors import IBaseDocument
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from transmogrify.dexterity.interfaces import IDeserializer
from zope.annotation import IAnnotations
import codecs
import json
import logging
import transaction


logger = logging.getLogger('reimport_document_dates')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


MIGRATION_DATE = date(2018, 8, 5)


def _load_json(json_path):
    logger.info("Loading %s" % json_path)
    with codecs.open(json_path, 'r', 'utf-8-sig') as json_file:
        data = json.load(json_file)

    return data


def reimport_document_dates(plone, options):
    data = _load_json(options.json_path)

    deserializer = IDeserializer(IDocumentMetadata['document_date'])
    document_dates = {
        item['guid']: deserializer(item.get('document_date'), None, None)
        for item in data}

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        object_provides=[IBaseDocument.__identifier__],
        bundle_guid=document_dates.keys())

    for brain in ProgressLogger('Update document date', brains):
        obj = brain.getObject()
        guid = IAnnotations(obj).get(BUNDLE_GUID_KEY)
        bundle_document_date = document_dates.pop(guid)

        # Skip items without a document_date
        if not bundle_document_date:
            continue

        # Skip when document already ok.
        if bundle_document_date == obj.document_date:
            continue

        # Skip current date is newer than the migration date.
        if obj.document_date > MIGRATION_DATE:
            continue

        IDocumentMetadata(obj).document_date = bundle_document_date
        obj.reindexObject(idxs=['document_date'])

    if len(document_dates):
        logger.info('Not all document_dates from the bundle has been consumed.')
        logger.info('Remainging UIDS.')
        logger.info(document_dates.keys())


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    parser.add_option("-f", "--json-path", dest="json_path",
                      help="Path to documents.json file ")
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    reimport_document_dates(plone, options)

    if not options.dryrun:
        transaction.commit()
