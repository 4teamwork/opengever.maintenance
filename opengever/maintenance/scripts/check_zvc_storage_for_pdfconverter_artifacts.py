"""
This script checks the ZVCStorage for pdfconverter artifacts.

Example Usage:

    bin/instance run check_zvc_storage_for_pdfconverter_artifacts.py
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from Products.CMFEditions.utilities import dereference
import transaction
from Products.CMFEditions.interfaces.IStorage import StorageRetrieveError


def check_zvc_storage_for_pdfconverter_artifacts(plone, options):
    catalog = api.portal.get_tool('portal_catalog')
    portal_archivist = api.portal.get_tool('portal_archivist')

    objects_ok = []
    objects_artifact = []
    objects_storage_retrieve_error = []

    def get_history_id(obj):
        history_id = dereference(obj, zodb_hook=portal_archivist)[1]
        return history_id

    def get_lazy_history(obj):
        history = portal_archivist.getHistory(obj, get_history_id(obj), [], True)
        return history

    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')

    total = len(brains)
    for i, brain in enumerate(brains):
        if i % 100 == 0:
            print "Checking %s / %s" % (i, total)

        obj = brain.getObject()
        obj_path = '/'.join(obj.getPhysicalPath())

        history = get_lazy_history(obj)
        try:
            vdata = history._history[None]
        except StorageRetrieveError:
            objects_storage_retrieve_error.append(obj_path)
            continue

        referenced_data = getattr(vdata, 'referenced_data', None)

        if 'pdfconverter' in repr(referenced_data):
            objects_artifact.append(obj_path)
        else:
            objects_ok.append(obj_path)

    for path in objects_storage_retrieve_error:
        print "StorageRetrieveError: %s" % path

    for path in objects_ok:
        print "OK: %s" % path

    for path in objects_artifact:
        print "PDFConverter Artifact: %s" % path

    print
    print "Counts:"
    print "StorageRetrieveError: %s" % len(objects_storage_retrieve_error)
    print "OK: %s" % len(objects_ok)
    print "PDFConverter Artifact: %s" % len(objects_artifact)


def parse_options():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':
    app = setup_app()

    options, args = parse_options()
    plone = setup_plone(app, options)

    transaction.doom()

    check_zvc_storage_for_pdfconverter_artifacts(plone, options)
