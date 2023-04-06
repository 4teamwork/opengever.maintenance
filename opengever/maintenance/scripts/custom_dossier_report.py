from ftw.solr.interfaces import ISolrSearch
from ftw.solr.query import make_filters
from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.base.reporter import XLSReporter
from opengever.base.solr import OGSolrContentListingObject
from opengever.base.solr import OGSolrDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.browser.report import DossierReporter
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from os.path import join as pjoin
from zope.component import getUtility
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
import argparse
import os
import sys


QUERY = dict(
    object_provides=IDossierMarker.__identifier__
)

COLUMNS = [
    'title',
    'reference',
    'review_state',
    'dossier_type_label',
    'responsible_fullname',
    'sequence_number',
    'gsNumber_custom_field_string',
    'bgdsNumber_custom_field_string',
]

MAX_ROWS = 30000


class CustomDossierReporter(DossierReporter):

    filename = 'custom_dossier_report.xlsx'

    column_settings = DossierReporter.column_settings + (
        {
            'id': 'dossier_type_label',
            'title': 'Dossier-Typ',
        },
        {
            'id': 'sequence_number',
            'title': 'Dossier-ID',
        },
    )


class CustomDossierReportGenerator(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.request = getRequest()
        self.solr = getUtility(ISolrSearch)
        alsoProvides(self.request, IOpengeverBaseLayer)

    def get_objs(self):
        filters = make_filters(**QUERY)
        response = self.solr.search(
            filters=filters, sort='path asc', rows=MAX_ROWS)
        solr_docs = [OGSolrDocument(d) for d in response.docs]
        return [OGSolrContentListingObject(d) for d in solr_docs]

    def run(self):
        self.request.form['columns'] = COLUMNS
        reporter_view = CustomDossierReporter(self.portal, self.request)
        columns = reporter_view.columns()
        objs = self.get_objs()

        reporter = XLSReporter(
            self.request,
            columns,
            objs,
            field_mapper=reporter_view.fields,
        )

        data = reporter()
        if not data:
            raise Exception('Failed to produce report')

        out_path = pjoin(os.getcwd(), reporter_view.filename)

        with open(out_path, 'wb') as outfile:
            outfile.write(data)

        print()
        print('Custom report saved to %s' % out_path)


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')

    options = parser.parse_args(sys.argv[3:])

    plone = setup_plone(app, options)

    generator = CustomDossierReportGenerator(plone, options)
    generator.run()
