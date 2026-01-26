import csv
import sys
from plone import api
from opengever.maintenance import dm
from opengever.repository.interfaces import IRepositoryFolder
from opengever.dossier.businesscase import IBusinessCaseDossier
from opengever.document.behaviors import IBaseDocument
from ftw.solr.interfaces import ISolrSearch
from zope.component import getUtility
from ftw.solr.query import make_filters
import json

BASE_URL = 'https://example.com'

dm()

def main():
    writer = csv.writer(sys.stdout, delimiter=';')

    writer.writerow([
        'ordnungsposition_name',
        'ordnungsposition_url',
        'referenz_nr',
        'anzahl_dossiers',
        'anzahl_dossiers_inkl_subdossiers',
        'anzahl_dokumente',
        'datenvolumen_mb',
    ])

    catalog = api.portal.get_tool('portal_catalog')

    repo_brains = catalog.unrestrictedSearchResults(
        object_provides=IRepositoryFolder.__identifier__,
        sort_on='reference',
    )

    for repo_brain in repo_brains:
        solr = getUtility(ISolrSearch)
        repo_path = repo_brain.getPath()

        # main_dossier_brains = catalog(object_provides=IBusinessCaseDossier.__identifier__,
        #                               path=repo_path,
        #                               is_subdossier=False)

        main_dossiers_qury = {
            'path': {'query': repo_path, 'depth': -1},
            'object_provides': IBusinessCaseDossier.__identifier__,
            'is_subdossier': False
        }

        total_main_dossiers = solr.search(
            filters=make_filters(**main_dossiers_qury),
            rows=0,
        ).num_found

        all_dossiers_qury = {
            'path': {'query': repo_path, 'depth': -1},
            'object_provides': IBusinessCaseDossier.__identifier__,
        }

        total_dossiers = solr.search(
            filters=make_filters(**all_dossiers_qury),
            rows=0,
        ).num_found

        document_query = {'path': {'query': repo_path, 'depth': -1},
                          'object_provides': IBaseDocument.__identifier__}

        resp = solr.search(
            filters=make_filters(**document_query),
            rows=0,
            facet=True,
            **{'json.facet': json.dumps({'filesize': 'sum(filesize)'})})

        total_docs = resp.num_found
        filesize = resp.get('facets').get('filesize', 0)
        filesize_mb = round(filesize / 1024 / 1024, 1)

        writer.writerow([
            repo_brain.Title,
            repo_brain.getURL().replace(plone.absolute_url(), BASE_URL),
            repo_brain.reference,
            total_main_dossiers,
            total_dossiers,
            total_docs,
            filesize_mb,
        ])


if __name__ == '__main__':
    main()
