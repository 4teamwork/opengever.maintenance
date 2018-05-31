from opengever.base.interfaces import ISearchSettings
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import queryMultiAdapter
import argparse
import logging
import sys


logger = logging.getLogger('reindex_subject_index')
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


def reindex_subject_index():
    solr_enabled = api.portal.get_registry_record(
        name='use_solr', interface=ISearchSettings)
    if not solr_enabled:
        raise Exception('Solr is not enabled.')

    portal = api.portal.get()
    solr_maintenance = queryMultiAdapter(
        (portal, portal.REQUEST), name=u'solr-maintenance')
    solr_maintenance.reindex(idxs=['Subject'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()
    setup_plone(app, options)

    reindex_subject_index()


if __name__ == '__main__':
    main()
