from logging import getLogger
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.app.intid.interfaces import IIntIds
import argparse


def main(portal):
    intid_util = api.portal.getUtility(IIntIds)
    for brain in portal.portal_catalog.unrestrictedSearchResults():
        obj = brain.getObject()
        try:
            intid_util.getId(obj)
        except KeyError:
            path = '/'.join(obj.getPhysicalPath())
            portal_type = obj.portal_type
            created = str(obj.created())
            logger.error('IntId missing on %s of portal type %s created on %s', path, portal_type, created)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None, help='Absolute path to the Plone site')
    options = parser.parse_args(sys.argv[3:])
    app = setup_app()
    portal = setup_plone(app, options)
    main(portal)
