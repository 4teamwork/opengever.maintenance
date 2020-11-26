"""bin/instance run ./scripts/mark_dossiers_linked_with_workspaces_and_dump_links.py
"""

from opengever.base.oguid import Oguid
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.workspaceclient.interfaces import ILinkedToWorkspace
from opengever.workspaceclient.storage import LinkedWorkspacesStorage
from plone import api
from plone.i18n.normalizer import filenamenormalizer
from zope.interface import alsoProvides
import json
import os.path
import transaction


def mark_dossiers_with_workspaces_and_dump_links(directory):
    query = {'object_provides': ['opengever.dossier.behaviors.dossier.IDossierMarker'],
             'is_subdossier': False}
    catalog = api.portal.get_tool('portal_catalog')
    dossier_brains = catalog.unrestrictedSearchResults(query)
    result = {}
    for brain in dossier_brains:
        dossier = brain.getObject()
        linked_workspaces = LinkedWorkspacesStorage(dossier).list()
        if linked_workspaces:
            dossier_oguid = Oguid.for_object(dossier).id
            alsoProvides(dossier, ILinkedToWorkspace)
            dossier.reindexObject(idxs=['object_provides'])
            for linked_workspace in linked_workspaces:
                result[linked_workspace] = dossier_oguid

    print(json.dumps(result, sort_keys=True, indent=4))
    if directory:
        dump(directory, result)


def dump(directory, result):
    filename = filenamenormalizer.normalize(get_current_admin_unit().public_url) + '.json'
    path = os.path.abspath(os.path.join(directory, filename))
    print('Dumping to {}'.format(path))
    with open(path, 'w+') as fio:
        json.dump(result, fio, sort_keys=True, indent=4)


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option('-d', '--dump-directory', dest='directory',
                      help='Path to a directory where a JSON file is created with the output.')
    (options, args) = parser.parse_args()
    setup_plone(app, options)
    mark_dossiers_with_workspaces_and_dump_links(options.directory)
    transaction.commit()
    print('Done.')


if __name__ == '__main__':
    main()
