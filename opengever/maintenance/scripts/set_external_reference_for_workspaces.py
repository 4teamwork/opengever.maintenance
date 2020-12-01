"""
bin/instance run ./scripts/set_external_reference_for_workspaces.py path/to/file
Set linked dossier oguid as external_reference for linked workspaces.
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import json
import sys
import transaction


def set_external_reference(file_path):
    with open(file_path) as file_:
        data = json.load(file_)
    uids = data.keys()
    query = {"object_provides": "opengever.workspace.interfaces.IWorkspace"}
    catalog = api.portal.get_tool('portal_catalog')
    workspace_brains = catalog.unrestrictedSearchResults(query)
    for brain in workspace_brains:
        workspace = brain.getObject()
        if workspace.UID() in uids:
            workspace.external_reference = data[workspace.UID()]
        else:
            workspace.external_reference = u''
        workspace.reindexObject(idxs=['external_reference'])
    return len(uids), len(workspace_brains)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    if not len(args) == 1:
        print("Missing argument, please provide a path to a JSON file")
        sys.exit(1)

    file_path = args[0]
    setup_plone(app)
    nof_linked_workspaces, nof_workspaces = set_external_reference(file_path)
    transaction.commit()
    print("Done. {} of {} workspaces are linked".format(nof_linked_workspaces, nof_workspaces))


if __name__ == '__main__':
    main()
