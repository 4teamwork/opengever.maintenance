"""
We noticed objects where the local roles are not in sync with the role assignment manager storage.

This script will check all dossiers and fix it by reappling the local roles based on the role assignment manager storage.

See: https://4teamwork.atlassian.net/browse/TI-2096

Example usage:

    bin/instance0 run src/opengever.maintenance/opengever/maintenance/scripts/sync_local_roles_with_role_assignment_manager.py
"""
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance import dm
from opengever.maintenance.scripts.item_processor import Processor
from plone import api

dm()

catalog = api.portal.get_tool('portal_catalog')


def process_item(brain):
    obj = brain.getObject()
    manager = RoleAssignmentManager(obj)
    local_roles_before = obj.get_local_roles()
    manager._update_local_roles(reindex=False)
    if local_roles_before != obj.get_local_roles():
        print("Found broken object: {}".format(obj.absolute_url()))


Processor().run(
    catalog.unrestrictedSearchResults(object_provides=IDossierMarker.__identifier__),
    batch_size=1000, process_item_method=process_item)
