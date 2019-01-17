from opengever.base.oguid import Oguid
from opengever.base.role_assignments import RoleAssignmentManager
from plone import api
from zope.annotation.interfaces import IAnnotatable


class LocalRolesAssignmentMigration(object):
    """The current local roles migration from ftw.usermigration does
    not handle role assigments correctly as we do in GEVER. So we provide
    our own local roles migration instead.
    """

    def __init__(self, portal, request):
        self.portal = portal
        self.request = request

    def execute(self, principal_mapping, mode):
        migrator = RoleAssignmentsMigrator(self.portal, principal_mapping,
                                mode=mode, strict=True)
        results = migrator.migrate()
        return results


class RoleAssignmentsMigrator(object):
    def __init__(self, portal, principal_mapping, mode='move', strict=True):
        self.portal = portal
        self.principal_mapping = principal_mapping
        self.mode = mode

        self.strict = strict
        self.catalog = api.portal.get_tool('portal_catalog')

        # Statistics
        self.moved = []
        self.copied = []
        self.deleted = []

    def migrate_and_recurse(self, context):
        if not IAnnotatable.providedBy(context):
            return

        path = '/'.join(context.getPhysicalPath())
        assigment_manager = RoleAssignmentManager(context)

        changed = False
        for assigment in assigment_manager.storage._storage():
            for old_id, new_id in self.principal_mapping.items():
                if assigment['principal'] == old_id:
                    changed = True
                    if self.mode == 'move':
                        assigment['principal'] = new_id
                        self.moved.append((path, old_id, new_id))
                    elif self.mode == 'copy':
                        if assigment['reference']:
                            reference = Oguid.parse(
                                assigment['reference']).resolve_object()
                        else:
                            reference = None

                        assigment_manager.storage.add_or_update(
                            new_id, assigment['roles'], assigment['cause'], reference)
                        self.copied.append((path, old_id, new_id))
                    elif self.mode == 'delete':
                        assigment_manager.storage.clear(assigment)
                        self.deleted.append((path, old_id, None))

        if changed:
            assigment_manager._update_local_roles()

        for obj in context.objectValues():
            self.migrate_and_recurse(obj)

    def migrate(self):
        self.migrate_and_recurse(self.portal)
        return {
            'role_assignments': {
                'moved': self.moved,
                'copied': self.copied,
                'deleted': self.deleted},
        }
