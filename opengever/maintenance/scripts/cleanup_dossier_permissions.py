from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.task import ITask
from plone import api
import transaction


DISREGARDED_ROLES = [
    "Administrator", "Authenticated", "Role Manager",
    "Records Manager", "Member", "Owner"]

SEPERATOR = 100 * "="


class DossierPermissionCleaner(object):
    """A maintenance object handler, which checks and simplifies the
    local roles of dossiers with a blocked permission inheritance.

    Used in SG, after the DIGS migration.
    """

    same_permission = []
    different_permission = []

    def __init__(self):
        self.catalog = api.portal.get_tool('portal_catalog')

    def get_dossiers_with_blocked_permissions(self):
        brains = self.catalog(object_provides=IDossierMarker.__identifier__)

        for brain in brains:
            dossier = brain.getObject()
            local_roles_blocked = getattr(dossier, '__ac_local_roles_block__', False)

            if local_roles_blocked:
                yield dossier

    def check(self):
        self.same_permission = []
        self.different_permission = []

        for dossier in self.get_dossiers_with_blocked_permissions():
            if dossier.is_subdossier():
                raise Exception(
                    'Subdossier with blocked permission: {}'.format(dossier))

            repo = aq_parent(aq_inner(dossier))
            dossier_roles = self.get_local_roles_mapping(dossier)

            diff = {}
            for principal, roles in dossier_roles.items():
                repo_roles = self.get_roles_on_obj_for_principal(
                    repo, principal)

                if roles != repo_roles:
                    diff[principal] = (roles, repo_roles)

            info  = {
                'obj': dossier,
                'dossier_roles': dossier_roles,
                'repo_roles': repo_roles,
                'diff': diff}

            if diff:
                self.different_permission.append(info)
            else:
                self.same_permission.append(info)

    def adjust_dossier_permission(self):
        stats = []
        print SEPERATOR
        print 'Fixing dossiers with same permission'
        print SEPERATOR

        for item in self.same_permission:
            info = self._adjust_dossier_permission(item['obj'])
            self.log_dossier(info)
            stats.append(info)

        print SEPERATOR
        print 'Fixing dossiers with different permission'
        print SEPERATOR

        for item in self.different_permission:
            info = self._adjust_dossier_permission(item['obj'])
            self.log_dossier(info)
            stats.append(info)

        return stats

    def log_dossier(self, info):
        obj = info.get('obj')
        print u'{} - {}'.format(obj.title, u'/'.join(obj.getPhysicalPath()))
        for principal, roles in info.get('roles', []):
            print '   {}: {}'.format(principal, ', '.join(roles))

        if info.get('warning'):
            print '   {}'.format(info.get('warning'))

        print ""

    def _adjust_dossier_permission(self, dossier):
        """Readd permisssion inheritance and remove local roles.
        """

        tasks = self.catalog(
            object_provides=ITask.__identifier__,
            path='/'.join(dossier.getPhysicalPath()))

        self.remove_block_inheritance_flag(dossier)

        if tasks:
            stats = {'obj': dossier,
                     'warning': u'Local roles not cleared, dossier contains tasks.'}
        else:
            stats = self.clear_local_roles(dossier)

        dossier.reindexObjectSecurity()
        return stats

    def remove_block_inheritance_flag(self, dossier):
        dossier.__ac_local_roles_block__ = False

    def clear_local_roles(self, dossier):
        removed = []
        for principal, roles in dossier.get_local_roles():
            # skip local roles set on users
            if not api.group.get(principal):
                continue

            dossier.manage_delLocalRoles(principal)
            removed.append((principal, roles))

        return {'obj': dossier, 'roles': removed}

    def get_roles_on_obj_for_principal(self, obj, principal):
        try:
            roles = api.group.get_roles(groupname=principal, obj=obj)
            return tuple(role for role in roles if role not in DISREGARDED_ROLES)

        except:
            print 'Skipped principal {} - it is a user'.format(principal)

        return ()

    def get_local_roles_mapping(self, obj):
        local_roles = {}
        for principal, roles in obj.get_local_roles():
            roles = tuple(role for role in roles if role not in DISREGARDED_ROLES)

            if roles:
                local_roles[principal] = roles

        return local_roles


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    cleaner = DossierPermissionCleaner()
    cleaner.check()

    stats = cleaner.adjust_dossier_permission()
    transaction.commit()


if __name__ == '__main__':
    main()
