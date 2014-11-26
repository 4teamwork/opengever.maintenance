from AccessControl.interfaces import IRoleManager
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import defaultKeys
from collective.transmogrifier.utils import Matcher
from zope.interface import classProvides
from zope.interface import implements


class ChangeLocalRolesSection(object):
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.transmogrifier = transmogrifier
        self.name = name
        self.options = options
        self.previous = previous
        self.context = transmogrifier.context

        if 'path-key' in options:
            pathkeys = options['path-key'].splitlines()
        else:
            pathkeys = defaultKeys(options['blueprint'], name, 'path')
        self.pathkey = Matcher(*pathkeys)

        if 'local-roles-key' in options:
            roleskeys = options['local-roles-key'].splitlines()
        else:
            roleskeys = defaultKeys(options['blueprint'], name, 'ac_local_roles')
        self.roleskey = Matcher(*roleskeys)

    def __iter__(self):
        for item in self.previous:
            pathkey = self.pathkey(*item.keys())[0]
            roleskey = self.roleskey(*item.keys())[0]

            if not pathkey or not roleskey or \
               roleskey not in item:    # not enough info
                yield item; continue

            obj = self.context.unrestrictedTraverse(item[pathkey].lstrip('/'), None)
            if obj is None:             # path doesn't exist
                yield item; continue

            usernames = []

            if IRoleManager.providedBy(obj):
                for principal, roles in item[roleskey].items():
                    usernames.append(principal)
                    if roles:
                        obj.manage_setLocalRoles(principal, roles)
                        obj.reindexObjectSecurity()

            # Make sure that users which do not appear in extraLocalCoordinators
            # will have their roles cleared
            for username, roles in obj.get_local_roles():
                if not username in usernames:
                    obj.manage_delLocalRoles([username])

            obj.reindexObjectSecurity()

            yield item
