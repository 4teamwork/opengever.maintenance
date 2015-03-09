from five import grok
from plone import api
from Products.CMFPlone.interfaces import IPloneSiteRoot
from zope.component.hooks import getSite
from zope.interface import Interface


def get_principals_from_local_roles(obj):
    local_roles = obj.get_local_roles()
    principals = [assignment[0] for assignment in local_roles]
    return principals


def get_principals_from_role_manager():
    site = getSite()
    role_manager = site.acl_users.portal_role_manager
    principal_roles = list(role_manager._principal_roles.items())
    principals = [assignment[0] for assignment in principal_roles]
    return principals


def get_all_role_principals(context):
    path = '/'.join(context.getPhysicalPath())
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(path=path)

    all_principals = set()
    for brain in brains:
        obj = brain.getObject()
        principals = get_principals_from_local_roles(obj)
        all_principals.update(principals)

    if IPloneSiteRoot.providedBy(context):
        # Invoked on Plone site - include local roles of Plone site in
        # search since the Plone site isn't catalogued
        site = context
        principals = get_principals_from_local_roles(site)
        all_principals.update(principals)

    # Include principals from portal_role_manager
    principals = get_principals_from_role_manager()
    all_principals.update(principals)
    return all_principals


class ListRolePrincipalsView(grok.View):
    """Lists all the unique principals that are used in role assignments
    (below the adapted context).

    Also includes the Plone site (if called on site root) and any principals
    from global role assignments in portal_role_manager.
    """

    grok.name('list-role-principals')
    grok.context(Interface)
    grok.require('cmf.ManagePortal')

    def render(self):
        all_principals = get_all_role_principals(self.context)
        result = '\n'.join(sorted(p for p in all_principals))
        return result
