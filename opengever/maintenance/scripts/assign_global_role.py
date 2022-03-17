"""
Globally assigns a given role to a principal.

Example Usage:

    bin/instance run assign_global_role.py <role> <principal>
    bin/instance run assign_global_role.py 'PropertySheetsManager' 'some-group-id'
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import argparse
import sys
import transaction


def assign_gobal_role(plone, args):
    role = args.role
    principal = args.principal

    acl_users = api.portal.get_tool("acl_users")
    role_manager = acl_users.portal_role_manager
    valid_roles = role_manager.validRoles()

    if role not in valid_roles:
        raise Exception("Role %r not found. Valid roles are: %r" % (role, valid_roles))

    role_manager.assignRoleToPrincipal(role, principal)
    print("Assigned role %r to principal %r" % (role, principal))


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("role", help="Role Name")
    parser.add_argument("principal", help="Principal ID")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    assign_gobal_role(plone, args)
    transaction.commit()
