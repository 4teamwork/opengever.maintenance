"""
Connects a GEVER deployment to InterGEVER:
- Adds a webaction on Dossiers to launch eCH-0147 export via Intergever
- Ensures the 'intergever.app' service user is present with appropriate roles

Example Usage:

    bin/instance run connect_to_intergever.py sgtest
"""
from opengever.api.validation import get_validation_errors
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.webactions.exceptions import ActionAlreadyExists
from opengever.webactions.schema import IWebActionSchema
from opengever.webactions.storage import get_storage
from plone import api
from random import SystemRandom
import argparse
import string
import sys
import transaction


SERVICE_USER_ID = "intergever.app"

NOTIFICATION_ROLE = "PrivilegedNotificationDispatcher"

CLUSTERS = {
    "sgtest": {
        "gever_base_url": "https://gevertest.sg.ch",
        "intergever_url": "https://gevertest.sg.ch/intergever",
    },
    "oggdev": {
        "gever_base_url": "https://dev.onegovgever.ch/",
        "intergever_url": "https://igdev.onegovgever.ch/intergever",
    },
    "local": {
        "gever_base_url": "http://localhost:8080/",
        "intergever_url": "http://localhost:3000/intergever",
    },
}


def register_webaction(plone, options):
    cluster_id = options.cluster
    cluster = CLUSTERS[cluster_id]

    gever_base_url = cluster["gever_base_url"].rstrip("/")
    intergever_url = cluster["intergever_url"].rstrip("/")

    target_url = "%s/ech0147_export/?dossier_url=%s{path}" % (
        intergever_url,
        gever_base_url,
    )

    title = u"eCH-0147 Export via InterGEVER"
    unique_name = u"intergever-export"

    action_data = {
        u"display": "actions-menu",
        u"mode": "blank",
        u"order": 0,
        u"scope": "global",
        u"target_url": target_url,
        u"title": title,
        u"types": [u"opengever.dossier.businesscasedossier"],
        u"unique_name": unique_name,
    }

    errors = get_validation_errors(action_data, IWebActionSchema)
    if errors:
        raise Exception("Invalid webaction: %s" % errors)

    storage = get_storage()

    try:
        new_action_id = storage.add(action_data)
        print("Webaction created with ID %s" % new_action_id)
    except ActionAlreadyExists:
        print("Webaction with unique_name %r already exists, skipped." % unique_name)


def random_password():
    rand = SystemRandom()
    chars = string.ascii_letters + string.digits
    pw = "".join(rand.choice(chars) for i in range(32))
    return pw


def ensure_service_user_present(plone, options):
    uf = api.portal.get_tool("acl_users")
    user_manager = uf["source_users"]

    if SERVICE_USER_ID not in user_manager.getUserIds():
        user_manager.addUser(SERVICE_USER_ID, SERVICE_USER_ID, random_password())
        print("Created InterGEVER service user %r" % SERVICE_USER_ID)
    else:
        print("InterGEVER service user %r already exists" % SERVICE_USER_ID)

    role_manager = uf.portal_role_manager
    valid_roles = role_manager.validRoles()
    if NOTIFICATION_ROLE not in valid_roles:
        raise Exception(
            "Role %r not found! Is your GEVER deployment up to date? "
            "(At least version 2022.7 is required)" % NOTIFICATION_ROLE
        )

    service_user = api.user.get(SERVICE_USER_ID)
    existing_roles = role_manager.getRolesForPrincipal(service_user)

    roles_to_assign = [NOTIFICATION_ROLE]
    for role in roles_to_assign:
        if role not in existing_roles:
            role_manager.assignRoleToPrincipal(role, SERVICE_USER_ID)
            print("Assigned role %r to InterGEVER service user" % role)

        else:
            print("InterGEVER service user already has role %r assigned" % role)


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", choices=CLUSTERS.keys(), help="Cluster")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    register_webaction(plone, args)
    ensure_service_user_present(plone, args)
    transaction.commit()
