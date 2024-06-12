"""
Connects a GEVER deployment to InterGEVER:
- Adds a webaction on Dossiers to launch eCH-0147 export via Intergever
- Ensures the 'intergever.app' service user is present with appropriate roles

Example Usage:

    bin/instance run connect_to_intergever.py sgtest

    # To update webactions only
    bin/instance run connect_to_intergever.py sgtest --jobs webactions

    # To ensure the service user is present without updating webactions:
    bin/instance run connect_to_intergever.py sgtest --jobs service_user
"""
import argparse
import string
import sys
from random import SystemRandom

import transaction
from opengever.api.validation import get_validation_errors
from opengever.webactions.exceptions import ActionAlreadyExists
from opengever.webactions.schema import IWebActionSchema
from opengever.webactions.storage import WebActionsStorage, get_storage
from plone import api
from Products.CMFPlone.utils import safe_unicode

from opengever.maintenance.debughelpers import setup_app, setup_plone

SERVICE_USER_ID = "intergever.app"

NOTIFICATION_ROLE = "PrivilegedNotificationDispatcher"

CLUSTERS = {
    "sgtest": {
        "gever_base_url": "https://gevertest.sg.ch",
        "intergever_url": "https://intergevertest.sg.ch",
    },
    "sgprod": {
        "gever_base_url": "https://gever.sg.ch",
        "intergever_url": "https://intergevertest.sg.ch",
        "groups_by_site": {
            "abb": ["ACL-SVC-GEVER-BLD-ABB-EINGANGSKORB-RW-GS"],
            "afdl": ["ACL-SVC-GEVER-FD-AFDL-EINGANGSKORB-RW-GS"],
            "afhn": ["ACL-SVC-GEVER-DI-AFHN-EINGANGSKORB-RW-GS"],
            "afku": ["ACL-SVC-GEVER-DI-AFKU-EINGANGSKORB-RW-GS"],
            "afso": ["ACL-SVC-GEVER-DI-AFSO-EINGANGSKORB-RW-GS"],
            "afu": ["ACL-SVC-GEVER-BUD-AFU-EINGANGSKORB-RW-GS"],
            "agve": ["ACL-SVC-GEVER-GD-AGVE-EINGANGSKORB-RW-GS"],
            "agvo": ["ACL-SVC-GEVER-GD-AGVO-EINGANGSKORB-RW-GS"],
            "ahs": ["ACL-SVC-GEVER-BLD-AHS-EINGANGSKORB-RW-GS"],
            "ams": ["ACL-SVC-GEVER-BLD-AMS-EINGANGSKORB-RW-GS"],
            "anjf": ["ACL-SVC-GEVER-VD-ANJF-EINGANGSKORB-RW-GS"],
            "arch": ["ACL-SVC-GEVER-DI-AFKUARCH-EINGANGSKORB-RW-GS"],
            "areg": ["ACL-SVC-GEVER-BD-AREG-EINGANGSKORB-RW-GS"],
            "asp": ["ACL-SVC-GEVER-BLD-ASP-EINGANGSKORB-RW-GS"],
            "avs": ["ACL-SVC-GEVER-BLD-AVS-EINGANGSKORB-RW-GS"],
            "avsv": ["ACL-SVC-GEVER-GD-AVSV-EINGANGSKORB-RW-GS"],
            "vdawa": ["ACL-SVC-GEVER-VD-AWA-EINGANGSKORB-RW-GS"],
            "awe": ["ACL-SVC-GEVER-BUD-AWE-EINGANGSKORB-RW-GS"],
            "bdgs": ["ACL-SVC-GEVER-BUD-BUDGS-EINGANGSKORB-BUDGS-RW-GS"],
            "bldgs": ["ACL-SVC-GEVER-BLD-BLDGS-EINGANGSKORB-GS"],
            "digs": ["ACL-SVC-GEVER-DI-DIGS-EINGANGSKORB-RW-GS"],
            "dip": ["ACL-SVC-GEVER-FD-DIP-EINGANGSKORB-RW-GS"],
            "egov": ["ACL-SVC-GEVER-EGOVSG-EINGANGSKORB-RW-GS"],
            "fdgs": ["ACL-SVC-GEVER-FD-FDGS-EINGANGSKORB-GS"],
            "gdgs": ["ACL-SVC-GEVER-GD-GDGS-EINGANGSKORB-GS"],
            "gergs": ["ACL-SVC-GEVER-GER-GERGS-EINGANGSKORB-RW-GS"],
            "hba": ["ACL-SVC-GEVER-BUD-HBA-EINGANGSKORB-RW-GS"],
            "kaa": ["ACL-SVC-GEVER-GD-KAA-EINGANGSKORB-RW-GS"],
            "kapo": ["ACL-SVC-GEVER-SJD-KAPO-EINGANGSKORB-RW-GS"],
            "kdp": ["ACL-SVC-GEVER-DI-AFKUKDP-EINGANGSKORB-RW-GS"],
            "kfk": ["ACL-SVC-GEVER-FD-KFK-EINGANGSKORB-RW-GS"],
            "ksta": ["ACL-SVC-GEVER-FD-KSTA-EINGANGSKORB-RW-GS"],
            "kufoe": ["ACL-SVC-GEVER-DI-AFKUKUFOE-EINGANGSKORB-RW-GS"],
            "lwa": ["ACL-SVC-GEVER-VD-LWA-EINGANGSKORB-RW-GS"],
            "ma": ["ACL-SVC-GEVER-SJD-MA-EINGANGSKORB-RW-GS"],
            "pa": ["ACL-SVC-GEVER-FD-PA-EINGANGSKORB-RW-GS"],
            "sjdgs": ["ACL-SVC-GEVER-SJD-SJDGS-EINGANGSKORB-GS"],
            "sksk": ["ACL-SVC-GEVER-SK-SK-EINGANGSKORB-RW-GS"],
            "sta": ["ACL-SVC-GEVER-SJD-STA-EINGANGSKORB-RW-GS"],
            "stasg": ["ACL-SVC-GEVER-DI-AFKUSTASG-EINGANGSKORB-RW-GS"],
            "stia": ["ACL-SVC-GEVER-DI-STIA-EINGANGSKORB-RW-GS"],
            "stva": ["ACL-SVC-GEVER-SJD-STVA-EINGANGSKORB-RW-GS"],
            "tba": ["ACL-SVC-GEVER-BUD-TBA-EINGANGSKORB-RW-GS"],
            "tbagevi": ["ACL-SVC-GEVER-BD-TBAGEVI-EINGANGSKORB-RW-GS"],
            "vdgs": ["ACL-SVC-GEVER-VD-VDGS-EINGANGSKORB-GS"],
            "sjdajv": ["ACL-SVC-GEVER-SJD-AJV-EINGANGSKORB-RW-GS"],
            "vdkfa":  ["ACL-SVC-GEVER-VD-KFA-EINGANGSKORB-RW-GS"],
            "dikonk":  ["ACL-SVC-GEVER-DI-KONK-EINGANGSKORB-RW-GS"],

        },
    },
    "walenstadtprod": {
        "gever_base_url": "https://walenstadt.onegovgever.ch",
        "intergever_url": "https://intergever-walenstadt.onegovgever.ch",
    },
    "pfaefersprod": {
        "gever_base_url": "https://pfaefers.onegovgever.ch",
        "intergever_url": "https://intergever-pfaefers.onegovgever.ch",
        "groups_by_site": {
            "pfaefers": ["Eingangskorb Gemeinderatskanzlei"],
        },
    },
    "quartenprod": {
        "gever_base_url": "https://quarten.onegovgever.ch",
        "intergever_url": "https://intergever-quarten.onegovgever.ch",
        "groups_by_site": {
            "quarten": ["quarten_users"],
        },
    },
    "sevelenprod": {
        "gever_base_url": "https://sevelen.onegovgever.ch",
        "intergever_url": "https://intergever-sevelen.onegovgever.ch",
        "groups_by_site": {
            "sevelen": ["sevelen_admins"],
        },
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


def register_webactions(plone, options):
    cluster_id = options.cluster
    cluster = CLUSTERS[cluster_id]

    gever_base_url = cluster["gever_base_url"].rstrip("/")
    intergever_url = cluster["intergever_url"].rstrip("/")
    groups_by_site = cluster.get("groups_by_site", {})

    actions = [
        {
            "title": u"Export eCH-0147/Inter-GEVER",
            "unique_name": u"intergever-export",
            "target_url": "%s/ech0147_export/?dossier_url=%s{path}" % (intergever_url, gever_base_url),
            "types": [u"opengever.dossier.businesscasedossier"],
        },
        {
            "title": u"Import eCH-0147",
            "unique_name": u"intergever-import",
            "target_url": "%s/inbox" % intergever_url,
            "types": [
                u"opengever.repository.repositoryfolder",
                u"opengever.dossier.businesscasedossier"
            ],
        }
    ]

    for action in actions:
        action_data = {
            u"display": "actions-menu",
            u"mode": "blank",
            u"order": 0,
            u"scope": "global",
            u"target_url": action["target_url"],
            u"title": action["title"],
            u"types": action["types"],
            u"unique_name": action["unique_name"],
        }

        groups = groups_by_site.get(api.portal.get().id, [])
        groups = map(safe_unicode, groups)
        if groups:
            action_data.update({"groups": groups})

        errors = get_validation_errors(action_data, IWebActionSchema)
        if errors:
            raise Exception("Invalid webaction: %s" % errors)

        storage = get_storage()

        try:
            new_action_id = storage.add(action_data)
            print("Webaction created with ID %s" % new_action_id)
            if groups:
                print("Restricted webaction to groups: %r" % groups)

        except ActionAlreadyExists:
            if options.update_webaction:
                unique_name = action_data.pop('unique_name')
                existing_action_id = storage._indexes[WebActionsStorage.IDX_UNIQUE_NAME][unique_name]
                storage.update(existing_action_id, action_data)
                print("Webaction with ID %s has been updated" % existing_action_id)
                if groups:
                    print("Restricted webaction to groups: %r" % groups)
            else:
                print("Webaction with unique_name %r already exists, skipped." % action["unique_name"])


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
    parser.add_argument('--update-webaction', action='store_true')
    parser.add_argument(
        '--jobs', nargs='+',
        default=['webactions', 'service_user'],
        help="Jobs to execute (options: webactions, service_user)")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    if 'webactions' in args.jobs:
        register_webactions(plone, args)

    if 'service_user' in args.jobs:
        ensure_service_user_present(plone, args)

    transaction.commit()
