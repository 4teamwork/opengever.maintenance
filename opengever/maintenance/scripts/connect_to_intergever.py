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
from opengever.webactions.storage import WebActionsStorage
from opengever.webactions.storage import get_storage
from plone import api
from Products.CMFPlone.utils import safe_unicode

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone

SERVICE_USER_ID = "intergever.app"

NOTIFICATION_ROLE = "PrivilegedNotificationDispatcher"

CLUSTERS = {
    "sgtest": {
        "gever_base_url": "https://gevertest.sg.ch",
        "intergever_url": "https://intergevertest.sg.ch",
    },
    "sgprod": {
        "gever_base_url": "https://gever.sg.ch",
        "intergever_url": "https://intergever.sg.ch",
        "groups_by_site": {
            "abb": ["ACL-SVC-GEVER-BLD-ABB-EINGANGSKORB-RW-GS"],
            "afdl": ["ACL-SVC-GEVER-FD-AFDL-EINGANGSKORB-RW-GS"],
            "afgb": ["ACL-SVC-GEVER-DI-AFGB-EINGANGSKORB-RW-GS"],
            "afhn": ["ACL-SVC-GEVER-DI-AFHN-EINGANGSKORB-RW-GS"],
            "afku": ["ACL-SVC-GEVER-DI-AFKU-EINGANGSKORB-RW-GS"],
            "afmz": ["ACL-SVC-GEVER-SJD-AFMZ-EINGANGSKORB-RW-GS"],
            "afso": ["ACL-SVC-GEVER-DI-AFSO-EINGANGSKORB-RW-GS"],
            "afu": ["ACL-SVC-GEVER-BUD-AFU-EINGANGSKORB-RW-GS"],
            "agve": ["ACL-SVC-GEVER-GD-AGVE-EINGANGSKORB-RW-GS"],
            "agvo": ["ACL-SVC-GEVER-GD-AGVO-EINGANGSKORB-RW-GS"],
            "ahs": ["ACL-SVC-GEVER-BLD-AHS-EINGANGSKORB-RW-GS"],
            "ams": ["ACL-SVC-GEVER-BLD-AMS-EINGANGSKORB-RW-GS"],
            "anjf": ["ACL-SVC-GEVER-VD-ANJF-EINGANGSKORB-RW-GS"],
            "aoev": ["ACL-SVC-GEVER-VD-AOEV-EINGANGSKORB-RW-GS"],
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
            "fsg": ["ACL-SVC-GEVER-Fachstelle_Gever-Eingangskorb-GS"],
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
        "intergever_url": "http://localhost:3333/",
        "administrators_group": "gever_admins",
        "connected_admin_units": [
            {
                "plone_site_id": "fd",
                "intergever_group": "FD-Alle",
                "manual_export_group": "FD-Alle",
                "limited_admin_group": "FD-Admin",
                "notification_group": "FD-Alle",
            },
            {
                "plone_site_id": "ska",
                "intergever_group": "SKA-Alle",
                "manual_export_group": "SKA-Alle",
                "limited_admin_group": "SKA-Alle",
                "notification_group": "SKA-Alle",
            },
        ],
    },
}


def get_unit_config(connected_admin_units, plone_site_id):
    try:
        unit_config = [
            au for au in connected_admin_units
            if au["plone_site_id"] == plone_site_id
        ][0]
    except IndexError:
        raise Exception("No connected admin unit found for site %r" % plone_site_id)
    return unit_config


def register_webactions(plone, options):
    cluster_id = options.cluster
    cluster = CLUSTERS[cluster_id]

    gever_base_url = cluster["gever_base_url"].rstrip("/")
    intergever_url = cluster["intergever_url"].rstrip("/")
    connected_admin_units = cluster.get("connected_admin_units", {})

    actions = [
        {
            "title": u"eCH-0147 Export via Inter-GEVER",
            "unique_name": u"intergever-export",
            "target_url": "%s/ech0147_export/connector?dossier_url=%s{path}" % (intergever_url, gever_base_url),
            "types": [u"opengever.dossier.businesscasedossier"],
            "order": 0,
        },
        {
            "title": u"eCH-0147 Export",
            "unique_name": u"intergever-manual-export",
            "target_url": "%s/ech0147_export/manual?dossier_url=%s{path}" % (intergever_url, gever_base_url),
            "types": [u"opengever.dossier.businesscasedossier"],
            "order": 1,
        },
        {
            "title": u"eCH-0147 Import",
            "unique_name": u"intergever-import",
            "target_url": "%s/inbox" % intergever_url,
            "types": [
                u"opengever.repository.repositoryfolder",
                u"opengever.dossier.businesscasedossier"
            ],
            "order": 2,
        }
    ]

    for action in actions:
        action_name = action["unique_name"]
        action_data = {
            u"display": "actions-menu",
            u"mode": "blank",
            u"order": action["order"],
            u"scope": "global",
            u"target_url": action["target_url"],
            u"title": action["title"],
            u"types": action["types"],
            u"unique_name": action_name,
        }

        plone_site_id = api.portal.get().id
        unit_config = get_unit_config(connected_admin_units, plone_site_id)

        if action_name == "intergever-export":
            groups = [unit_config["intergever_group"]]

        elif action_name == "intergever-manual-export":
            groups = [unit_config["manual_export_group"]]

        elif action_name == "intergever-import":
            groups = [
                unit_config["intergever_group"],
                unit_config["manual_export_group"],
            ]

        groups.append(unit_config.get("limited_admin_group", []))
        groups.append(cluster.get("administrators_group", []))

        groups = list(set(groups))
        groups = map(safe_unicode, groups)
        groups.sort()

        action_data.update({"groups": groups})

        errors = get_validation_errors(action_data, IWebActionSchema)
        if errors:
            raise Exception("Invalid webaction: %s" % errors)

        storage = get_storage()

        try:
            new_action_id = storage.add(action_data)
            print("Webaction %s created with ID %s" % (action_name, new_action_id))
            if groups:
                print("Restricted webaction %r to groups: %r" % (action_name, groups))

        except ActionAlreadyExists:
            if options.update_webaction:
                unique_name = action_data.pop('unique_name')
                existing_action_id = storage._indexes[WebActionsStorage.IDX_UNIQUE_NAME][unique_name]
                storage.update(existing_action_id, action_data)
                print("Webaction %r with ID %s has been updated" % (action_name, existing_action_id))
                if groups:
                    print("Restricted webaction %r to groups: %r" % (action_name, groups))
            else:
                print("Webaction with unique_name %r already exists, skipped." % action["unique_name"])


def random_password():
    rand = SystemRandom()
    chars = string.ascii_letters + string.digits
    pw = "".join(rand.choice(chars) for i in range(32))
    return pw


def has_service_user():
    uf = api.portal.get_tool("acl_users")
    user_manager = uf["source_users"]

    return SERVICE_USER_ID in user_manager.getUserIds()


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
    parser.add_argument('-s', '--site-root')
    parser.add_argument("cluster", choices=CLUSTERS.keys(), help="Cluster")
    parser.add_argument('--update-webaction', action='store_true')
    parser.add_argument(
        '--jobs', nargs='+',
        default=['webactions', 'service_user'],
        help="Jobs to execute (options: webactions, service_user)")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app(), options=args)

    if 'webactions' in args.jobs:
        if 'service_user' not in args.jobs and not has_service_user():
            print("No service user registered for this deployment: %s" % api.portal.get().id)
        else:
            register_webactions(plone, args)

    if 'service_user' in args.jobs:
        ensure_service_user_present(plone, args)

    transaction.commit()
