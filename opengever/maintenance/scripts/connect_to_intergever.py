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
        "administrator_group": "ACL-SVC-GEVER-KTSG-TESTGEVERADMIN-RW-GS",
        "connected_admin_units": [
            {
                "plone_site_id": "digs",
                "limited_admin_group": "ACL-SVC-GEVER-TestDIGS-Administratoren-GS",
                "intergever_group": "ACL-SVC-GEVER-TestDIGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-TestDIGS-Benutzer-GS",
                "notification_group": "ACL-SVC-GEVER-TestDIGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "stasg",
                "limited_admin_group": "ACL-SVC-GEVER-TestSTASG-Administratoren-GS",
                "intergever_group": "ACL-SVC-GEVER-TestSTASG-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-TestSTASG-Benutzer-GS",
                "notification_group": "ACL-SVC-GEVER-TestSTASG-INTER-GEVER-RW-GS",
            },
        ],
    },
    "sgprod": {
        "gever_base_url": "https://gever.sg.ch",
        "intergever_url": "https://intergever.sg.ch",
        "administrator_group": "ACL-SVC-GEVER-KTSG-GEVERADMIN-RW-GS",
        "connected_admin_units": [
            {
                "plone_site_id": "abb",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-ABB-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-ABB-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-ABB-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-ABB-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afdl",
                "limited_admin_group": "ACL-SVC-GEVER-FD-AFDL-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-AFDL-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-AFDL-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-AFDL-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afgb",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFGB-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFGB-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFGB-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFGB-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afhn",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFHN-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFHN-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFHN-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFHN-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afku",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFKU-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFKU-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFKU-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFKU-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afmz",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-AFMZ-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-AFMZ-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-AFMZ-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-AFMZ-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afso",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFSO-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFSO-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFSO-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFSO-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "afu",
                "limited_admin_group": "ACL-SVC-GEVER-BUD-AFU-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BUD-AFU-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BUD-AFU-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BUD-AFU-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "agve",
                "limited_admin_group": "ACL-SVC-GEVER-GD-AGVE-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GD-AGVE-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GD-AGVE-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GD-AGVE-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "agvo",
                "limited_admin_group": "ACL-SVC-GEVER-GD-AGVO-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GD-AGVO-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GD-AGVO-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GD-AGVO-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "ahs",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-AHS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-AHS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-AHS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-AHS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "ajv",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-AJV-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-AJV-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-AJV-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-AJV-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "ams",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-AMS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-AMS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-AMS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-AMS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "anjf",
                "limited_admin_group": "ACL-SVC-GEVER-VD-ANJF-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-ANJF-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-ANJF-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-ANJF-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "aoev",
                "limited_admin_group": "ACL-SVC-GEVER-VD-AOEV-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-AOEV-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-AOEV-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-AOEV-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "arch",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFKUARCH-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFKUARCH-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFKUARCH-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFKUARCH-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "areg",
                "limited_admin_group": "ACL-SVC-GEVER-BD-AREG-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BD-AREG-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BD-AREG-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BD-AREG-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "asp",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-ASP-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-ASP-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-ASP-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-ASP-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "avs",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-AVS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-AVS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-AVS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-AVS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "avsv",
                "limited_admin_group": "ACL-SVC-GEVER-GD-AVSV-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GD-AVSV-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GD-AVSV-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GD-AVSV-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "awa",
                "limited_admin_group": "ACL-SVC-GEVER-VD-AWA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-AWA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-AWA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-AWA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "awe",
                "limited_admin_group": "ACL-SVC-GEVER-BUD-AWE-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BUD-AWE-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BUD-AWE-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BUD-AWE-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "bdgs",
                "limited_admin_group": "ACL-SVC-GEVER-BUD-BUDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BUD-BUDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BUD-BUDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BUD-BUDGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "bldgs",
                "limited_admin_group": "ACL-SVC-GEVER-BLD-BLDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BLD-BLDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BLD-BLDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BLD-BLDGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "digs",
                "limited_admin_group": "ACL-SVC-GEVER-DI-DIGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-DIGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-DIGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-DIGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "dip",
                "limited_admin_group": "ACL-SVC-GEVER-FD-DIP-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-DIP-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-DIP-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-DIP-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "egov",
                "limited_admin_group": "ACL-SVC-GEVER-EGOVSG-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-EGOVSG-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-EGOVSG-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-EGOVSG-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "fdgs",
                "limited_admin_group": "ACL-SVC-GEVER-FD-FDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-FDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-FDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-FDGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "fsg",
                "limited_admin_group": "ACL-SVC-GEVER-SK-FSG-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SK-FSG-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SK-FSG-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SK-FSG-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "gdgs",
                "limited_admin_group": "ACL-SVC-GEVER-GD-GDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GD-GDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GD-GDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GD-GDGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "gergs",
                "limited_admin_group": "ACL-SVC-GEVER-GER-GERGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GER-GERGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GER-GERGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GER-GERGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "hba",
                "limited_admin_group": "ACL-SVC-GEVER-BUD-HBA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BUD-HBA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BUD-HBA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BUD-HBA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kaa",
                "limited_admin_group": "ACL-SVC-GEVER-GD-KAA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-GD-KAA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-GD-KAA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-GD-KAA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kapo",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-KAPO-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-KAPO-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-KAPO-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-KAPO-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kdp",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFKUKDP-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFKUKDP-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFKUKDP-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFKUKDP-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kfa",
                "limited_admin_group": "ACL-SVC-GEVER-VD-KFA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-KFA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-KFA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-KFA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kfk",
                "limited_admin_group": "ACL-SVC-GEVER-FD-KFK-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-KFK-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-KFK-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-KFK-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "konk",
                "limited_admin_group": "ACL-SVC-GEVER-DI-KONK-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-KONK-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-KONK-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-KONK-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "ksta",
                "limited_admin_group": "ACL-SVC-GEVER-FD-KSTA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-KSTA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-KSTA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-KSTA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "kufoe",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFKUKUFOE-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFKUKUFOE-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFKUKUFOE-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFKUKUFOE-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "lwa",
                "limited_admin_group": "ACL-SVC-GEVER-VD-LWA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-LWA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-LWA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-LWA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "ma",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-MA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-MA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-MA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-MA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "pa",
                "limited_admin_group": "ACL-SVC-GEVER-FD-PA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-FD-PA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-FD-PA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-FD-PA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "sjdgs",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-SJDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-SJDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-SJDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-SJDGS-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "sksk",
                "limited_admin_group": "ACL-SVC-GEVER-SK-SK-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SK-SK-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SK-SK-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SK-SK-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "sta",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-STA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-STA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-STA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-STA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "stasg",
                "limited_admin_group": "ACL-SVC-GEVER-DI-AFKUSTASG-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-AFKUSTASG-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-AFKUSTASG-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-AFKUSTASG-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "stia",
                "limited_admin_group": "ACL-SVC-GEVER-DI-STIA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-DI-STIA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-DI-STIA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-DI-STIA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "stva",
                "limited_admin_group": "ACL-SVC-GEVER-SJD-STVA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-SJD-STVA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-SJD-STVA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-SJD-STVA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "tba",
                "limited_admin_group": "ACL-SVC-GEVER-BUD-TBA-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BUD-TBA-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BUD-TBA-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BUD-TBA-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "tbagevi",
                "limited_admin_group": "ACL-SVC-GEVER-BD-TBAGEVI-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-BD-TBAGEVI-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-BD-TBAGEVI-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-BD-TBAGEVI-INTER-GEVER-RW-GS",
            },
            {
                "plone_site_id": "vdgs",
                "limited_admin_group": "ACL-SVC-GEVER-VD-VDGS-ADMIN-RW-GS",
                "intergever_group": "ACL-SVC-GEVER-VD-VDGS-INTER-GEVER-RW-GS",
                "manual_export_group": "ACL-SVC-GEVER-VD-VDGS-ALLE-RW-GS",
                "notification_group": "ACL-SVC-GEVER-VD-VDGS-INTER-GEVER-RW-GS",
            },
        ],
    },
    "walenstadtprod": {
        "gever_base_url": "https://walenstadt.onegovgever.ch",
        "intergever_url": "https://intergever-walenstadt.onegovgever.ch",
        "administrator_group": "administratoren",
        "connected_admin_units": [
            {
                "plone_site_id": "walenstadt",
                "intergever_group": "benutzer",
                "manual_export_group": "benutzer",
                "notification_group": "benutzer",
            },
        ],
    },
    "pfaefersprod": {
        "gever_base_url": "https://pfaefers.onegovgever.ch",
        "intergever_url": "https://intergever-pfaefers.onegovgever.ch",
        "administrator_group": "Administratoren",
        "connected_admin_units": [
            {
                "plone_site_id": "pfaefers",
                "intergever_group": "Eingangskorb Gemeinderatskanzlei",
                "manual_export_group": "Eingangskorb Gemeinderatskanzlei",
                "notification_group": "Eingangskorb Gemeinderatskanzlei",
            },
        ],
    },
    "quartenprod": {
        "gever_base_url": "https://quarten.onegovgever.ch",
        "intergever_url": "https://intergever-quarten.onegovgever.ch",
        "administrator_group": "quarten_admins",
        "connected_admin_units": [
            {
                "plone_site_id": "quarten",
                "intergever_group": "quarten_users",
                "manual_export_group": "quarten_users",
                "notification_group": "quarten_users",
            },
        ],
    },
    "sevelenprod": {
        "gever_base_url": "https://sevelen.onegovgever.ch",
        "intergever_url": "https://intergever-sevelen.onegovgever.ch",
        "administrator_group": "sevelen_admins",
        "connected_admin_units": [
            {
                "plone_site_id": "sevelen",
                "intergever_group": "sevelen_admins",
                "manual_export_group": "sevelen_admins",
                "notification_group": "sevelen_admins",
            },
        ],
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
