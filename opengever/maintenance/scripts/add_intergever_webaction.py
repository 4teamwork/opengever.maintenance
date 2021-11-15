"""
Adds a webaction on Dossiers to launch eCH-0147 export via Intergever.

Example Usage:

    bin/instance run add_intergever_webaction.py sgtest
"""
from opengever.api.validation import get_validation_errors
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.webactions.exceptions import ActionAlreadyExists
from opengever.webactions.schema import IWebActionSchema
from opengever.webactions.storage import get_storage
import argparse
import sys
import transaction


CLUSTERS = {
    "sgtest": {
        "gever_base_url": "https://gevertest.sg.ch",
        "intergever_url": "https://gevertest.sg.ch/intergever",
    }
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


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("cluster", choices=CLUSTERS.keys(), help="Cluster")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    register_webaction(plone, args)
    transaction.commit()
