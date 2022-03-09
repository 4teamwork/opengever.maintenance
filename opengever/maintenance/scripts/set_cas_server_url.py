"""
Set the server CAS URL

Example Usage:

    bin/instance run set_cas_server_url.py https://dev.onegovgever.ch/portal/cas
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
import argparse
import sys
import transaction


def set_cas_url(plone, args):
    url = args.cas_url
    plone.acl_users.cas_auth.cas_server_url = url
    print("Set cas server URL {}".format(url))


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("cas_url", help="cas server url")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    set_cas_url(plone, args)
    transaction.commit()
