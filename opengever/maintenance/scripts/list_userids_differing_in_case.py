"""
This script checks for users that are present in OGDS and LDAP with IDs
that differ ONLY by case and lists them in a mapping.

(This is used to produce the mapping for migrating users that changed casing,
e.g. mixed case -> lowercase user IDs.)
"""

from opengever.base.model import create_session
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.interfaces import ILDAPSearch
from opengever.ogds.base.interfaces import IOGDSUpdater
from opengever.ogds.models.user import User
from pprint import pprint


def case_insensitive_get(seq, key):
    matches = [item for item in seq if item.lower() == key.lower()]
    assert len(matches) in (0, 1)
    if matches:
        return matches[0]


def generate_usermigration_mapping(plone):
    """Produce mapping for the migration of users that changed casing.
    """
    # Get LDAP users
    ogds_updater = IOGDSUpdater(plone)
    ldap_plugins = ogds_updater._ldap_plugins()
    ldap_userids = []
    for plugin in ldap_plugins:
        ldap_userfolder = plugin._getLDAPUserFolder()
        uid_attr = ogds_updater._get_uid_attr(ldap_userfolder)

        ldap_util = ILDAPSearch(ldap_userfolder)
        ldap_users = ldap_util.get_users()
        ldap_userids.extend([u[1][uid_attr] for u in ldap_users])

    # Get OGDS users
    session = create_session()
    users = session.query(User)

    mapping = {}

    # Find users that exist with different case in LDAP
    for user in users:
        existing_userid = user.userid
        if existing_userid not in ldap_userids:
            match = case_insensitive_get(ldap_userids, existing_userid)
            if match:
                # User from OGDS exists with a different ID in LDAP that
                # ONLY differs by case
                mapping[existing_userid] = match

    print
    print "Mapping of users that exist with different case (OGDS -> LDAP):"
    print
    pprint(mapping)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    plone = setup_plone(app, options)

    generate_usermigration_mapping(plone)


if __name__ == '__main__':
    main()
