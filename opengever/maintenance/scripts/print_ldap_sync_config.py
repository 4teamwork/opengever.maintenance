"""
This scripts helps creating the configuration file for the ldapsync.
It will print a list of source and target groups which can be copy/pasted
into an ldapsync config file
(https://github.com/4teamwork/ldapsync/blob/master/ldapsync.example.yaml)

    bin/instance run print_ldap_sync_config.py "ou=Groups,ou=Dev,ou=OneGovGEVER,dc=4teamwork,dc=ch"
"""

from opengever.ogds.base.interfaces import ILDAPSearch
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from Products.LDAPMultiPlugins.interfaces import ILDAPMultiPlugin
from opengever.ogds.models.org_unit import OrgUnit
from opengever.maintenance.debughelpers import setup_option_parser
import sys


def _ldap_plugins(portal):
    ldap_plugins = []
    for item in portal['acl_users'].objectValues():
        if ILDAPMultiPlugin.providedBy(item):
            ldap_plugins.append(item)
    return ldap_plugins


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    if not len(args) == 1:
        print "Missing argument, please provide a target_base_dn"
        sys.exit(1)

    target_base_dn = args[0]

    app = setup_app()
    plone = setup_plone(app)
    plugins = _ldap_plugins(plone)

    config_string = "  - source_group_dn: {}\n    target_group_dn: CN={},{}"
    config = []
    for orgunit in OrgUnit.query.all():
        results = []
        for plugin in plugins:
            ldap_userfolder = plugin._getLDAPUserFolder()
            ldap_util = ILDAPSearch(ldap_userfolder)

            results = ldap_util.search(base_dn=ldap_userfolder.groups_base, search_filter=u"cn={}".format(orgunit.users_group.groupid), attrs=["CN"])
            if results:
                break

        group_cn = orgunit.users_group.groupid
        if len(results) == 0:
            print "\nCould not find group {} for {}".format(group_cn, orgunit.unit_id)
            print "Skipping\n"
            continue
        elif len(results) == 0:
            print "Found multiple results for group {} for {}".format(group_cn, orgunit.unit_id)
            print "Skipping\n"
            continue
        group_dn = results[0][0]
        config.append(config_string.format(group_dn, group_cn, target_base_dn))

    print "\n".join(config)


if __name__ == '__main__':
    main()
