from opengever.maintenance import dm
from opengever.ogds.auth.plugin import install_ogds_auth_plugin
from plone import api
import transaction

dm()

acl_users = api.portal.get().acl_users

# remove ldap plugin
if 'ldap' in acl_users:
    api.portal.get().acl_users.manage_delObjects(['ldap'])

# install ogds_auth plugin
install_ogds_auth_plugin(title="OGDS Authentication Plugin")

transaction.commit()
