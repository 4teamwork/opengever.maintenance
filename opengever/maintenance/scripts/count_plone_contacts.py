from opengever.contact.contact import IContact
from opengever.ogds.base.utils import get_current_admin_unit
from plone import api


catalog = api.portal.get_tool('portal_catalog')
results = catalog.unrestrictedSearchResults(object_provides=IContact.__identifier__)

print("{}: {}".format(get_current_admin_unit().unit_id, len(results)))
