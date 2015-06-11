"""
Compatibility module to help support all relevant versions of opengever.core
"""


def get_site_url():
    """Compatibility function to retrieve the `site_url` for the current
    admin_unit (>= 3.0) or client (< 3.0).
    """
    try:
        # >= 3.0
        from opengever.ogds.base.utils import get_current_admin_unit
        site_url = get_current_admin_unit().site_url
    except ImportError:
        # 2.7 - 2.9
        from opengever.ogds.base.utils import get_client_id
        from opengever.ogds.base.interfaces import IContactInformation
        from zope.component import getUtility

        client_id = get_client_id()
        info = getUtility(IContactInformation)
        client = info.get_client_by_id(client_id)
        site_url = client.site_url

    return site_url
