from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot
import json

try:
    from opengever.base.model import create_session
except ImportError:
    # opengever.core < 4.2
    from opengever.ogds.base.utils import create_session


class HealthCheckView(grok.View):
    """Health check view to be used by superlance httpok plugin and/or
    HAProxy to determine whether an instance is in a healthy state.

    WARNING: Keep this cheap, because
    1) it will be called regularly
    2) it's protected with zope.Public -> accessible for Anonymous users
    """

    grok.name('health-check')
    grok.context(IPloneSiteRoot)
    grok.require('zope2.Public')

    def render(self):
        # Access the session in order to trigger a possible
        # 'MySQL server has gone away' error
        session = create_session()
        session.execute('SELECT 1')

        result = dict(status='OK')
        return json.dumps(result)
