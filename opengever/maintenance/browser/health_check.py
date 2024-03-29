from ftw.solr.interfaces import ISolrConnectionManager
from Products.Five.browser import BrowserView
from zope.component import queryUtility
import json

try:
    from opengever.base.model import create_session
except ImportError:
    # opengever.core < 4.2
    from opengever.ogds.base.utils import create_session

try:
    from opengever.nightlyjobs.runner import nightly_run_within_24h
except ImportError:
    # opengever.core < 2021.19.0
    def nightly_run_within_24h():
        return True

try:
    from opengever.ogds.base.sync.import_stamp import ogds_sync_within_24h
except ImportError:
    # opengever.core < 2021.19.0
    def ogds_sync_within_24h():
        return True

try:
    from opengever.ogds.base.sync.import_stamp import get_ogds_sync_stamp
except ImportError:
    # opengever.core < 2021.19.0
    def get_ogds_sync_stamp():
        return None

try:
    from opengever.nightlyjobs.runner import get_nightly_run_timestamp
except ImportError:
    # opengever.core < 2021.19.0
    def get_nightly_run_timestamp():
        return None


class HealthCheckView(BrowserView):
    """Health check view to be used by superlance httpok plugin and/or
    HAProxy to determine whether an instance is in a healthy state.

    WARNING: Keep this cheap, because
    1) it will be called regularly
    2) it's protected with zope2.Public -> accessible for Anonymous users
    """

    def __call__(self):
        extended = bool(self.request.form.get('extended'))

        # Access the session in order to trigger a possible
        # 'MySQL server has gone away' error
        session = create_session()
        session.execute('SELECT 1')

        result = dict(status='OK')

        if extended:
            nightly_ok = nightly_run_within_24h()
            ogds_ok = ogds_sync_within_24h()
            solr_ping_resp = self.ping_solr()
            overall_ok = nightly_ok and ogds_ok and solr_ping_resp.is_ok()

            # Instance is considered healthy once we reach this point
            instance_status = 'healthy'

            ogds_status = 'healthy' if ogds_ok else 'unhealthy'
            nightly_status = 'healthy' if nightly_ok else 'unhealthy'
            solr_status = 'healthy' if solr_ping_resp.is_ok() else 'unhealthy'
            overall_status = 'healthy' if overall_ok else 'unhealthy'

            last_ogds_sync = get_ogds_sync_stamp()
            if last_ogds_sync:
                last_ogds_sync = last_ogds_sync.isoformat()

            last_nightly_run = get_nightly_run_timestamp()
            if last_nightly_run:
                last_nightly_run = last_nightly_run.isoformat()

            result = {
                'status': overall_status,
                'instance': {
                    'instance_status': instance_status,
                },
                'ogds': {
                    'ogds_sync_status': ogds_status,
                    'last_ogds_sync': last_ogds_sync,
                },
                'nightly_jobs': {
                    'nightly_jobs_status': nightly_status,
                    'last_nightly_run': last_nightly_run,
                },
                'solr': {
                    'solr_ping_status': solr_status,
                    'solr_ping_details': solr_ping_resp.error_msg(),
                }
            }

        return json.dumps(result)

    def ping_solr(self):
        manager = queryUtility(ISolrConnectionManager)
        return manager.connection.get('/admin/ping')
