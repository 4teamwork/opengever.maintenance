from datetime import datetime
from five import grok
from opengever.document.checkout.manager import ICheckinCheckoutManager
from plone.locking.interfaces import IRefreshableLockable
from Products.CMFPlone.interfaces import IPloneSiteRoot
from zope.component import getMultiAdapter


def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


class LockMaintenanceView(grok.View):
    """A view to list current WebDAV locks.
    """

    grok.name('lock_maintenance')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def get_lock_infos(self):
        results = []
        catalog = self.context.portal_catalog

        docs = catalog(portal_type='opengever.document.document')
        for doc in docs:
            obj = doc.getObject()
            lockable = IRefreshableLockable(obj)
            lock_info = lockable.lock_info()
            if not lock_info == []:
                infos = {}
                infos['title'] = obj.Title()
                infos['url'] = obj.absolute_url()
                # Ignoring multiple locks for now
                infos['token'] = lock_info[0]['token']
                infos['creator'] = lock_info[0]['creator']
                lock_time = datetime.fromtimestamp(lock_info[0]['time'])
                duration = datetime.now() - lock_time
                infos['time'] = lock_time.strftime("%Y-%m-%d %H:%M:%S")
                infos['duration'] = strfdelta(duration, "{days}d {hours}h {minutes}m {seconds}s")
                infos['type'] = lock_info[0]['type']

                manager = getMultiAdapter((obj, obj.REQUEST),
                                              ICheckinCheckoutManager)
                checked_out = manager.checked_out()
                infos['checked_out'] = checked_out

                results.append(infos)

        return results
