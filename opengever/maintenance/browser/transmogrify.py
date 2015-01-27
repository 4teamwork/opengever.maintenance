from collective.transmogrifier.transmogrifier import Transmogrifier
from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot
from transaction.interfaces import DoomedTransaction
import logging
import transaction


log = logging.getLogger('opengever.maintenance')


class TransmogrifyView(grok.View):
    """A view to run a transmogrifier config.
    """

    grok.name('transmogrify')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def render(self):
        cfg = self.request.form.get('cfg')
        if not cfg:
            raise Exception("Please specify 'cfg' query parameter!")

        doom = self.request.form.get('doom', False)
        if doom:
            transaction.doom()

        transmogrifier = Transmogrifier(self.context)
        transmogrifier(cfg)

        try:
            # Do an explicit commit() because we want to notice a doomed
            # transaction instead of having ZPublisher catch it silently
            transaction.commit()
        except DoomedTransaction:
            log.info("Transaction has been doomed, commit() prevented.")
        return "Done."
