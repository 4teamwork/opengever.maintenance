from collective.transmogrifier.transmogrifier import Transmogrifier
from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot


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

        transmogrifier = Transmogrifier(self.context)
        transmogrifier(cfg)
        return "Done."
