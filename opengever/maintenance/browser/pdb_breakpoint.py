from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot


class PdbBreakpointView(grok.View):
    """A view to trigger a pdb breakpoint.
    """

    grok.name('pdb-breakpoint')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')


    def render(self):
        import pdb; pdb.set_trace( )
        pass
        return "Done"
