from five import grok
from zope.interface import Interface


class PdbBreakpointView(grok.View):
    """A view to trigger a pdb breakpoint.
    """

    grok.name('pdb-breakpoint')
    grok.context(Interface)
    grok.require('cmf.ManagePortal')


    def render(self):
        import pdb; pdb.set_trace( )
        pass
        return "Done"
