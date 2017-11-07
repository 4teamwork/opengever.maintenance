from Products.Five.browser import BrowserView


class PdbBreakpointView(BrowserView):
    """A view to trigger a pdb breakpoint.
    """

    def __call__(self):
        import pdb; pdb.set_trace( )
        pass
        return "Done"
