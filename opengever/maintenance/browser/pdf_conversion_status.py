from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot


class PDFConversionStatusView(grok.View):
    """A view to list pending PDF preview conversions.
    """

    grok.name('pdf-conversion-status')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def update(self):
        # disable Plone's editable border
        self.request.set('disable_border', True)
        super(PDFConversionStatusView, self).update()

    def get_overview_stats(self):
        missing = 0
        ready = 0
        converting = 0

        doc_infos = self.get_doc_infos()
        for info in doc_infos:
            if info['state'] == "ready":
                ready += 1
            elif info['state'] == "converting":
                converting += 1
            else:
                missing += 1
        total = missing + converting + ready
        stats = dict(missing=missing,
                     ready=ready,
                     converting=converting,
                     total=total)
        return stats

    def get_doc_infos(self):
        """Returns a list of dicts with information about conversion
        states of documents.
        """
        catalog = self.context.portal_catalog
        docs = catalog(portal_type='opengever.document.document')
        objs = [brain.getObject() for brain in docs]
        docs_with_file = [o for o in objs if o.file is not None]

        # Local imports to avoid startup failure when grokking package on
        # setups without opengever.pdfconverter installed
        from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_CONVERTING
        from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_READY
        from opengever.pdfconverter.behaviors.preview import IPreview

        doc_infos = []
        for doc in docs_with_file:
            url = doc.absolute_url()
            title = doc.Title()

            state = IPreview(doc).conversion_state
            if state == CONVERSION_STATE_READY:
                state = "ready"
            elif state == CONVERSION_STATE_CONVERTING:
                state = "converting"
            else:
                state = "missing"

            info = dict(url=url, title=title, state=state)
            doc_infos.append(info)

        return doc_infos

    def get_pending_docs(self):
        docs = self.get_doc_infos()
        return [d for d in docs if d['state'] in ('converting', 'missing')]
