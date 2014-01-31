from collections import Counter
from five import grok
from opengever.maintenance.pdf_conversion.helpers import DocumentCollector
from opengever.maintenance.pdf_conversion.helpers import get_status
from Products.CMFPlone.interfaces import IPloneSiteRoot
from opengever.maintenance.pdf_conversion.helpers import reset_conversion_status


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

    def __call__(self):
        form = self.request.form
        if form.get('reset_conversion_status'):
            return self.reset_conversion_status()
        else:
            # Render template
            return super(PDFConversionStatusView, self).__call__()

    @property
    def collector(self):
        if not hasattr(self, '_document_collector'):
            self._document_collector = DocumentCollector(self.context)
        return self._document_collector

    def overview_stats(self):
        c = self.collector
        counter = Counter()

        counter['total_docs'] = len(c.all_docs())

        counter['docs_with_file'] = len(c.docs_with_file())
        counter['docs_without_file'] = len(c.docs_without_file())
        counter['conversion_required'] = len(c.non_pdf_docs())
        counter['conversion_not_required'] = len(c.pdf_docs())

        counter['converted'] = len(c.converted_docs())
        counter['converting'] = len(c.converting_docs())
        counter['failed'] = len(c.failed_docs())
        counter['not_converted'] = len(c.not_converted_docs())

        counter['docs_missing_pdf'] = len(c.docs_missing_pdf())
        return counter

    def outstanding_docs(self):
        doc_infos = []
        for doc in self.collector.docs_missing_pdf():
            url = doc.absolute_url()
            title = doc.Title()[:70]
            status = get_status(doc)

            info = dict(url=url, title=title, status=status)
            doc_infos.append(info)
        return doc_infos

    def reset_conversion_status(self):
        docs = self.collector.converting_docs()
        self.request.response.write(
            "Resetting conversion status for %s docs...\n" % len(docs))
        reset_conversion_status(docs)
        self.request.response.write("Done.")

