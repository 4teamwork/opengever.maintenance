from five import grok
from pprint import pformat
from Products.CMFPlone.interfaces import IPloneSiteRoot
from Products.Five.browser.pagetemplatefile import ViewPageTemplateFile


class UploadHeadersView(grok.View):
    """
    """

    form_template = ViewPageTemplateFile('upload_headers.pt')

    grok.name('upload-headers')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def render(self):
        if not self.request.form.get('submitted'):
            html = self.form_template()
            return html
        else:
            uploaded_file = self.request.form.get('uploaded_file')
            response = ''
            response += "ZPublisher.HTTPRequest.FileUpload headers: \n%s" % (
                uploaded_file.headers.dict)
            response += "\n\n\n\n\n\n"
            response += "Full Request headers: \n%s" % (
                pformat(self.request.__dict__))
            return response

    def update(self):
        # disable Plone's editable border
        self.request.set('disable_border', True)
        super(UploadHeadersView, self).update()

