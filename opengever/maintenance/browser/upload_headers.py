from pprint import pformat
from Products.Five.browser import BrowserView


class UploadHeadersView(BrowserView):
    """
    """

    def __call__(self):
        # disable Plone's editable border
        self.request.set('disable_border', True)

        if not self.request.form.get('submitted'):
            return self.index()
        else:
            uploaded_file = self.request.form.get('uploaded_file')
            response = ''
            response += "ZPublisher.HTTPRequest.FileUpload headers: \n%s" % (
                uploaded_file.headers.dict)
            response += "\n\n\n\n\n\n"
            response += "Full Request headers: \n%s" % (
                pformat(self.request.__dict__))
            return response
