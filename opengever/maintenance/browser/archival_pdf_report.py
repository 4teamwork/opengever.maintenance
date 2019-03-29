from Products.Five.browser import BrowserView
from opengever.maintenance.archival_pdf_checker import ArchivalPDFChecker


class ArchivalPDFReport(BrowserView):

    def __call__(self):
        checker = ArchivalPDFChecker(self.context)
        checker.run()
        return checker.render_result_tables()
