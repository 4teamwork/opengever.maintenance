"""
Script to generate a report of archival PDFs.

    bin/instance run archival_pdf_report.py
"""

from opengever.maintenance.archival_pdf_checker import ArchivalPDFChecker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
import logging
import transaction


root_logger = logging.root


class ArchivalPDFReporter(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options

        self.context = portal
        if options.path is not None:
            self.context = self.portal.unrestrictedTraverse(options.path)

    def run(self):
        checker = ArchivalPDFChecker(self.context)
        checker.run()
        print checker.render_result_tables()


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("--path", default=None)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    reporter = ArchivalPDFReporter(plone, options)
    reporter.run()
