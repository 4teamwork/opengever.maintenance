from argparse import HelpFormatter
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.pdf_conversion.helpers import DocumentCollector
from opengever.maintenance.pdf_conversion.helpers import get_status
from opengever.maintenance.pdf_conversion.helpers import PDFConverter
import argparse
import random
import sys


class ConvertMissingPDFs(object):
    def __init__(self, app, args):
        self.app = app
        self.args = args

    def _parse_args(self):
        prog = "%s" % sys.argv[0]

        # Top level parser
        formatter = lambda prog: HelpFormatter(prog, max_help_position=30)
        parser = argparse.ArgumentParser(prog=prog,
                                         formatter_class=formatter)

        # Global arguments
        parser.add_argument(
            '-q',
            '--quiet',
            action='store_true',
            help='Only output bare minimum.')

        parser.add_argument(
            '-l',
            '--list',
            action='store_true',
            help="Only list documents missing a PDF (don't convert).")

        parser.add_argument(
            '-n',
            '--num_docs',
            type=int,
            default=50,
            help="Number of documents to be converted to PDF.")

        return parser.parse_args(self.args)

    def run(self):
        self.options = self._parse_args()
        site = setup_plone(self.app)
        self.collector = DocumentCollector(site)

        c = self.collector
        if not self.options.quiet:
            print "SUMMARY"
            print "======="
            print "Total Documents: %s" % len(c.all_docs())
            print "Documents with file: %s" % len(c.docs_with_file())
            print "Documents missing PDF: %s" % len(c.docs_missing_pdf())
            print

        if self.options.list:
            # List only
            self.list_docs()
        else:
            # Queue conversion jobs
            self.convert_docs()

    def list_docs(self):
        """List documents that should have a preview PDF, but don't.
        """
        for doc in self.collector.docs_missing_pdf():
            status = get_status(doc)
            print doc, status

    def convert_docs(self):
        """Convert a random batch documents to PDF.
        """
        # Select `num_docs` random documents to be converted
        docs_missing_pdf = self.collector.docs_missing_pdf()
        random.shuffle(docs_missing_pdf)
        docs = docs_missing_pdf[:self.options.num_docs]

        # Queue conversion jobs
        converter = PDFConverter()

        print "Queueing conversion jobs for %s docs...\n" % len(docs)

        for doc in docs:
            result = converter.queue_conversion_job(doc)
            if result == 'SUCCESS':
                msg = "Queued job for %s.\n" % doc
            else:
                msg = "Queueing job for %s failed: %s\n" % (doc, result)
            print msg

        print "Done.\n"


def convert_missing_pdfs(app, args):
    """
    zopectl.command entry point handler.

    app
        The Zope Application Root object.
    args
        Any additional arguments that were passed on the command line.
    """

    cmd = ConvertMissingPDFs(app, args)
    cmd.run()