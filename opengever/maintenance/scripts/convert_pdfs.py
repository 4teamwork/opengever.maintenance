from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_CONVERTING
from opengever.pdfconverter.behaviors.preview import IPreview
from opengever.pdfconverter.interfaces import IPDFConverterSettings
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
import random
import sys
import transaction


try:
    from collections import Counter
except ImportError:
    from opengever.maintenance.utils import Counter


# Print a warning message if more than batch_size * WARNING_FACTOR PDFs need
# to be converted
WARNING_FACTOR = 5
SEPARATOR = '-' * 78

MSG_TOTAL = 'Total Documents: %i'
MSG_OK = 'Docs OK: %i'
MSG_PENDING = 'Docs with conversion pending: %i'
MSG_NO_FILE = 'Docs without a file: %i'
MSG_NO_CONVERSION_NEEDED = 'Docs with no conversion needed: %i'
MSG_PDF_MISSING = 'Docs with missing PDF: %i'


class PDFConversionManager(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.counter = Counter()

    def queue_conversion_job(self, brain):
        """Queue a conversion job for a particular document.
        """
        if self.options.verbose:
            print "Queueing conversion job..."
        conversion_view = self.portal.restrictedTraverse(
            '%s/pdfconversion' % '/'.join(brain.getPath().split('/')[2:]))
        self.portal.REQUEST.form.update({'convert': '1'})
        conversion_view()
        transaction.commit()
        self.counter['conversion_queued'] += 1
        if self.options.verbose:
            print "Conversion job queued."

    def print_stats(self, brains):
        if self.options.verbose:
            print MSG_TOTAL % len(brains)
            print MSG_OK % self.counter['ok']
            print MSG_PENDING % self.counter['pending']
            print MSG_NO_FILE % self.counter['no_file']
            print MSG_NO_CONVERSION_NEEDED % self.counter['no_conversion_needed']
            print MSG_PDF_MISSING % self.counter['pdf_missing']
            print SEPARATOR

        print "%s docs need to be converted to PDF." % self.counter['pdf_missing']
        print "%s conversion jobs queued." % self.counter['conversion_queued']

        if self.counter['pdf_missing'] > WARNING_FACTOR * self.options.batch_size:
            sys.stderr.write('WARNING: %s: %s docs need to be converted to PDF.\n' %
                    (self.portal.id.upper(), self.counter['pdf_missing']))
        print SEPARATOR

    def convert_pdfs(self):
        """Queue PDF conversion jobs for as many documents (that don't have a
        preview PDF yet) as specified by the --batch-size option.
        """

        brains = self.portal.portal_catalog(
                                portal_type="opengever.document.document")
        # Randomize order of documents so in case of failures the conversion
        # doesn't get stuck with the same set of documents every time.
        brains = list(brains)
        random.shuffle(brains)

        if self.options.verbose:
            print "Checking %s documents for preview PDFs..." % len(brains)
            print SEPARATOR

        registry = getUtility(IRegistry)
        settings = registry.forInterface(IPDFConverterSettings)

        for brain in brains:
            doc = brain.getObject()
            if doc.file:
                if IPreview(doc).preview_file:
                    self.counter['ok'] += 1
                elif IPreview(doc).conversion_state == CONVERSION_STATE_CONVERTING:
                    self.counter['pending'] += 1
                else:
                    filename = doc.file.filename
                    file_ext = filename[filename.rfind('.') + 1:]
                    if file_ext not in settings.types_to_convert:
                        self.counter['no_conversion_needed'] += 1
                    else:
                        self.counter['pdf_missing'] += 1
                        if self.options.verbose:
                            filename = doc.file.filename.encode('utf-8').ljust(45)
                            conversion_state = str(IPreview(doc).conversion_state).ljust(5)
                            url = doc.absolute_url().ljust(50)
                            print 'PDF missing: %s | PDF STATUS: %s | %s' % (
                                filename,
                                conversion_state,
                                url)

                        if self.options.convert and self.counter['conversion_queued'] < self.options.batch_size:
                            self.queue_conversion_job(brain)

            else:
                self.counter['no_file'] += 1

        self.print_stats(brains)


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-b", "--batch-size", dest="batch_size", type="int", default=100)
    parser.add_option("-C", "--convert", action="store_true", dest="convert", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    manager = PDFConversionManager(plone, options)
    manager.convert_pdfs()

if __name__ == '__main__':
    main()











































