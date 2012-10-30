from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_CONVERTING
from opengever.pdfconverter.behaviors.preview import IPreview
from opengever.pdfconverter.interfaces import IPDFConverterSettings
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
import sys
import transaction


try:
    from collections import Counter
except ImportError:
    from opengever.maintenance.utils import Counter

SEPARATOR = '-' * 78
# Print a warning message if more than batch_size * WARNING_FACTOR PDFs need
# to be converted
WARNING_FACTOR = 5

def convert_pdfs(portal, options):
    """Queue PDF conversion jobs for as many documents that don't have a preview
    PDF yet as specified by the --batch-size option. 
    """

    brains = portal.portal_catalog(portal_type="opengever.document.document")

    if options.verbose:
        msg = "Checking %s documents for preview PDFs..." % len(brains)
        print msg
        print '-' * len(msg)

    counter = Counter()

    registry = getUtility(IRegistry)
    settings = registry.forInterface(IPDFConverterSettings)

    for brain in brains:
        doc = brain.getObject()
        if doc.file:
            if IPreview(doc).preview_file:
                counter['ok'] += 1
            elif IPreview(doc).conversion_state == CONVERSION_STATE_CONVERTING:
                counter['pending'] += 1
            else:
                filename = doc.file.filename
                file_ext = filename[filename.rfind('.') + 1:]
                if file_ext not in settings.types_to_convert:
                    counter['no_conversion_needed'] += 1
                else:
                    counter['pdf_missing'] += 1
                    if options.verbose:
                        filename = doc.file.filename.encode('utf-8').ljust(45)
                        conversion_state = str(IPreview(doc).conversion_state).ljust(5)
                        url = doc.absolute_url().ljust(50)
                        print 'PDF missing: %s | PDF STATUS: %s | %s' % (
                            filename,
                            conversion_state,
                            url)

                    if options.convert and counter['conversion_queued'] < options.batch_size:
                        # Queue a conversion job
                        if options.verbose:
                            print "start manually converting ..."
                        conversion_view = portal.restrictedTraverse(
                            '%s/pdfconversion' % '/'.join(brain.getPath().split('/')[2:]))
                        portal.REQUEST.form.update({'convert': '1'})
                        conversion_view()
                        transaction.commit()
                        counter['conversion_queued'] += 1
                        if options.verbose:
                            print "Manually converting started."

        else:
            counter['no_file'] += 1

    if options.verbose:
        print SEPARATOR
        print 'Total Documents: %i' %(len(brains))
        print 'Total Docs OK: %i' %(counter['ok'])
        print 'Total Docs with conversion pending: %i' %(counter['pending'])
        print 'Total Docs without a file: %i' %(counter['no_file'])
        print 'Total Docs with no conversion needed: %i' %(counter['no_conversion_needed'])
        print 'Total Docs with missing PDF: %i' %(counter['pdf_missing'])
    else:
        print counter['pdf_missing']

    if counter['pdf_missing'] > WARNING_FACTOR * options.batch_size:
        sys.stderr.write('WARNING: %s PDFs total need to be converted.\n' %
                         counter['pdf_missing'])


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-b", "--batch-size", dest="batch_size", type="int", default=100)
    parser.add_option("-C", "--convert", action="store_true", dest="convert", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    convert_pdfs(plone, options)

if __name__ == '__main__':
    main()











































