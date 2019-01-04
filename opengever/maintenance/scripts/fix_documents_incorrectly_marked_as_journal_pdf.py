"""
This script searches documents that were wronlgy assigned the
IDossierJournalPDFMarker interface. We search for all documents
providing that interface and check that they have the expected title
for a journal pdf. If not, we remove the marker interface.

    bin/instance run ./scripts/fix_documents_incorrectly_marked_as_journal_pdf.py

Options:
  -n : dry run
"""
from opengever.document.interfaces import IDossierJournalPDFMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from plone import api
from zope.interface import noLongerProvides
import sys
import transaction


def remove_journal_pdf_interface(document):
    """ Remove the IDossierJournalPDFMarker from a document.
    """
    noLongerProvides(document, IDossierJournalPDFMarker)
    document.reindexObject(idxs=['object_provides'])


def get_incorrectly_marked_documents():
    """ Search for documents providing the IDossierJournalPDFMarker interface
    but not having the expected title for a dossier journal PDF.
    """
    journal_pdf_brains = api.content.find(portal_type='opengever.document.document',
                                          object_provides=IDossierJournalPDFMarker)

    for brain in journal_pdf_brains:
        document = brain.getObject()
        dossier = document.get_parent_dossier()
        title_fr = "Journal du dossier {}".format(dossier.title)
        title_de = "Dossier Journal {}".format(dossier.title)
        title_en = "Journal of dossier {}".format(dossier.title)
        if (brain.Title.startswith(title_fr) or
                brain.Title.startswith(title_de) or
                brain.Title.startswith(title_en)):
            continue
        yield document
    return


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 0:
        print "Not expecting any argument"
        sys.exit(1)

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    app = setup_app()
    setup_plone(app)

    table = TextTable()
    table.add_row(("Title", "Path"))

    for document in get_incorrectly_marked_documents():
        remove_journal_pdf_interface(document)
        table.add_row((document.title, document.absolute_url_path()))

    sys.stdout.write("\n\nTable of all documents to correct:\n")
    sys.stdout.write(table.generate_output())
    sys.stdout.write("\n\nSummary:\n")
    if options.dryrun:
        sys.stdout.write("Would correct {} documents\n\n".format(len(table.data) - 1))
    else:
        sys.stdout.write("Corrected {} documents\n\n".format(len(table.data) - 1))

    log_filename = LogFilePathFinder().get_logfile_path(
        'fix_documents_incorrectly_marked_as_journal_pdf', extension="csv")
    with open(log_filename, "w") as logfile:
        table.write_csv(logfile)

    if not options.dryrun:
        sys.stdout.write("committing ...\n")
        transaction.commit()

    sys.stdout.write("done.\n")


if __name__ == '__main__':
    main()
