"""
This script searches documents with missing filename extension
and adds the extension corresponding to the document mimetype.
    bin/instance run ./scripts/add_missing_file_extensions.py
Options:
  -n : dry run
  -p : path in which to search files
"""
from opengever.base.filename import GeverFileNameNormalizer
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable
from plone import api
import sys
import transaction


class FilenameExtensionFixer(object):

    def __init__(self, options):
        self.search_path = options.path
        self.creator = options.creator
        self.corrected = TextTable()
        self.corrected.add_row(("Path", "Filename", "Mimetype", "New filename"))
        self.skipped = TextTable()
        self.skipped.add_row(("Path", "Filename", "Mimetype"))
        self.registry = api.portal.get_tool("mimetypes_registry")
        self.normalizer = GeverFileNameNormalizer()

    def fix_extensions(self):
        for document in self.get_documents_with_missing_extension():
            self.fix_file_extension(document)

    def fix_file_extension(self, document):
        """ Add missing extension to document filename
        """
        path = document.absolute_url_path()
        filename = document.get_filename()

        mimetype = None
        try:
            mimetype = document.get_mimetype()[0]
            extension = self.registry.lookup(mimetype)[0].extensions[0]
            if extension == "pjpg":
                if "jpg" in self.registry.lookup(mimetype)[0].extensions:
                    extension = "jpg"
            new_filename = u"{}.{}".format(filename, extension)
            document.get_file().filename = new_filename
            self.corrected.add_row((path, filename, mimetype, new_filename))
        except Exception as err:
            print("failed to correct document at {}".format(path))
            print(repr(err))
            self.skipped.add_row((path, filename, mimetype))

    def get_documents_with_missing_extension(self):
        """ Search for documents with missing filename extensions.
        """
        query = {"portal_type": ['opengever.document.document', 'ftw.mail.mail'],
                 "path": self.search_path}
        if self.creator:
            query["Creator"] = self.creator
        document_brains = api.content.find(**query)

        for brain in document_brains:
            document = brain.getObject()
            if not document.has_file():
                continue
            filename = document.get_filename()
            extension = self.normalizer.split_filename_extension(filename)[1]
            if not extension:
                yield document


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False,
                      help="do not commit changes")
    parser.add_option("-p", "--path", action="store",
                      dest="path", type="string", default="",
                      help="restrict search to given path")
    parser.add_option("--creator", action="store",
                      dest="creator", type="string", default="",
                      help="restrict search to documents created by the given user")
    (options, args) = parser.parse_args()

    if not len(args) == 0:
        print "Not expecting any argument"
        sys.exit(1)

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    if options.path:
        print "restricting search to {}".format(options.path)

    if options.creator:
        print "restricting search to documents created by {}".format(options.creator)

    app = setup_app()
    setup_plone(app)

    extension_fixer = FilenameExtensionFixer(options)
    extension_fixer.fix_extensions()

    sys.stdout.write("\n\nTable of all corrected documents:\n")
    sys.stdout.write(extension_fixer.corrected.generate_output())
    sys.stdout.write("\n\nTable of all skipped documents :\n")
    sys.stdout.write(extension_fixer.skipped.generate_output())
    sys.stdout.write("\n\nSummary:\n")
    if options.dryrun:
        sys.stdout.write("Would correct {} documents\n\n".format(extension_fixer.corrected.nrows))
        sys.stdout.write("Would skip {} documents\n\n".format(extension_fixer.skipped.nrows))
    else:
        sys.stdout.write("Corrected {} documents\n\n".format(extension_fixer.corrected.nrows))
        sys.stdout.write("Skipped {} documents\n\n".format(extension_fixer.skipped.nrows))

    log_filename = LogFilePathFinder().get_logfile_path(
        'add_missing_file_extensions_corrected', extension="csv")
    with open(log_filename, "w") as logfile:
        extension_fixer.corrected.write_csv(logfile)

    log_filename = LogFilePathFinder().get_logfile_path(
        'add_missing_file_extensions_skipped', extension="csv")
    with open(log_filename, "w") as logfile:
        extension_fixer.skipped.write_csv(logfile)

    if not options.dryrun:
        sys.stdout.write("committing ...\n")
        transaction.commit()

    sys.stdout.write("done.\n")


if __name__ == '__main__':
    main()
