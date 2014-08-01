from argparse import HelpFormatter
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.pdf_conversion.helpers import DocumentCollector
from opengever.maintenance.pdf_conversion.helpers import get_status
from opengever.maintenance.pdf_conversion.helpers import PDFConverter
from opengever.maintenance.profile_version import get_profile_version
from opengever.maintenance.profile_version import set_profile_version
import argparse
import random
import sys
import transaction


class Command(object):
    """
    Base class for commands to be executed by a zopectl.command
    entry point handler.
    """

    def __init__(self, app, args):
        """
        app
            The Zope Application Root object.
        args
            Any additional arguments that were passed on the command line.
        """
        self.app = app
        self.args = args

        arg_parser = self._build_arg_parser()
        self.options = arg_parser.parse_args(self.args)

    def _build_arg_parser(self):
        """
        Builds a default argument parser taking no arguments.
        Subclasses may extend this method to accept custom arguments.
        """
        prog = "%s" % sys.argv[0]

        # Top level parser
        formatter = lambda prog: HelpFormatter(prog, max_help_position=30)
        parser = argparse.ArgumentParser(prog=prog,
                                         formatter_class=formatter)
        return parser

    def run(self):
        raise NotImplementedError


class ConvertMissingPDFsCmd(Command):
    """Render PDF previews for documents that don't have one yet
    """

    def _build_arg_parser(self):
        parser = super(ConvertMissingPDFsCmd, self)._build_arg_parser()

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

        return parser

    def run(self):
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


def convert_missing_pdfs_cmd(app, args):
    cmd = ConvertMissingPDFsCmd(app, args)
    cmd.run()


class GetProfileVersionCmd(Command):
    """
    Displays the DB version of a particular GS profile.

    Usage example:
    # bin/instance get_profile_version foo.bar:default
    """

    def _build_arg_parser(self):
        parser = super(GetProfileVersionCmd, self)._build_arg_parser()

        parser.add_argument(
            'profile_id',
            help="Profile ID (example: foo.bar:default)")

        return parser

    def run(self):
        site = setup_plone(self.app)

        version = get_profile_version(site, self.options.profile_id)
        print "Version for profile '{}': {}".format(self.options.profile_id,
                                                    repr(version))


def get_profile_version_cmd(app, args):
    cmd = GetProfileVersionCmd(app, args)
    cmd.run()


class SetProfileVersionCmd(Command):
    """
    Sets the DB version of a particular GS profile.

    Usage example:
    # bin/instance set_profile_version foo.bar:default 1
    """

    def _build_arg_parser(self):
        parser = super(SetProfileVersionCmd, self)._build_arg_parser()

        parser.add_argument(
            'profile_id',
            help="Profile ID (example: foo.bar:default)")

        parser.add_argument(
            'version',
            help="DB version to set the profile to.")

        return parser

    def run(self):
        site = setup_plone(self.app)
        set_profile_version(site,
                            self.options.profile_id,
                            self.options.version)
        transaction.commit()


def set_profile_version_cmd(app, args):
    cmd = SetProfileVersionCmd(app, args)
    cmd.run()
