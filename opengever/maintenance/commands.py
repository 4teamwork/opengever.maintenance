from argparse import HelpFormatter
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.profile_version import get_profile_version
from opengever.maintenance.profile_version import set_profile_version
import argparse
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
