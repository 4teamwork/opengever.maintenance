from AccessControl.SecurityManagement import getSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManagement import setSecurityManager
from AccessControl.User import UnrestrictedUser as BaseUnrestrictedUser
from App.config import getConfiguration
from contextlib import contextmanager
from datetime import datetime
from plone import api
from zope.globalrequest import getRequest
import logging
import os
import sys


def join_lines(fn):
    """Decorator that joins a sequence of lines with newlines.
    """
    def wrapped(self, *args, **kwargs):
        return '\n'.join(fn(self, *args, **kwargs))
    return wrapped


class UnrestrictedUser(BaseUnrestrictedUser):
    """Unrestricted user that still has an id.
    """

    def getId(self):
        """Return the ID of the user.
        """
        return self.getUserName()


@contextmanager
def elevated_privileges():
    """Temporarily elevate current user's privileges.

    See http://docs.plone.org/develop/plone/security/permissions.html#bypassing-permission-checks
    for more documentation on this code.

    Copy of opengever.base.security.elevated_privileges to make this
    functionality available for older plone versions.
    XXX can be removed eventually

    """
    old_manager = getSecurityManager()
    try:
        # Clone the current user and assign a new role.
        # Note that the username (getId()) is left in exception
        # tracebacks in the error_log,
        # so it is an important thing to store.
        tmp_user = UnrestrictedUser(
            api.user.get_current().getId(), '', ('manage', ), ''
            )

        # Wrap the user in the acquisition context of the portal
        tmp_user = tmp_user.__of__(api.portal.get().acl_users)
        newSecurityManager(getRequest(), tmp_user)

        yield
    finally:
        # Restore the old security manager
        setSecurityManager(old_manager)


class TextTable(object):
    """ class used to create a text table that can be printed to stout or to log
    for example.

    column_definitions: a string or list of python format specifiers, for example "<>"
                        for a first column left aligned and a second column right aligned

    separator:          string used to separate elements of a row

    with_title:         whether the first row is a title row. Title row will be
                        separated from data by a horizontal line
    """
    def __init__(self, column_definitions=None, separator=" | ", with_title=True):
        self.column_definitions = column_definitions
        self.separator = separator
        self.data = []
        self.with_title = with_title

    def add_row(self, row):
        self.data.append(row)

    @property
    def nrows(self):
        if len(self.data) == 0 or (self.with_title and len(self.data) == 1):
            return 0
        if self.with_title:
            return len(self.data) - 1
        return len(self.data)

    @property
    def ncols(self):
        if len(self.data) == 0:
            return 0
        return len(self.data[0])

    def calculate_column_width(self):

        self.widths = [0 for i in range(self.ncols)]
        for row in self.data:
            for i, el in enumerate(row):
                if len(str(el)) > self.widths[i]:
                    self.widths[i] = len(str(el))

    def get_format_string(self):
        self.calculate_column_width()
        frmtstr = []
        if not self.column_definitions:
            self.column_definitions = "".join("<" for i in range(self.ncols))
        for col_format, width in zip(self.column_definitions, self.widths):
            frmtstr.append("{{:{}{}}}".format(col_format, width))
        return self.separator.join(frmtstr)

    def generate_output(self, frmtstr=None):
        if len(self.data) == 0 or (self.with_title and len(self.data) == 1):
            return ""
        if frmtstr is None:
            frmtstr = self.get_format_string()
        output = ""
        start_index = 0
        if self.with_title:
            output += frmtstr.format(*self.data[0]) + "\n"
            tot_width = sum(self.widths) + (len(self.widths) - 1) * len(self.separator)
            output += "{{:->{}}}\n".format(tot_width).format("")
            start_index = 1
        return output + "\n".join(frmtstr.format(*row) for row in self.data[start_index:])

    def write_csv(self, file):
        frmtstr = " , ".join("{}" for i in range(self.ncols))
        for row in self.data:
            file.write(frmtstr.format(*row)+"\n")


class LogFilePathFinder(object):

    def __init__(self):
        self.root_logger = logging.root

    def get_logfile_path(self, filename_basis, add_timestamp=True, extension="log"):
        log_dir = self.get_logdir()
        filename = filename_basis
        if add_timestamp:
            ts = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
            filename += "-{}".format(ts)
        filename += ".{}".format(extension)
        return os.path.join(log_dir, filename)

    def get_logdir(self):
        """Determine the log directory.
        This will be derived from Zope2's EventLog location, in order to not
        have to figure out the path to var/log/ ourselves.
        """
        zconf = getConfiguration()
        eventlog = getattr(zconf, 'eventlog', None)

        if eventlog is None:
            self.root_logger.error('')
            self.root_logger.error(
                "Couldn't find eventlog configuration in order to determine "
                "logfile location - aborting!")
            self.root_logger.error('')
            sys.exit(1)

        handler_factories = eventlog.handler_factories
        eventlog_path = handler_factories[0].section.path
        assert eventlog_path.endswith('.log')
        log_dir = os.path.dirname(eventlog_path)
        return log_dir
