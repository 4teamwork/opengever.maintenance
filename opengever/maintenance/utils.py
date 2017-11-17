from AccessControl.SecurityManagement import getSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManagement import setSecurityManager
from AccessControl.User import UnrestrictedUser as BaseUnrestrictedUser
from contextlib import contextmanager
from plone import api
from zope.globalrequest import getRequest
import os
import subprocess


def join_lines(fn):
    """Decorator that joins a sequence of lines with newlines.
    """
    def wrapped(self, *args, **kwargs):
        return '\n'.join(fn(self, *args, **kwargs))
    return wrapped


def get_rss():
    """Get current memory usage (RSS) of this process.
    """
    out = subprocess.check_output(
        ["ps", "-p", "%s" % os.getpid(), "-o", "rss"])
    try:
        return int(out.splitlines()[-1].strip())
    except ValueError:
        return 0


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
