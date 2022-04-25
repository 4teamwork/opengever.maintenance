"""
Makes a repository read-only by recursively modifying local roles so that
only read only access is retained.

Example Usage:

    bin/instance run make_repository_readonly.py 'ordnungssystem1'


Arguments:
    <repo_id>   ID of the repository to set read only

Options:
    --dry-run   Dry run
    --verify    Verify that View permission didn't change (slow!)
"""
from logging import Formatter
from logging import getLogger
from logging import INFO
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.base import DOSSIER_STATES_CLOSED
from opengever.dossier.base import DOSSIER_STATES_OPEN
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.meeting.proposal import IProposal
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task import CLOSED_TASK_STATES
from opengever.task import OPEN_TASK_STATES
from opengever.task.task import ITask
from plone import api
from Products.CMFCore.interfaces import IFolderish
from Products.CMFPlone.CatalogTool import allowedRolesAndUsers
from Products.CMFPlone.utils import transaction_note
import argparse
import json
import logging
import sys
import transaction


def get_path(obj):
    return "/".join(obj.getPhysicalPath())


def provides_any(obj, iface_list):
    return any([iface.providedBy(obj) for iface in iface_list])


def setup_logger():
    logger = getLogger()
    logger.setLevel(logging.INFO)
    stream_handler = logger.handlers[0]
    stream_handler.setLevel(INFO)
    log_formatter = Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    stream_handler.setFormatter(log_formatter)
    return logger


logger = setup_logger()


class MakeRepositoryReadOnly(object):
    """Make a repository read only by modifying local role assignments.
    """

    ROLES_ALLOWED_TO_REMOVE = (
        u"Contributor",
        u"DossierManager",
        u"Editor",
        u"Publisher",
        u"Reviewer",
    )

    OPEN_STATES = DOSSIER_STATES_OPEN + OPEN_TASK_STATES

    CLOSED_STATES = DOSSIER_STATES_CLOSED + CLOSED_TASK_STATES + [
        'task-state-skipped',
        'task-state-planned',
    ]

    def __init__(self, plone, args):
        self.plone = plone

        self.repository_id = args.repository_id
        self.verify = args.verify
        self.dryrun = args.dryrun

        self.reporoot = self.plone[self.repository_id]
        if not IRepositoryRoot.providedBy(self.reporoot):
            raise Exception("Object %r is not a repository root!" % self.reporoot)

    def run(self):
        self.assert_no_open_dossiers()
        self.assert_no_open_tasks()

        with VerifyViewPermissionUnchanged(self.reporoot, enabled=self.verify):
            with OperationsLog(self.repository_id) as ops_log:
                self.drop_all_roles_except_reader(ops_log)

        logger.info("Successfully made repository %r readonly." % self.repository_id)
        if self.dryrun:
            logger.info("(Dry Run - no changes committed)")

    def drop_all_roles_except_reader(self, ops_log):
        for obj in RepositoryContentIterator(self.reporoot, yield_root=True):
            logger.info("Checking " + get_path(obj))

            self.assert_in_closed_state(obj)

            manager = RoleAssignmentManager(obj)
            for assignment_data in manager.storage.get_all():
                old_assignment = dict(assignment_data)
                new_assignment = dict(assignment_data)
                new_assignment["roles"] = [u"Reader"]

                self.assert_no_unexpected_roles_dropped(old_assignment["roles"])

                manager.add_or_update(
                    principal=new_assignment["principal"],
                    roles=new_assignment["roles"],
                    cause=new_assignment["cause"],
                    reference=new_assignment["reference"],
                    reindex=False,
                )
                ops_log.local_roles_modified(obj, old_assignment, new_assignment)

    def assert_no_open_dossiers(self):
        """Quick check using catalog to make sure no open dossiers exist.
        """
        self._assert_no_open_objects("dossiers", IDossierMarker)

    def assert_no_open_tasks(self):
        """Quick check using catalog to make sure no open tasks exist.
        """
        self._assert_no_open_objects("tasks", ITask)

    def _assert_no_open_objects(self, type_name, type_iface):
        catalog = api.portal.get_tool("portal_catalog")
        open_objects = catalog.unrestrictedSearchResults(
            path=get_path(self.reporoot),
            object_provides=type_iface.__identifier__,
            review_state=self.OPEN_STATES,
        )
        if len(open_objects) > 0:
            logger.error("The following %s are not closed:" % type_name)
            for brain in open_objects:
                logger.error(brain.getPath())
            raise Exception("Found open %s!" % type_name)

    def assert_in_closed_state(self, obj):
        """Check again that dossiers and tasks are closed (using the obj).
        """
        if provides_any(obj, [IRepositoryRoot, IRepositoryFolder]):
            return

        review_state = api.content.get_state(obj)

        if review_state not in self.CLOSED_STATES + self.OPEN_STATES:
            raise Exception(
                "Unexpected review state %r for object %r! Please handle this "
                "review state by making sure it's listed in either "
                "OPEN_STATES or CLOSED_STATES." % (review_state, obj))

        if review_state not in self.CLOSED_STATES:
            raise Exception("Unexpected open object: %r" % obj)

    def assert_no_unexpected_roles_dropped(self, existing_roles):
        for existing_role in existing_roles:
            if existing_role == u"Reader":
                continue
            if existing_role not in self.ROLES_ALLOWED_TO_REMOVE:
                raise Exception("Refusing to remove role %r" % existing_role)


class RepositoryContentIterator(object):
    """Recursively traverses content in a repository, yielding those items
    that may have local roles that need to be adjusted to make the entire
    repository read-only.
    """

    # Types that may have local roles that need to be reduced to ['Reader']
    TYPES_WITH_LOCAL_ROLES = (
        IRepositoryRoot,
        IRepositoryFolder,
        IDossierMarker,
    )

    # All other types (can't have local roles, or they don't need to be
    # modified because they behave read-only anyway inside closed dossiers or
    # tasks because of workflow).
    TYPES_WITHOUT_LOCAL_ROLES = (
        IBaseDocument,
        IProposal,
        ITask,
    )

    def __init__(self, root, yield_root=False):
        self.root = root
        self.yield_root = yield_root

    def __iter__(self):
        if self.yield_root:
            assert IRepositoryRoot.providedBy(self.root)
            yield self.root

        if IFolderish.providedBy(self.root):
            for obj in self.root.objectValues():
                if self.is_type_with_local_roles(obj):
                    yield obj

                for child in RepositoryContentIterator(obj):
                    yield child

    def is_type_with_local_roles(self, obj):

        if provides_any(obj, self.TYPES_WITH_LOCAL_ROLES):
            return True

        else:
            if not provides_any(obj, self.TYPES_WITHOUT_LOCAL_ROLES):
                raise Exception(
                    "Unexpected type for %r. Please list it in either "
                    "TYPES_WITH_LOCAL_ROLES or TYPES_WITHOUT_LOCAL_ROLES" % obj
                )


class OperationsLog(object):
    """Log performed operations to the console as well as a structured
    operations log on in var/log/ in JSON format.
    """

    def __init__(self, repository_id):
        self.ops_log_path = LogFilePathFinder().get_logfile_path(
            "make-repository-%s-readonly" % repository_id,
            add_timestamp=True,
            extension="json.log",
        )

    def __enter__(self):
        self.ops_logfile = open(self.ops_log_path, "wb")
        return self

    def log_operation(self, operation):
        self.ops_logfile.write(json.dumps(operation) + "\n")

    def local_roles_modified(self, obj, old_assignment, new_assignment):
        op = "LocalRoleAssignmentModified"
        operation = {
            "operation": op,
            "obj": obj.UID(),
            "before": old_assignment,
            "after": new_assignment,
        }
        self.log_operation(operation)
        logger.info("%s: %s (Principal: %s, Cause: %s)" % (
            op,
            get_path(obj),
            old_assignment["principal"],
            old_assignment["cause"])
        )

    def __exit__(self, exc_type, exc_value, traceback):
        self.ops_logfile.close()


class VerifyViewPermissionUnchanged(object):
    """Context manager that verifies that the View permission didn't change.

    More precisely it makes sure that for every object where we (potentially)
    modified the local roles, the list of principals that have 'View' is the
    same before and after the change.

    This is an assumption that we make that allows us to skip reindexing of
    allowedRolesAndUsers, therefore making the script much faster.

    It's also a business level constraint for this script: Just "making a
    repository read-only" should not result in any other users having View
    access than before.

    This is mostly intended to be used during development, or a test run on
    PROD with --dry-run beforehand, because it will be quite slow.
    """

    def __init__(self, root, enabled=False):
        self.root = root
        self.enabled = enabled
        self.before = None
        self.after = None

    def __enter__(self):
        if not self.enabled:
            return

        self.before = self.build_view_permission_report(self.root)

    def __exit__(self, exc_type, exc_value, traceback):
        if not self.enabled:
            return

        if exc_type is not None:
            # No need to build 2nd report and verify if an exception occured
            return

        self.after = self.build_view_permission_report(self.root)
        self.assert_view_permission_unchanged(self.before, self.after)

    def build_view_permission_report(self, root):
        """Build a mapping of {path: list_of_principals_with_view}.
        """
        principals_with_view = {}
        for obj in RepositoryContentIterator(root, yield_root=True):
            allowed = allowedRolesAndUsers(obj)()
            principals_with_view[get_path(obj)] = set(allowed)

        return principals_with_view

    def assert_view_permission_unchanged(self, before, after):
        if not before == after:
            logger.error("View permission changed unexpectedly:")
            for path, principals in before.items():
                principals_after = after[path]
                if principals != principals_after:
                    logger.error("Object at %s:" % path)
                    logger.error("  Principals with view before: %r" % principals)
                    logger.error("  Principals with view after: %r" % principals_after)
                    logger.error("")

            raise Exception("View permission changed unexpectedly")


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repository_id",
        help="Repository ID",
    )
    parser.add_argument(
        "--verify",
        help="Verify that View permission didn't change (very slow)",
        action="store_true",
        default=False,
    )

    parser.add_argument(
        "-n",
        "--dry-run",
        dest="dryrun",
        help="Dry-Run",
        action="store_true",
        default=False,
    )

    args = parser.parse_args(sys.argv[3:])

    if args.dryrun:
        transaction.doom()

    plone = setup_plone(setup_app())

    MakeRepositoryReadOnly(plone, args).run()

    if not args.dryrun:
        transaction_note('Make repository %s readonly' % args.repository_id)
        transaction.commit()
        logger.info("Transaction committed.")
