from AccessControl import getSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from opengever.document.interfaces import ICheckinCheckoutManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from Products.CMFCore.utils import getToolByName
from zope.component import getMultiAdapter
import sys
import transaction


SEPARATOR = '-' * 78


def checkin_not_allowed_reason(obj):
        """Checks whether checkin is allowed for the current user on the
        adapted document.
        """
        manager = getMultiAdapter((obj, obj.REQUEST), ICheckinCheckoutManager)
        # is it checked out?
        if not manager.checked_out():
            return "Not checked out"

        # is it versionable?
        if not manager.repository.isVersionable(obj):
            return "Not versionable"

        # is the user able to write to the object?
        if not manager.check_permission('Modify portal content'):
            return "No Modify Portal Content"

        # does the user have the necessary permission?
        if not manager.check_permission('opengever.document: Checkin'):
            return "No Checkin permission"

        # is the user either the one who owns the checkout or
        # a manager?
        current_user_id = getSecurityManager().getUser().getId()
        if not manager.checked_out() == current_user_id:
            return "User does not own checkout"


def checkin_documents_for_user(portal, options):
    """Attempts to check in all documents checked out by a particular user.
    """
    username = options.user

    # Assume security context of user
    user = portal.acl_users.getUser(username)
    user = user.__of__(portal.acl_users)
    newSecurityManager(portal, user)

    catalog = getToolByName(portal, 'portal_catalog')
    docs = catalog(portal_type='opengever.document.document')
    checked_out_docs = [b.getObject() for b in docs if b.checked_out == username]

    for obj in checked_out_docs:
        manager = getMultiAdapter((obj, obj.REQUEST), ICheckinCheckoutManager)
        if not manager.is_checkin_allowed():
            print "WARNING: Checkin not allowed for document %s" % obj.absolute_url()
            print checkin_not_allowed_reason(obj)
        else:
            if not options.dryrun:
                manager.checkin(comment=options.comment)
                print "Checked in document %s" % obj.absolute_url()
            else:
                print "Would checkin document %s" % obj.absolute_url()

    if not options.dryrun:
        transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-u", "--user", action="store", dest="user", default=None)
    parser.add_option("-n", "--dry-run", action="store_true", dest="dryrun", default=False)
    parser.add_option("-c", "--comment", action="store", dest="comment", default='')
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)

    if not options.user:
        print "Please supply a username!"
        sys.exit(1)

    checkin_documents_for_user(plone, options)


if __name__ == '__main__':
    main()
