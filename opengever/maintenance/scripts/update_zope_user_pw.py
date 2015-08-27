from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
import sys
import transaction


USAGE = """\
Usage: bin/instance run update_zope_user_pw.py <user_id> <new_password>
"""

SEPARATOR = '-' * 78


def update_password(app, user_id, new_pass):
    pas = app.acl_users
    zodb_user_manager = pas.users
    zodb_user_manager.updateUserPassword(user_id, new_pass)
    transaction.commit()
    print "Password updated for user '{}'.".format(user_id)


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 2:
        print USAGE
        print "Error: Incorrect number of arguments"
        sys.exit(1)

    user_id, new_pass = args
    update_password(app, user_id, new_pass)
    print SEPARATOR


if __name__ == '__main__':
    main()
