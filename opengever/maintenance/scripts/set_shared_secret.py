from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.debughelpers import setup_option_parser
import sys
import transaction


USAGE = """\
Usage: bin/instance run set_shared_secret.py <shared_secret>
"""

SEPARATOR = '-' * 78


def set_shared_secret(plone, secret):
    acl_users = plone.acl_users
    acl_users.session._shared_secret = secret

    transaction.commit()
    print "Shared secret successfully set."

def main():
    app = setup_app()
    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print USAGE
        print "Error: Incorrect number of arguments"
        sys.exit(1)

    secret = args[0]
    plone = setup_plone(app, options)
    set_shared_secret(plone, secret)
    print SEPARATOR


if __name__ == '__main__':
    main()
