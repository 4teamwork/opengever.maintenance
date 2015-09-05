"""
Sets the _secret_ts of the ftw.zm session_auth plugin to a given value so that
- secret rotation happens at the same time for all synced Plone instances
- secret rotation can be triggered on next login

In order to trigger secret rotation, choose a timestamp at least
_secret_max_age seconds in the past, for example time.time() - (6*60*60 + 1)
"""

from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
import sys
import transaction


USAGE = """\
Usage: bin/instance run set_secret_ts.py <new_secret_ts>

Example:
bin/instance run set_secret_ts.py 1441468033.626985
"""

SEPARATOR = '-' * 78


def set_secret_ts(app, new_secret_ts):
    session_auth = app.acl_users.session_auth
    session_auth._secret_ts = new_secret_ts
    print "session_auth._secret_ts set to {!r} ({})".format(
        new_secret_ts, datetime.fromtimestamp(new_secret_ts))
    transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print USAGE
        print "Error: Incorrect number of arguments"
        sys.exit(1)

    new_timestamp_str = args[0]
    new_secret_ts = float(new_timestamp_str)

    print SEPARATOR
    setup_plone(app, options)
    set_secret_ts(app, new_secret_ts)


if __name__ == '__main__':
    main()
