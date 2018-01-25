"""
Script to list top users of tasks.
    bin/instance run get_top_task_users.py

"""
from collections import Counter
from opengever.globalindex.model.task import Task
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import create_session
from opengever.ogds.models.user import User


def get_top_task_users():
    counts_by_au = {}
    totals = Counter()

    session = create_session()
    tasks = session.query(Task).all()

    for task in tasks:
        au = task.admin_unit_id
        if au not in counts_by_au:
            counts_by_au[au] = Counter()

        user_ids = set(
            [task.responsible.encode('utf-8'), task.issuer.encode('utf-8')])
        for userid in user_ids:
            counts_by_au[au][userid] += 1
            totals[userid] += 1

    print "Counts by admin unit"
    print "=" * 80
    print

    for au, user_counts in counts_by_au.items():
        top_users = []
        for userid, num_tasks in user_counts.most_common(3):
            user = session.query(User).filter_by(userid=userid).one()
            email = user.email.encode('utf-8')
            top_users.extend([userid, email, str(num_tasks)])
        print ';'.join([au] + top_users)

    print
    print "Total counts"
    print "=" * 80
    print

    for userid, num_tasks in totals.most_common(25):
        user = session.query(User).filter_by(userid=userid).one()
        email = user.email.encode('utf-8')
        print "%s;%s;%s" % (userid, email, num_tasks)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    setup_plone(app, options)

    get_top_task_users()
