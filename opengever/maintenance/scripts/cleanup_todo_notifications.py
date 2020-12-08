"""
This script deletes notifications for activities on ToDos that were incorrectly
sent. These were due to a bug adding new participant of a workspace as Watcher
on all ToDos and not just the ones in that particular workspace.

Note that it is not possible to know which notifications were wrongfully sent,
as we cannot know what users were legitimate watchers at the time of the
Activity. Instead, we delete all notifications for ToDo activities that were
sent to users who are not currently watchers on the corresponding resource.

    bin/instance run ./scripts/cleanup_todo_notifications.py

"""
from opengever.activity.model import Activity
from opengever.activity.model import Notification
from opengever.activity.model import Resource
from opengever.base.model import create_session
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
import sys
import transaction


def find_notifications_to_delete():
    to_delete = []
    todo_activities = Activity.query.join(Resource).filter(
        Resource.admin_unit_id == get_current_admin_unit().id()).filter(
        Activity.kind.in_(['todo-modified', 'todo-assigned'])
        )
    for activity in todo_activities:
        # we determine the current set of users that are watching the given
        # resource
        userids = set()
        watchers = activity.resource.watchers
        for watcher in watchers:
            userids = userids.union(watcher.get_user_ids())

        # We mark as to_delete all notifications for users that are not
        # currently watchers on the given resource.
        notifications = Notification.query.filter_by(activity_id=activity.id)
        to_delete.extend([notification for notification in notifications
                          if notification.userid not in userids])

    return to_delete


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 0:
        print "Not expecting any arguments"
        sys.exit(1)

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    to_delete = find_notifications_to_delete()

    affected_resources = {notification.activity.resource
                          for notification in to_delete}
    affected_objects = {resource.oguid.resolve_object()
                        for resource in affected_resources}
    print "affected objects: {}".format(affected_objects)
    print "deleting {} notifications".format(len(to_delete))

    if not options.dryrun:
        print "deleting ..."
        session = create_session()
        for notification in to_delete:
            session.delete(notification)

        transaction.commit()


if __name__ == '__main__':
    main()
