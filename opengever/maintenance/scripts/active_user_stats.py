"""
Script that calculates the current active users in total over all admin units an writes it into
a logfile.

bin/instance0 run src/opengever.maintenance/opengever/maintenance/scripts/active_user_stats.py
"""
import json
import logging
from datetime import date
from datetime import datetime
from datetime import timedelta
from logging import FileHandler
from os.path import join

import pytz
from ftw.contentstats.logger import get_log_dir_path
from opengever.maintenance import dm
from opengever.maintenance.debughelpers import setup_option_parser
from tzlocal import get_localzone

from opengever.meeting.model import Committee
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.ogds.models.group import Group
from opengever.ogds.models.group import groups_users
from opengever.ogds.models.org_unit import OrgUnit
from opengever.ogds.models.user import User

logger = logging.getLogger('opengever.maintenance')
LOG_TZ = get_localzone()


class UserStatsCalculator(object):
    """Returns statistics for the active users on the system
    """
    def get_stats(self):
        stats = {
            'active_users': self.calc_active_unique_users(),
            'active_users_logged_in_last_30_days': self.calc_users_logged_in_last_x_days(30),
            'active_users_logged_in_last_365_days': self.calc_users_logged_in_last_x_days(365),
            'active_users_logged_in_once_at_least_once': self.calc_users_logged_in_at_least_once(),
            'active_users_never_logged_in': self.calc_users_never_logged_in(),
            'active_users_in_any_org_unit': self.calc_active_unique_users_in_any_org_unit(),
            'active_users_in_any_org_unit_of_current_admin_unit':
                self.calc_active_unique_users_in_any_org_unit_of_current_admin_unit(),
            'active_spv_users': self.calc_active_unique_spv_users(),
        }

        return stats

    def get_active_users_query(self):
        return User.query.filter_by(active=True)

    def get_active_users_by_groups_query(self, group_ids):
        query = self.get_active_users_query().join(groups_users).join(Group)
        return query.filter(Group.groupid.in_(group_ids))

    def calc_active_unique_users(self):
        return self.get_active_users_query().count()

    def calc_users_logged_in_last_x_days(self, days):
        last_x_days = date.today() - timedelta(days=days)
        return self.get_active_users_query().filter(
            User.last_login > last_x_days).count()

    def calc_users_never_logged_in(self):
        return self.get_active_users_query().filter(
            User.last_login == None).count()  # noqa

    def calc_users_logged_in_at_least_once(self):
        return self.get_active_users_query().filter(
            User.last_login != None).count()  # noqa

    def calc_active_unique_spv_users(self):
        group_ids = [
            committee.group_id for committee in
            Committee.query.filter_by(workflow_state='active')]

        query = self.get_active_users_by_groups_query(group_ids)
        return query.distinct().count()

    def calc_active_unique_users_in_any_org_unit(self):
        group_ids = [
            org_unit.users_group_id for org_unit in
            OrgUnit.query.filter_by(enabled=True).all()]

        query = self.get_active_users_by_groups_query(group_ids)
        return query.distinct().count()

    def calc_active_unique_users_in_any_org_unit_of_current_admin_unit(self):
        current_admin_unit_id = get_current_admin_unit().id()
        group_ids = [
            org_unit.users_group_id for org_unit in
            OrgUnit.query.filter_by(enabled=True,
                                    admin_unit_id=current_admin_unit_id).all()]

        query = self.get_active_users_by_groups_query(group_ids)
        return query.distinct().count()


if __name__ == '__main__':
    dm()

    parser = setup_option_parser()
    options, args = parser.parse_args()

    stats = UserStatsCalculator().get_stats()

    if options.verbose:
        print(json.dumps(stats))

    # Taken from ftw.contentstats
    path = join(get_log_dir_path(), 'user-stats-json.log')
    handler = FileHandler(path)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    ts = datetime.utcnow().replace(tzinfo=pytz.utc)
    stats['timestamp'] = ts.astimezone(LOG_TZ).isoformat()

    value = json.dumps(stats, sort_keys=True)
    logger.info(value)
