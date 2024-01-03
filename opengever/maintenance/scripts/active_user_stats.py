"""
Script that calculates the current active users in total over all admin units an writes it into
a logfile.

bin/instance0 run src/opengever.maintenance/opengever/maintenance/scripts/active_user_stats.py
"""
from datetime import datetime
from ftw.contentstats.logger import get_log_dir_path
from logging import FileHandler
from opengever.maintenance import dm
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.meeting import is_meeting_feature_enabled
from opengever.meeting.model import Committee
from opengever.ogds.models.group import Group
from opengever.ogds.models.group import groups_users
from opengever.ogds.models.org_unit import OrgUnit
from opengever.ogds.models.user import User
from opengever.workspace import is_workspace_feature_enabled
from os.path import join
from tzlocal import get_localzone
import json
import logging
import pytz

logger = logging.getLogger('opengever.maintenance')
LOG_TZ = get_localzone()


class UserStatsCalculator(object):
    """Returns statistics for the active users on the system
    """
    def get_stats(self):
        stats = {
            'total_active_unique_gever_users': 0,
            'total_active_unique_teamraum_users': 0,
            'total_active_unique_spv_users': 0,
        }

        unique_users = self.calc_total_active_unique_users()
        if not is_workspace_feature_enabled():
            stats['total_active_unique_gever_users'] = unique_users
        else:
            stats['total_active_unique_teamraum_users'] = unique_users

        if is_meeting_feature_enabled():
            stats['total_active_unique_spv_users'] = self.calc_total_active_unique_spv_users()

        return stats

    def get_active_users_by_groups_query(self, group_ids):
        query = User.query.join(groups_users).join(Group)
        return query.filter(Group.groupid.in_(group_ids)).filter_by(active=True)

    def calc_total_active_unique_users(self):
        group_ids = [
            org_unit.users_group_id for org_unit in
            OrgUnit.query.filter_by(enabled=True).all()]

        query = self.get_active_users_by_groups_query(group_ids)
        return query.count()

    def calc_total_active_unique_spv_users(self):
        group_ids = [
            committee.group_id for committee in
            Committee.query.filter_by(workflow_state='active')]

        query = self.get_active_users_by_groups_query(group_ids)
        return query.count()


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
