from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.models.group import Group
from plone import api
from sqlalchemy.orm.exc import NoResultFound
import logging
import transaction


root_logger = logging.root

"""
Script to list both groups that have been imported to OGDS, and groups that
are actually used as local roles.

    bin/instance run list_imported_and_used_groups.py

"""


class GroupLister(object):

    def __init__(self, options):
        self.prefix = options.prefix
        self.catalog = api.portal.get_tool('portal_catalog')

    def collect_all_sql_groups(self):
        sql_groupinfos = []
        sql_groups = Group.query.order_by(Group.groupid)
        for sql_group in sql_groups:
            if self.prefix:
                if not sql_group.groupid.lower().startswith(self.prefix.lower()):
                    continue

            sql_groupinfos.append((sql_group.groupid, sql_group.title))
        return sql_groupinfos

    def collect_all_groups_from_local_roles(self):
        # In theory, pretty much everything except documents and mails could
        # have local role assignments
        brains = self.catalog.unrestrictedSearchResults(
            portal_type={
                'not': ['opengever.document.document', 'ftw.mail.mail']}
        )
        total = len(brains)

        all_principals_used_in_local_roles = set()

        for i, brain in enumerate(brains):
            if i % 1000 == 0:
                print "%s/%s" % (i, total)

            obj = brain.getObject()
            local_roles = getattr(obj, '__ac_local_roles__', None)
            if local_roles is None:
                continue

            principals_used_in_local_roles = local_roles.keys()
            for principal in principals_used_in_local_roles:
                all_principals_used_in_local_roles.add(principal)

        groups_used_in_local_roles = []
        for principal in all_principals_used_in_local_roles:
            try:
                group = Group.query.filter_by(groupid=principal).one()
                groups_used_in_local_roles.append((group.groupid, group.title))
            except NoResultFound:
                print "%s doesn't seem to be a group" % principal
                pass

        groups_used_in_local_roles.sort()
        return groups_used_in_local_roles

    def display_grouplist(self, grouplist):
        for group_id, group_title in grouplist:
            if group_title is None:
                group_title = ''
            print "%s;%s" % (group_id, group_title)

    def run(self):
        print
        sql_groupinfos = self.collect_all_sql_groups()
        print "# Groups from SQL"
        print "id;title"
        self.display_grouplist(sql_groupinfos)

        print
        print
        print

        groups_from_local_roles = self.collect_all_groups_from_local_roles()
        print
        print "# Groups from local roles"
        print "id;title"
        self.display_grouplist(groups_from_local_roles)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-p", "--prefix", action="store", dest="prefix", default=None)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    group_lister = GroupLister(options)
    group_lister.run()
