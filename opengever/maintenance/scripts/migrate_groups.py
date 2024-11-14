"""
A script which updates all local_roles for a given group_mapping (old:new).
Used for service requests from SG, e.g. https://4teamwork.atlassian.net/browse/CA-5829

Usage:

bin/instance0 run migrate_groups.py mode mapping

  - mode is one of 'analyse' or 'update'
  - mapping is a path to a json file containing the group mapping (dictionary
    with old group names as keys and new group names as values).

optional arguments:
  -s : siteroot
  -n : dry-run
  -i : Intermediate commits. Will commit after each object.
  -t : Will check and update tasks only. Used to update remote tasks on other
       deployments than the one where the groups were modified. We can therefore
       assume that the groups are only used for permissions on tasks (remote tasks).
"""
import argparse
import json
import logging
import os
import sys
from collections import Counter

import transaction
from Acquisition import aq_base
from ftw.solr.converters import CONVERTERS
from ftw.solr.interfaces import ISolrConnectionManager
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.base.role_assignments import RoleAssignmentManager
from opengever.contact.interfaces import IContactFolder
from opengever.disposition.interfaces import IDisposition
from opengever.document.document import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.templatefolder.interfaces import ITemplateFolder
from opengever.globalindex.model.task import Task
from opengever.inbox.container import IInboxContainer
from opengever.inbox.inbox import IInbox
from opengever.meeting.proposal import IBaseProposal
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.ogds.models.group import Group
from opengever.ogds.models.org_unit import OrgUnit
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task.task import ITask
from opengever.tasktemplates.content.tasktemplate import ITaskTemplate
from opengever.tasktemplates.content.templatefoldersschema import \
    ITaskTemplateFolderSchema
from opengever.workspace.interfaces import IWorkspace
from opengever.workspace.interfaces import IWorkspaceFolder
from plone import api
from zope.component import queryUtility

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.maintenance.utils import TextTable

logger = logging.getLogger()
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


class LocalRolesUpdater(object):

    def __init__(self, options):
        self.options = options
        with open(options.mapping, "r") as fin:
            self.group_mapping = json.load(fin)
        self.old_group_ids = set(self.group_mapping.keys())

        if self.options.tasks_only:
            self.include_tasks = True
        else:
            self.include_tasks = False

        self.log = []
        self.orgunits_with_modified_inbox_group = []
        self.catalog = api.portal.get_tool('portal_catalog')
        self.sm = queryUtility(ISolrConnectionManager)

    def check_preconditions(self):
        for org_unit in OrgUnit.query:
            if org_unit.users_group_id in self.old_group_ids:
                logger.warning(
                    'The group {} is defined as users_group of OrgUnit {}. '
                    'This script does not handle this case correctly. '
                    'It will adjust the org_units table in the OGDS '
                    'but you will have to adjust the global roles in the '
                    'portal_role_manager'.format(
                        org_unit.users_group_id, org_unit))
                raw_input("Press Enter to continue...")

            if org_unit.inbox_group_id in self.old_group_ids:
                logger.warning(
                    'The group {} is defined as inbox_group of OrgUnit {}. '
                    'We will therefore also update tasks.'.format(
                        org_unit.inbox_group_id, org_unit))
                self.include_tasks = True
                self.orgunits_with_modified_inbox_group.append(org_unit.id())
                raw_input("Press Enter to continue...")

        # Make sure that we did not make any typo in the group names
        for old_principal, new_principal in self.group_mapping.items():
            if Group.query.filter(Group.groupid == old_principal).count() != 1:
                raise Exception(
                    'The group {} not found in the OGDS'.format(old_principal))

            if Group.query.filter(Group.groupid == new_principal).count() != 1:
                raise Exception(
                    'The group {} not found in the OGDS'.format(new_principal))

    def analyse(self):
        self.check_preconditions()

        # We only need to check repositoryroots, -folders, dossiers,
        # tasktemplates, tasktemplatefolders, proposals and dispositions.
        # Because all these groups are not used as a org_unit group or inbox_group,
        # its not possible that those are granted on a document or task
        # because only managers would be able to give local roles
        # on other objects but they shouldn't

        if self.options.tasks_only:
            interfaces_to_update = []
        else:
            interfaces_to_update = [
                IRepositoryRoot.__identifier__,
                IRepositoryFolder.__identifier__,
                IDossierMarker.__identifier__,
                ITaskTemplateFolderSchema.__identifier__,
                ITaskTemplate.__identifier__,
                IBaseProposal.__identifier__,
                IDisposition.__identifier__,
                ITemplateFolder.__identifier__,
                IContactFolder.__identifier__,
                IInboxContainer.__identifier__,
                IInbox.__identifier__,
                IWorkspace.__identifier__,
                IWorkspaceFolder.__identifier__,
            ]

        if self.include_tasks:
            interfaces_to_update.append(ITask.__identifier__)

        logger.info(
            'Object types that will be checked:\n\n{}\n'.format(
                '\n'.join(interfaces_to_update))
        )

        brains = self.catalog.unrestrictedSearchResults(
            object_provides=interfaces_to_update,
            sort_on="path",
            sort_order="descending",
        )

        self.objs_to_update = []
        for brain in ProgressLogger('Analysing objects...', brains, logger=logger):
            obj = brain.getObject()
            if self.needs_update(obj):
                self.objs_to_update.append(obj)

                if ITask.providedBy(obj):
                    for item in getattr(aq_base(obj), 'relatedItems', []):
                        doc = item.to_object
                        if self.needs_update(doc):
                            self.objs_to_update.append(doc)

                        if self._is_inside_a_proposal(doc):
                            proposal = doc.get_proposal()
                            if self.needs_update(proposal):
                                self.objs_to_update.append(proposal)

                    if self.options.tasks_only:
                        dossier = obj.get_containing_dossier()
                        if dossier and self.needs_update(dossier) and dossier not in self.objs_to_update:
                            self.objs_to_update.append(dossier)

        logger.info(
            '{} Plone objects have to be uptated'.format(
                len(self.objs_to_update))
        )

        # Check for remote tasks that might need to be updated.
        logger.info('Checking remote tasks...')
        self.remote_tasks = []
        if self.orgunits_with_modified_inbox_group:
            self.remote_tasks = Task.query.filter(
                Task.assigned_org_unit.in_(self.orgunits_with_modified_inbox_group)).filter(
                Task.admin_unit_id != get_current_admin_unit().id()).filter(
                Task.review_state.notin_(('task-state-tested-and-closed', 'task-state-cancelled'))
            ).all()
        logger.info('DONE Checking remote tasks.')

        self.write_obj_paths()
        self.write_remote_tasks_paths()
        self.print_analysis_stats()

    def _is_inside_a_proposal(self, maybe_document):
        if not IBaseDocument.providedBy(maybe_document):
            return False
        return maybe_document.is_inside_a_proposal()

    def needs_update(self, obj):
        principals = [principal for (principal, roles) in obj.get_local_roles()]
        for principal in principals:
            if principal in self.old_group_ids:
                return True

    def update(self):
        # Update the OGDS if necessary:
        for org_unit in OrgUnit.query:
            if org_unit.users_group_id in self.old_group_ids:
                org_unit.users_group_id = self.group_mapping[org_unit.users_group_id]

            if org_unit.inbox_group_id in self.old_group_ids:
                org_unit.inbox_group_id = self.group_mapping[org_unit.inbox_group_id]

        # Update objects
        for i, obj in enumerate(ProgressLogger('Update role mappings', self.objs_to_update)):
            changes = []
            manager = RoleAssignmentManager(obj)

            # We directly access the role_assignment storage to check and
            # update all assignments in one step and calculating afterwards
            # to save time.
            for assignment in manager.storage._storage():
                if assignment['principal'] in self.old_group_ids:
                    old_principal = assignment['principal']
                    new_principal = self.group_mapping[old_principal]
                    assignment['principal'] = new_principal
                    changes.append({
                        'old_principal': old_principal,
                        'new_principal': new_principal,
                        'roles': assignment['roles'],
                    })

            self.log.append(('/'.join(obj.getPhysicalPath()), changes))
            logger.info("updating {}".format(self.log[-1][0]))
            manager._update_local_roles(reindex=False)

            if self.options.intermediate_commits and not self.options.dryrun:
                if i % self.options.intermediate_commits == 0:
                    logger.info('Committing after {} items...'.format(i))
                    transaction.commit()

        self.sync_tasks()
        self.update_indexes()

    def update_indexes(self):
        """Instead of reindexing the object security for all concerned objects
        and their children, we directly modify the catalog and solr indexes.
        """
        index = self.catalog._catalog.indexes["allowedRolesAndUsers"]
        uid_index = self.catalog._catalog.indexes['UID']

        schema = self.sm.schema
        field = schema.fields['allowedRolesAndUsers']
        field_class = schema.field_types[field[u'type']][u'class']
        multivalued = field.get(u'multiValued', False)
        converter = CONVERTERS.get(field_class)

        principal_mapping = {"user:{}".format(key): "user:{}".format(value)
                             for key, value in self.group_mapping.items()}
        old_principal_ids = set(principal_mapping.keys())

        logger.info('Updating indexes...')

        # Forward index is of the form _index[principal] = [docid1, docid2]
        for old_group, new_group in principal_mapping.items():
            if old_group in index._index:
                if new_group in index._index:
                    index._index[new_group].update(index._index.pop(old_group))
                else:
                    index._index[new_group] = index._index.pop(old_group)

        # Backward index is of the form _unindex[docid] = [principal1, principal2]
        # Solr index contains the same data as this backward index
        for docid, principals in index._unindex.items():
            if any([old_group in principals for old_group in old_principal_ids]):
                # Update the catalog index
                index._unindex[docid] = [principal_mapping.get(principal, principal)
                                         for principal in principals]
                # Update the solr index
                uid = uid_index.getEntryForObject(docid)
                value = converter(index._unindex[docid], multivalued)
                self.sm.connection.add({'allowedRolesAndUsers': {'set': value}, 'UID': uid})

        logger.info('DONE Updating indexes.')

    def sync_tasks(self):
        if self.options.tasks_only:
            # When only tasks are updated, we can simply sync the tasks.
            for obj in ProgressLogger('Syncing tasks', self.objs_to_update):
                if ITask.providedBy(obj) and self._needs_syncing(obj):
                    obj.sync()
        else:
            # When other objects were updated, we would need to check all
            # the tasks contained in any of the updated object, (as _principals
            # indexes all principals with view permissions on the task). Instead
            # we simply check all tasks.
            brains = self.catalog.unrestrictedSearchResults(
                object_provides=[ITask.__identifier__])
            for brain in ProgressLogger('Syncing tasks', brains):
                obj = brain.getObject()
                if self._needs_syncing(obj):
                    try:
                        obj.sync()
                    except Exception:
                        logger.warning("Could not sync task {}".format(obj.absolute_url()))

    @staticmethod
    def _needs_syncing(task):
        task_model = task.get_sql_object()
        if not task_model:
            logger.warning("Could not find task model for {}".format(task.absolute_url()))
            return False
        task_principals = [
            aa.principal for aa in task_model._principals]
        if task_principals != task.get_principals():
            return True
        return False

    def print_analysis_stats(self):
        obj_stats = Counter(obj.portal_type for obj in self.objs_to_update)

        self.stats_table = TextTable()
        self.stats_table.add_row(["portal_type", "number"])
        for row in obj_stats.items():
            self.stats_table.add_row(row)
        self.stats_table.add_row(["Total", sum(obj_stats.values())])
        self.stats_table.add_row(["Remote tasks", len(self.remote_tasks)])

        logger.info("\n\nAnalysis statistics:\n\n" + self.stats_table.generate_output())

        log_filename = LogFilePathFinder().get_logfile_path(
            'group_migration_stats', extension="csv")
        with open(log_filename, "w") as logfile:
            self.stats_table.write_csv(logfile)

    def write_obj_paths(self):
        paths_table = TextTable()
        paths_table.add_row(["portal_type", "path"])
        for obj in self.objs_to_update:
            paths_table.add_row([obj.portal_type, '/'.join(obj.getPhysicalPath())])

        log_filename = LogFilePathFinder().get_logfile_path(
            'group_migration_paths', extension="csv")
        with open(log_filename, "w") as logfile:
            paths_table.write_csv(logfile)

    def write_remote_tasks_paths(self):
        paths_table = TextTable()
        paths_table.add_row(["admin_unit_id", "path"])
        for task in self.remote_tasks:
            paths_table.add_row([task.admin_unit_id, task.physical_path])

        log_filename = LogFilePathFinder().get_logfile_path(
            'group_migration_remote_tasks_paths', extension="csv")
        with open(log_filename, "w") as logfile:
            paths_table.write_csv(logfile)

    def write_update_table(self):
        update_table = TextTable()
        update_table.add_row(['path', 'old_principal', 'new_principal', 'roles'])
        for path, changes in self.log:
            for change in changes:
                roles = ";".join(change['roles'])
                old_principal = change['old_principal']
                new_principal = change['new_principal']
                update_table.add_row([path, old_principal, new_principal, roles])

        log_filename = LogFilePathFinder().get_logfile_path(
            'group_migration_update', extension="csv")
        with open(log_filename, "w") as logfile:
            update_table.write_csv(logfile)

    def commit(self):
        transaction.commit()
        self.sm.connection.commit(soft_commit=False, after_commit=False)


def main():
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'mode',
        choices=['analyse', 'update'],
        help='Command',
    )
    parser.add_argument(
        'mapping',
        help='Path to JSON file containing a mapping from old to new groups',
    )
    parser.add_argument(
        '-s', dest='site_root',
        default=None,
        help='Absolute path to the Plone site',
    )
    parser.add_argument(
        '-n', dest='dryrun',
        action='store_true',
        default=False,
        help='Dry run',
    )
    parser.add_argument(
        '-i', dest='intermediate_commits',
        type=int,
        default=None,
        help='Intermediate commits',
    )
    parser.add_argument(
        '-t', dest='tasks_only',
        action='store_true',
        default=False,
        help='Used to update remote tasks on other deployments',
    )

    options = parser.parse_args(sys.argv[3:])

    setup_plone(app, options)
    logger.info('Migrating groups...')

    if options.dryrun:
        transaction.doom()
        logger.info('Dry run enabled')

    if not os.path.isfile(options.mapping):
        raise ValueError("{} is not a file.".format(options.mapping))

    updater = LocalRolesUpdater(options)
    if options.mode == 'analyse':
        logger.info('Mode: Analyse')
        options.dryrun = True
        updater.analyse()

    if options.mode == 'update':
        logger.info('Mode: Update')
        updater.analyse()
        updater.update()
        updater.write_update_table()

    if not options.dryrun:
        logger.info('Committing...')
        updater.commit()

    logger.info("All done")


if __name__ == '__main__':
    main()
