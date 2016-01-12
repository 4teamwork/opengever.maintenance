from datetime import datetime
from opengever.base.adapters import CHILD_REF_KEY
from opengever.base.adapters import DOSSIER_KEY
from opengever.base.adapters import PREFIX_REF_KEY
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.task import ITask
from plone import api
from pprint import pprint as pp
from zope.annotation.interfaces import IAnnotations
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
import transaction


SEPARATOR = '-' * 78


class ReferenceChecker(object):

    def __init__(self, options):
        self.options = options
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intids = getUtility(IIntIds)

    def all_repositories(self):
        return [repo.getObject() for repo in self.catalog(
            portal_type='opengever.repository.repositoryfolder')]

    def check_starts_by_one(self):
        to_fix = []
        for repository in self.all_repositories():
            dossiers = repository.listFolderContents(
                {'portal_type': 'opengever.dossier.businesscasedossier'})
            annotations = IAnnotations(repository)
            if not dossiers:
                'SKIPPED check for {} because no dossiers'.format(repository)
                continue

            dossier_mapping = annotations.get(DOSSIER_KEY)
            prefixes = dossier_mapping.get(PREFIX_REF_KEY).values()
            prefixes.sort()

            if not prefixes[0] == u'1':
                to_fix.append(repository)

        for repository in to_fix:
            self.fix_repository_which_does_not_start_by_one(repository)

    def fix_repository_which_does_not_start_by_one(self, repository):
        annotations = IAnnotations(repository)
        dossier_mapping = annotations.get(DOSSIER_KEY)
        prefix_mapping = dossier_mapping.get(PREFIX_REF_KEY)
        child_ref_mapping = dossier_mapping.get(CHILD_REF_KEY)
        reversed_child_ref_mapping = self.reverse_child_mapping(child_ref_mapping)
        for intid, prefix in dossier_mapping.get(PREFIX_REF_KEY).items():
            if intid in reversed_child_ref_mapping.keys():
                prefix_mapping[intid] = reversed_child_ref_mapping[intid][0]

            dossier = self.intids.getObject(intid)
            self.reindex_dossier_and_children(dossier)
            print 'Dossiers Fixed {}'.format('/'.join(dossier.getPhysicalPath()))

    def reverse_child_mapping(self, child_ref_mapping):
        mapping = {}
        for prefix, intid in child_ref_mapping.items():
            if mapping.get(intid):
                mapping[intid].append(prefix)
            else:
                mapping[intid] = [prefix]

        return dict([(key, sorted(value)) for key, value in mapping.items()])

    def reindex_dossier_and_children(self, dossier):
        children = self.catalog(path='/'.join(dossier.getPhysicalPath()))
        for child in children:
            obj = child.getObject()
            obj.reindexObject(idxs=['reference'])

            if ITask.providedBy(obj):
                obj.get_sql_object().sync_with(obj)

    def check_ref_nums(self):
        for repository in self.all_repositories():
            dossiers = repository.listFolderContents(
                {'portal_type': 'opengever.dossier.businesscasedossier'})
            annotations = IAnnotations(repository)
            dossier_mapping = annotations.get(DOSSIER_KEY)
            if not dossiers:
                'SKIPPED check for {} because no dossiers'.format(repository)
                continue
            if not dossier_mapping:
                raise Exception('No dossier mapping')

            fails = []
            for dossier in dossiers:
                intid = self.intids.getId(dossier)
                prefix = dossier_mapping.get(PREFIX_REF_KEY).get(intid)
                if not prefix:
                    raise Exception('dossier not registered in mapping')

                # check number is registered correctly in the CHILD_REF mapping
                if dossier_mapping.get(CHILD_REF_KEY).get(prefix) != intid:
                    # dossier not registered in CHILD_REF_KEY mapping
                    fails.append('Dossier not correctly registerd in CHILD_REF_KEY')

            if fails:
                print SEPARATOR
                print 'Problems detected on repository {}'.format(
                    '/'.join(repository.getPhysicalPath()))
                print SEPARATOR
                for fail in fails:
                    print fail

                prefixes = dossier_mapping.get(PREFIX_REF_KEY).values()
                if len(prefixes) != len(set(prefixes)):
                    raise Exception('mutliple objects registered for the same prefixes')

                self.add_missing_child_ref_entries(repository)

    def add_missing_child_ref_entries(self, repository):
        annotations = IAnnotations(repository)
        dossier_mapping = annotations.get(DOSSIER_KEY)
        pp(dossier_mapping)
        prefix_mapping = dossier_mapping.get(PREFIX_REF_KEY)
        child_ref_mapping = dossier_mapping.get(CHILD_REF_KEY)
        for intid, prefix in prefix_mapping.items():
            if not child_ref_mapping.get(prefix):
                child_ref_mapping[prefix] = intid

            elif child_ref_mapping.get(prefix) != intid:
                import pdb; pdb.set_trace()
                # different child_ref_mapping exists

        pp(dossier_mapping)


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    print SEPARATOR
    print "Date: {}".format(datetime.now().isoformat())
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    ReferenceChecker(options).check_ref_nums()

    if not options.dry_run:
        transaction.commit()

    print "Done."
    print SEPARATOR
    print SEPARATOR


if __name__ == '__main__':
    main()
