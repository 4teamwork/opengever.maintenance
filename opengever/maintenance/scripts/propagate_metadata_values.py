"""
Propagates metadata values related to the classification and lifecycle behaviors
to the objects underneath the given root object.

This example runs a dry-run and updates the selected fields:
    bin/instance run propagate_metadata_values.py -n --fields retention_period archival_value

To restrict the propagation to a subpath do:
    bin/instance run propagate_metadata_values.py --root-node-path ordnungssystem/subpath --fields archival_value

"""
from Acquisition import aq_chain
from opengever.base.behaviors.classification import IClassification
from opengever.base.behaviors.classification import IClassificationMarker
from opengever.base.behaviors.lifecycle import ILifeCycle
from opengever.base.behaviors.lifecycle import ILifeCycleMarker
from opengever.base.default_values import get_persisted_value_for_field
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.scripts.item_processor import Processor
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from zope.schema import ValidationError
import argparse
import sys
import transaction


FIELD_NAME_CLASSIFICATION = 'classification'
FIELD_NAME_PRIVACY_LAYER = 'privacy_layer'
FIELD_NAME_RETENTION_PERIOD = 'retention_period'
FIELD_NAME_ARCHIVAL_VALUE = 'archival_value'
FIELD_NAME_CUSTODY_PERIOD = 'custody_period'


class MetadataUpdater():
    """Propagates metadtata from repository folders down to dossiers.

    This is a reimplementation of https://github.com/4teamwork/opengever.sg/blob/master/opengever/sg/base/nightlyjobs/maintenance_jobs.py#L13

    Items are now updated immediately instead of via a nightly job. This change
    was necessary because updating a single field by modifying a class variable
    isn't possible in the nightly job setup. Additionally, we want to avoid
    creating dependencies from opengever.maintenance to a policy package.
    """
    fields = [
        IClassification['classification'],
        IClassification['privacy_layer'],
        ILifeCycle['retention_period'],
        ILifeCycle['archival_value'],
        ILifeCycle['custody_period']
    ]

    fields_to_index = {'public_trial': 'public_trial',
                       'retention_period': 'retention_expiration',
                       'archival_value': 'archival_value'}

    marker_interfaces = {IClassification: IClassificationMarker,
                         ILifeCycle: ILifeCycleMarker}

    stats_total_updated = 0

    def update_metadata(self, obj):
        if IRepositoryFolder.providedBy(obj):
            # Values have already been set correctly on repository folders
            # We should not have submitted any jobs for repofolders, but better
            # be safe.
            return

        parent_repo = self.get_parent_repofolder(obj)
        if not parent_repo:
            # We only update objects inside the repository, so we skip private
            # dossiers, sablon templates, proposal templates.
            return

        to_reindex = []
        for field in self.fields:
            marker = self.marker_interfaces[field.interface]
            if not marker.providedBy(obj):
                continue

            value = self.get_value_for_field(obj, field)
            parent_value = self.get_value_for_field(parent_repo, field)

            if value != parent_value:
                field.set(field.interface(obj), parent_value)

                if field.getName() in self.fields_to_index:
                    to_reindex.append(self.fields_to_index[field.getName()])

                self.stats_total_updated += 1

        if to_reindex:
            obj.reindexObject(idxs=to_reindex)

    def update_metadata_for_brain(self, brain):
        if brain.portal_type == 'opengever.repository.repositoryfolder':
            # Values have already been set correctly on
            # repository folders
            return

        try:
            obj = brain.getObject()
        except Exception:
            return
        self.update_metadata(obj)

    def get_parent_repofolder(self, obj):
        repositories = filter(IRepositoryFolder.providedBy, aq_chain(obj))
        return repositories[0] if repositories else None

    def get_value_for_field(self, obj, field):
        value = get_persisted_value_for_field(obj, field)
        try:
            field.bind(obj).validate(value)
            return value
        except ValidationError:
            pass
        return

    def print_stats(self):
        print("Total objects updated: {}".format(self.stats_total_updated))


def get_objects_to_update(repofolder_path):
    query = {
        'object_provides': [
            IClassificationMarker.__identifier__,
            ILifeCycleMarker.__identifier__,
        ],
        'path': repofolder_path,
    }

    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(**query)
    return brains


def propagate_archival_value(repofolder_path, options):
    brains = get_objects_to_update(repofolder_path)
    print("Considering %s objects total" % len(brains))

    fields = []
    if FIELD_NAME_CLASSIFICATION in options.fields:
        fields.append(IClassification[FIELD_NAME_CLASSIFICATION])

    if FIELD_NAME_PRIVACY_LAYER in options.fields:
        fields.append(IClassification[FIELD_NAME_PRIVACY_LAYER])

    if FIELD_NAME_RETENTION_PERIOD in options.fields:
        fields.append(ILifeCycle[FIELD_NAME_RETENTION_PERIOD])

    if FIELD_NAME_ARCHIVAL_VALUE in options.fields:
        fields.append(ILifeCycle[FIELD_NAME_ARCHIVAL_VALUE])

    if FIELD_NAME_CUSTODY_PERIOD in options.fields:
        fields.append(ILifeCycle[FIELD_NAME_CUSTODY_PERIOD])

    updater = MetadataUpdater()
    updater.fields = fields

    Processor().run(list(brains), batch_size=1000, dry_run=options.dry_run, process_item_method=updater.update_metadata_for_brain)
    updater.print_stats()


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser(
        description="Propagate metadata values of a specific path to its underliing objects")
    parser.add_argument('-n', "--dry-run", action="store_true",
                        dest='dry_run', default=False, help='Dryrun')
    parser.add_argument(
        '-y', '--yes',
        action='store_true',
        help='Automatically answer yes to all confirmation prompts'
    )
    parser.add_argument(
        '--site-root',
        dest='site_root',
        default=None,
        help='Absolute path to the Plone site.')
    parser.add_argument(
        '--root-node-path',
        default='ordnungssystem',
        help='Path to root node. All objects within this root node will be updated')
    parser.add_argument(
        '--fields',
        nargs='+',
        required=True,
        choices=[
            FIELD_NAME_CLASSIFICATION,
            FIELD_NAME_PRIVACY_LAYER,
            FIELD_NAME_RETENTION_PERIOD,
            FIELD_NAME_ARCHIVAL_VALUE,
            FIELD_NAME_CUSTODY_PERIOD
        ],
        help="Define which field values should be updated")

    options = parser.parse_args(sys.argv[3:])
    plone = setup_plone(app, options)

    root_obj = plone.unrestrictedTraverse(options.root_node_path)

    if options.dry_run:
        print("Dry run: true")
        transaction.doom()

    print("I will propagate the following values: {} for all objects underneath of {}".format(
        options.fields,
        root_obj.absolute_url(),
    ))

    if not options.yes:
        answer = raw_input("Are you sure? (Y/n)") or 'y'
        if answer.lower() not in ["y", "yes"]:
            sys.exit("Aborted by the user")

    propagate_archival_value('/'.join(root_obj.getPhysicalPath()), options)

    if options.dry_run:
        print("Run in dry mode")
    print("sEverything done!")
