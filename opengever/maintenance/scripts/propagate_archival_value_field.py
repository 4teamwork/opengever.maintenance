"""
====================
WARNING! This script does not work as expected since it is not possible to change
the fields of a class which will be used in nightly jobs. Use the follow-up script: propagate_restricted_field_value.py
instead.
====================

Propagates archival_value field.

    bin/instance run propagate_archival_value_field.py -n <repofolder_path>

"""
from opengever.base.behaviors.classification import IClassificationMarker
from opengever.base.behaviors.lifecycle import ILifeCycle
from opengever.base.behaviors.lifecycle import ILifeCycleMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.sg.base.nightlyjobs.maintenance_jobs import NightlyMetadataUpdater
from plone import api
import logging
import sys
import transaction


log = logging.getLogger('opengever.maintenance')
log.setLevel(logging.INFO)
log.root.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


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


def propagage_archival_value(repofolder_path, options):
    brains = get_objects_to_update(repofolder_path)
    print("Considering %s objects total" % len(brains))

    with NightlyMetadataUpdater() as updater:
        updater.fields = [ILifeCycle['archival_value']]

        for brain in brains:

            if brain.portal_type == 'opengever.repository.repositoryfolder':
                # Values have already been set correctly on
                # repository folders
                continue

            print("Updating: %s" % brain.getPath())
            if not options.dryrun:
                updater.add_by_brain(brain)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if len(args) != 1:
        print("Must supply exactly one argument (<repofolder_path>)")
        print("Usage: bin/instance run propagate_archival_value_field.py -n <repofolder_path>")
        sys.exit(1)

    repofolder_path = args[0]
    print("Considering objects in repofolder %s" % repofolder_path)

    setup_plone(app, options)

    if options.dryrun:
        print('Dryrun ...')
        transaction.doom()

    propagage_archival_value(repofolder_path, options)

    if not options.dryrun:
        transaction.commit()
