"""
The script updates the values of the passed restricted field for all children
of a given container, to make sure that the values for that field satisfy
the restrictions.

    bin/instance run ./scripts/propagate_restricted_field_value.py path/to/container fieldname
Options:
  -n : dry run
"""
from collections import namedtuple
from datetime import datetime
from opengever.base.acquisition import acquire_field_value
from opengever.base.behaviors.classification import IClassification
from opengever.base.behaviors.classification import IClassificationMarker
from opengever.base.behaviors.lifecycle import ILifeCycle
from opengever.base.behaviors.lifecycle import ILifeCycleMarker
from opengever.dossier.behaviors.dossier import IDossier
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from plone import api
import sys
import transaction

# Lightweight data structure to keep track of field values that got modified
FixedField = namedtuple(
    'FixedField', [
        'path',
        'field_name',
        'old_value',
        'new_value',
    ]
)

fieldname_to_field = {
    'classification': IClassification['classification'],
    'privacy_layer': IClassification['privacy_layer'],
    'retention_period': ILifeCycle['retention_period']}

fieldname_to_marker_interface = {
    'classification': IClassificationMarker,
    'privacy_layer': IClassificationMarker,
    'retention_period': ILifeCycleMarker}


class RestrictedVocabularyPropagator(object):

    def __init__(self, path, fieldname):
        self.container_path = path
        self.fieldname = fieldname
        self.catalog = api.portal.get_tool('portal_catalog')
        self.container = self.catalog.unrestrictedTraverse(path)
        self.field = fieldname_to_field[fieldname]
        self.marker = fieldname_to_marker_interface[fieldname]

    def run(self):
        self.propagate_vocab_restrictions()
        self.write_csv_log()

    def log(self, line):
        line += '\n'
        ts = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
        sys.stdout.write(ts + " " + line)

    def propagate_vocab_restrictions(self):
        """Propagate changes to fields with restricted vocabularies down to
        children of the folderish object (for the children whose field value would
        now violate the business rule imposed by the restricted vocabulary).
        """
        children = self.catalog.unrestrictedSearchResults(
            path={'query': '/'.join(self.container.getPhysicalPath())},
            object_provides=(self.marker.__identifier__,),
            sort_on="path"
        )

        nobj = len(children)
        self.log("Propagating {} to {} children".format(self.fieldname, nobj))

        self.changed = []
        for i, brain in enumerate(children, 1):
            if i % 100 == 0:
                self.log("Done {}/{}".format(i, nobj))

            try:
                obj = brain.getObject()
            except KeyError:
                self.log("KeyError when doing brain.getObject() "
                         "for %s, skipping." % brain.getPath())
                continue

            voc = self.field.bind(obj).source
            value = self.field.get(self.field.interface(obj))
            if value not in voc:
                # Change the child object's field value to a valid one
                # acquired from above
                new_value = acquire_field_value(self.field, obj.aq_parent)
                self.field.set(self.field.interface(obj), new_value)
                if (self.field.__name__ == "retention_period"
                        and IDossierMarker.providedBy(obj)
                        and IDossier(obj).end):
                    obj.reindexObject(idxs=["retention_expiration"])
                self.changed.append(
                    FixedField(brain.getPath(), self.fieldname, value, new_value))

        self.log("Finished propagation")

    def write_csv_log(self):
        csv_log_path = LogFilePathFinder().get_logfile_path(
            'propagate-{}'.format(self.fieldname), extension="csv")

        with open(csv_log_path, "w") as csv_log:
            csv_log.write(
                ', '.join(('Path', 'Field', 'Old value', 'New value')) + '\n')
            for change in self.changed:
                row = [change.path,
                       change.field_name,
                       change.old_value,
                       change.new_value]
                csv_log.write(', '.join(row) + '\n')


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False,
                      help="do not commit changes")

    (options, args) = parser.parse_args()
    if len(args) != 2:
        print "At least 2 arguments are necessary:"
        print "  - A path to the container"
        print "  - A field name ({})".format(fieldname_to_field.keys())
        sys.exit(1)

    path, fieldname = args

    if fieldname not in fieldname_to_field:
        print "Field name must be one of {}".format(fieldname_to_field.keys())
        sys.exit(1)

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    app = setup_app()
    setup_plone(app)

    propagator = RestrictedVocabularyPropagator(path, fieldname)
    propagator.run()

    if not options.dryrun:
        sys.stdout.write("committing ...\n")
        transaction.commit()

    sys.stdout.write("Done.\n")


if __name__ == '__main__':
    main()
