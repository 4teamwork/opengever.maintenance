"""
Script to fix broken intid references.

    bin/instance run ./scripts/fix_broken_intid_references.py [-n] --check --fix

or with separate check and fix:

   bin/instance run ./scripts/fix_broken_intid_references.py --check -o ./fix-ids
   bin/instance run ./scripts/fix_broken_intid_references.py --fix -o ./fix-ids

The --check will create a json file (and a backup of it) with all broken object paths
within the output directory.
The --fix will then read these paths and fix each object. It will commit the transaction
every few items and will update the the json with the remeaing items.
"""
from five.intid.intid import moveIntIdSubscriber
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import getUtility
from zope.intid import IIntIds
import json
import logging
import os
import sys
import time
import transaction

logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)


class IntIdReferenceFixer(object):

    def __init__(self, portal, dry_run, working_dir, search_path=None):
        self.portal = portal
        self.dry_run = dry_run
        self.working_dir = working_dir
        self.search_path = search_path
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intid_utility = getUtility(IIntIds)
        self.objs_to_fix_path = os.path.join(working_dir, 'objs_to_fix.json')

    def check(self):
        broken = []
        query = {}
        if self.search_path:
            query['path'] = {'query': self.search_path, 'depth': -1}

        for brain in ProgressLogger(
                'Checking for broken intid references',
                self.catalog.unrestrictedSearchResults(**query),
                logger=logger):

            obj = brain.getObject()
            try:
                ref = self.intid_utility.refs.get(self.intid_utility.getId(obj))
            except Exception as e:
                logger.error("Error processing object {}: {}".format(brain.getPath(), e))
                continue

            obj_path = '/'.join(obj.getPhysicalPath())
            if obj_path != ref.path:
                broken.append(obj_path)
                logger.info("Found broken object: {}".format(obj_path))

        if not broken:
            logger.info("Nothing is broken.")
        else:
            logger.info("{} objects are broken.".format(len(broken)))

        logger.info("Writing broken paths to: {}".format(self.objs_to_fix_path))

        self.write_broken_paths(broken, self.objs_to_fix_path)
        self.write_broken_paths(broken, self.objs_to_fix_path + '.backup')

    def fix(self, commit_every=100):
        logger.info("Loading broken paths from: {}".format(objs_to_fix_path))
        broken = self.load_broken_paths(self.objs_to_fix_path)

        if not broken:
            logger.info("There is nothing to fix.")
            return

        fixed = []
        for index, brain in enumerate(ProgressLogger(
                'Fixing broken intid references',
                self.catalog.unrestrictedSearchResults(path={'query': broken, 'depth': 0}),
                logger=logger)):
            try:
                moveIntIdSubscriber(brain.getObject(), None)
            except Exception as e:
                logger.error("Error processing object {}: {}".format(brain.getPath(), e))
                continue

            fixed.append(brain.getPath())

            if index > 0 and index % commit_every == 0:
                self.commit_fixes(broken, fixed)
        self.commit_fixes(broken, fixed)

    def write_broken_paths(self, broken, path):
        with open(path, "w") as file_:
            json.dump(tuple(broken), file_)

    def load_broken_paths(self, path):
        with open(path, "r") as file_:
            return json.load(file_)

    def commit_fixes(self, broken, fixed):
        if self.dry_run:
            return

        logger.info("Committing transaction and removing already fixed objects from the file: {}".format(
            self.objs_to_fix_path))

        transaction.commit()
        remaining = list(set(broken).difference(set(fixed)))
        self.write_broken_paths(remaining, self.objs_to_fix_path)

    def validate(self):
        logger.info("Starting validation checks...")
        broken = self.load_broken_paths(self.objs_to_fix_path)
        if len(broken):
            logger.error("There are still broken objects. See: {}".format(self.objs_to_fix_path))
        else:
            logger.info("Everything processed and fixed.")


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      help="Dry run")
    parser.add_option("--check", action="store_true",
                      help="Check for broken ids")
    parser.add_option("--fix", action="store_true",
                      help="Fix broken ids")
    parser.add_option("--path", dest="path", default=None,
                      help="Restrict operations to a specific path in the catalog")
    parser.add_option(
        '-o', dest='output_directory',
        default='var/maintenance-fix-broken-intid-references-{}'.format(
            time.strftime('%d%m%Y-%H%M%S')),
        help='Path to the output directory')
    (options, args) = parser.parse_args()

    objs_to_fix_path = os.path.join(
        options.output_directory, 'objs_to_fix.json')

    logger.info("Using the following output directory: {}".format(options.output_directory))

    if not options.output_directory:
        logger.info("Invalid output directory")
        sys.exit(1)

    if os.path.isdir(options.output_directory):
        logger.warn("Output directory already exists. Content will be replaced")
    else:
        logger.info("Creating output directory")
        os.mkdir(options.output_directory)

    if options.dry_run:
        logger.info("DRY RUN")
        transaction.doom()

    plone = setup_plone(app, options)
    fixer = IntIdReferenceFixer(
        plone,
        dry_run=options.dry_run,
        working_dir=options.output_directory,
        search_path=options.path
    )

    if options.check:
        fixer.check()

    if options.fix:
        fixer.fix()

    fixer.validate()
