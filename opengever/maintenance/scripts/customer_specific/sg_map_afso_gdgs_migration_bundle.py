"""
This is a copy of sg_map_areg_bdgs_migration_bundle.py with issue-specific
adjustments.

This script is solely intended for transforming the bundle exported as part of
TI-1703 (transforms the bundle in-place).

It deletes all exported repository folders from the bundle, because
in this specific case every single source repository folder is mapped to
an (existing) target repository folder - therefore no complicated logic is
needed to determine which repository folders to keep.

Dossiers are mapped to their target position according to the Excel sheet
from https://4teamwork.atlassian.net/browse/TI-1703
"""

from collections import OrderedDict
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from operator import itemgetter
from os.path import join as pjoin
import argparse
import codecs
import json
import os
import sys
import transaction


# TBD
MAPPING = {
    ((0, 1, 1), ): ((2, 1), ),
    ((0, 1, 2), ): ((2, 1), ),
    ((0, 5, 2, 2), ): ((5, 1), ),
    ((0, 6, 4, 1), ): ((2, 5, 4), ),
    ((0, 6, 4, 3), ): ((2, 5, 4), ),
    ((0, 6, 5, 3), ): ((2, 5, 5), (277, )),
    ((0, 6, 5, 4), ): ((2, 5, 5), (277, )),
    ((0, 6, 5, 5), ): ((2, 5, 5), (277, )),
    ((2, 0), ): ((4, 6, 0), ),
    ((2, 1), ): ((4, 6, 1), ),
    ((2, 2), ): ((4, 6, 2), ),
    ((2, 3, 0), ): ((4, 6, 0), ),
    ((2, 3, 1), ): None,
    ((2, 3, 1, 0), ): ((4, 6, 3, 1, 0), ),
    ((2, 3, 1, 1), ): ((4, 6, 3, 1, 1), ),
    ((2, 3, 1, 2), ): ((4, 6, 3, 1, 2), ),
    ((2, 3, 1, 3), ): ((4, 6, 3, 1, 3), ),
    ((2, 3, 1, 4), ): ((4, 6, 3, 1, 4), ),
    ((2, 3, 1, 5), ): ((4, 6, 3, 1, 5), ),
    ((2, 3, 1, 6), ): ((4, 6, 3, 1, 6), ),
    ((2, 3, 1, 9), ): ((4, 6, 3, 1, 9), ),
    ((2, 3, 2), ): None,
    ((2, 3, 2, 0), ): ((4, 6, 3, 2, 0), ),
    ((2, 3, 2, 1), ): ((4, 6, 3, 2, 1), ),
    ((2, 3, 2, 2), ): ((4, 6, 3, 2, 2), ),
    ((2, 3, 2, 3), ): ((4, 6, 3, 2, 3), ),
    ((2, 3, 2, 4), ): ((4, 6, 3, 2, 4), ),
    ((2, 3, 2, 5), ): ((4, 6, 3, 2, 5), ),
    ((2, 3, 2, 9), ): ((4, 6, 3, 2, 9), ),
    ((2, 3, 3), ): None,
    ((2, 3, 3, 0), ): ((4, 6, 3, 3, 0), ),
    ((2, 3, 3, 1), ): ((4, 6, 3, 3, 1), ),
    ((2, 3, 3, 2), ): ((4, 6, 3, 3, 2), ),
    ((2, 3, 3, 3), ): ((4, 6, 3, 3, 3), ),
    ((2, 3, 3, 4), ): ((4, 6, 3, 3, 4), ),
    ((2, 3, 3, 5), ): ((4, 6, 3, 3, 5), ),
    ((2, 3, 3, 6), ): ((4, 6, 3, 3, 6), ),
    ((2, 3, 3, 9), ): ((4, 6, 3, 3, 9), ),
    ((2, 3, 5), ): ((4, 6, 3, 4), ),
    ((2, 4), ): ((4, 6, 4), ),
    ((2, 5), ): ((5, 2), ),
    ((8, 6), ): ((5, 0), ),
}


class BundleMapper(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.bundle_dir = self.options.bundle_path
        self.unmapped_dossiers = []

    def run(self):
        self.map_dossiers()
        self.remove_repofolders()

    def order_dict(self, data):
        ordered = OrderedDict()
        ordered['guid'] = data.pop('guid')

        parent_guid = data.pop('parent_guid', None)
        permissions = data.pop('_permissions', None)

        if parent_guid:
            ordered['parent_guid'] = parent_guid

        ordered.update(OrderedDict(sorted(data.items())))

        if permissions:
            ordered['_permissions'] = permissions

        return ordered

    def map_dossiers(self):
        dossiers_json_path = pjoin(self.bundle_dir, 'dossiers.json')
        with open(dossiers_json_path) as json_file:
            dossier_items = json.load(json_file)

        dossier_items = map(self.order_dict, dossier_items)
        dossier_items.sort(key=itemgetter('guid'))

        for dossier in dossier_items:
            parent_reference = dossier.get('parent_reference')
            if not parent_reference:
                continue
            parent_reference = map(tuple, parent_reference)
            parent_reference = tuple(parent_reference)

            new_reference = MAPPING.get(parent_reference)
            if new_reference:
                print('Mapped %r to %r for %s' % (
                    parent_reference, new_reference, dossier['guid']))
                dossier['parent_reference'] = new_reference
            else:
                self.unmapped_dossiers.append(dossier)

        if self.unmapped_dossiers:
            print("Unmapped parent references:")
            print("Parent reference; GUID; Reference Nr.")
            for dossier in self.unmapped_dossiers:
                print(';'.join([
                    json.dumps(dossier.get('parent_reference')),
                    dossier.get('guid'),
                    dossier.get('former_reference_number')]))
            raise Exception("There are unmapped dossiers.")

        self.dump_to_jsonfile(dossier_items, dossiers_json_path)

    def remove_repofolders(self):
        os.remove(pjoin(self.bundle_dir, 'repofolders.json'))
        os.remove(pjoin(self.bundle_dir, 'reporoots.json'))

    def dump_to_jsonfile(self, data, json_path):
        with open(json_path, 'wb') as jsonfile:
            json.dump(
                data,
                codecs.getwriter('utf-8')(jsonfile),
                ensure_ascii=False,
                indent=4,
                separators=(',', ': ')
            )


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument('bundle_path', help='Path to bundle')

    options = parser.parse_args(sys.argv[3:])

    transaction.doom()
    plone = setup_plone(app, options)

    generator = BundleMapper(plone, options)
    generator.run()
