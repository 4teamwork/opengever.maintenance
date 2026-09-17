# -*- coding: utf-8 -*-
"""
TI-4087

Exports the whole Ordnungssystem (repository folders, dossiers and
subdossiers) as an xlsx file. Each row is one item; its ancestor titles are
flattened into "Ebene 1" .. "Ebene n" columns (n = deepest nesting level
found).

Usage:

    bin/instance run ti-4087.py [--export-path /tmp/]
"""
from opengever.base.reporter import XLSReporter
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance import dm
from opengever.repository.interfaces import IRepositoryFolder
from zope.globalrequest import getRequest
from zope.globalrequest import setRequest
import argparse
import logging
import os
import sys
import time
import transaction


logger = logging.getLogger('opengever.maintenance.ti_4087')
logging.root.addHandler(logging.StreamHandler(stream=sys.stdout))
logging.root.setLevel(logging.INFO)


def get_relative_path(portal, physical_path):
    portal_path = '/'.join(portal.getPhysicalPath())
    if physical_path.startswith(portal_path):
        return physical_path[len(portal_path):]
    return physical_path


def build_rows(portal):
    catalog = portal.portal_catalog
    brains = catalog.unrestrictedSearchResults(
        object_provides=[
            IRepositoryFolder.__identifier__,
            IDossierMarker.__identifier__,
        ],
        sort_on='path',
    )

    titles_by_path = {}
    rows = []
    max_depth = 0

    for brain in brains:
        path = brain.getPath()
        parent_path = path.rsplit('/', 1)[0]
        titles = titles_by_path.get(parent_path, []) + [brain.Title]
        titles_by_path[path] = titles
        max_depth = max(max_depth, len(titles))

        row = {
            'uid': brain.UID,
            'portal_type': brain.portal_type,
            'path': get_relative_path(portal, path),
        }
        for depth, title in enumerate(titles, 1):
            row['ebene_{}'.format(depth)] = title
        rows.append(row)

    return rows, max_depth


def build_column_map(max_depth):
    column_map = [
        {'id': 'uid', 'title': u'UID'},
        {'id': 'portal_type', 'title': u'Typ'},
        {'id': 'path', 'title': u'Pfad'},
    ]
    for depth in range(1, max_depth + 1):
        column_map.append({
            'id': 'ebene_{}'.format(depth),
            'title': u'Ebene {}'.format(depth),
            'default': u'',
        })
    return column_map


def main():
    parser = argparse.ArgumentParser(
        description='Export the Ordnungssystem (repository folders, '
                     'dossiers and subdossiers) as xlsx.')
    parser.add_argument(
        '--export-path', dest='export_path', default='/tmp/',
        help='Directory the export file is written to. Default: /tmp/.')
    options = parser.parse_args(sys.argv[3:])

    rows, max_depth = build_rows(plone)
    column_map = build_column_map(max_depth)

    data = XLSReporter(
        getRequest(),
        column_map,
        rows,
        sheet_title=u'Ordnungssystem',
    )()

    filename = os.path.join(
        options.export_path,
        'ti-4087_export_{}.xlsx'.format(int(time.time())))
    with open(filename, 'wb') as xlsx_file:
        xlsx_file.write(data)

    logger.info(u'Exported %s rows (max depth %s) to %s',
                len(rows), max_depth, filename)


if __name__ == '__main__':
    dm()  # Sets up the `plone` global, using the sole Plone site.
    setRequest(plone.REQUEST)
    transaction.doom()  # This export is read-only.

    main()
