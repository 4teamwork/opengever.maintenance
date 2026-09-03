# -*- coding: utf-8 -*-
from collections import OrderedDict
from ftw.upgrade import ProgressLogger
import csv
import os


class BaseExporter(object):
    """Base class for all CMI export types.

    Subclasses set `key`, `label`, `filename` and `headers`, and override
    `get_items`/`row_for_item`. `id_column` and `reference_columns` are
    optional: if set, `export()` collects both into `exported_ids` /
    `referenced_ids` so the runner can cross-check them across exporters
    once every export has finished.
    """

    key = None
    label = None
    filename = None
    headers = []
    id_column = None
    reference_columns = OrderedDict()

    def __init__(self, portal):
        self.portal = portal
        self.exported_ids = set()
        self.referenced_ids = OrderedDict()

    def get_items(self):
        """Return the list of items to export."""
        return []

    def row_for_item(self, item):
        """Return the CSV row (a list of column values) for a single item."""
        return []

    def export(self, export_dir):
        """Write the CSV file for this exporter into `export_dir`.

        Returns the number of rows written.
        """
        id_index = self.headers.index(self.id_column) if self.id_column else None
        ref_indices = [(self.headers.index(header), target_key)
                        for header, target_key in self.reference_columns.items()]

        path = os.path.join(export_dir, self.filename)
        count = 0
        with open(path, 'wb') as csv_file:
            writer = csv.writer(
                csv_file, delimiter=';', quotechar='"',
                quoting=csv.QUOTE_MINIMAL)
            writer.writerow([column.encode('utf-8') for column in self.headers])
            for item in ProgressLogger(self.label, self.get_items()):
                row = self.row_for_item(item)
                if id_index is not None:
                    self.exported_ids.add(row[id_index])
                for idx, target_key in ref_indices:
                    refs = self.referenced_ids.setdefault(target_key, set())
                    refs.update(value for value in row[idx].split(u'|') if value)
                writer.writerow([value.encode('utf-8') for value in row])
                count += 1
        return count
