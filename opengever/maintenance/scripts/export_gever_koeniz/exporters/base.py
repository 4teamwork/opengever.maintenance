from ftw.upgrade import ProgressLogger
import csv
import os


class BaseExporter(object):
    """Base class for all CMI export types.

    Subclasses set `key`, `label`, `filename` and `headers`, and override
    `get_items`/`row_for_item` to provide the actual data.
    """

    key = None
    label = None
    filename = None
    headers = []

    def __init__(self, portal):
        self.portal = portal

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
        path = os.path.join(export_dir, self.filename)
        count = 0
        with open(path, 'wb') as csv_file:
            writer = csv.writer(
                csv_file, delimiter=';', quotechar='"',
                quoting=csv.QUOTE_MINIMAL)
            writer.writerow([column.encode('utf-8') for column in self.headers])
            for item in ProgressLogger(self.label, self.get_items()):
                writer.writerow(
                    [value.encode('utf-8') for value in self.row_for_item(item)])
                count += 1
        return count
