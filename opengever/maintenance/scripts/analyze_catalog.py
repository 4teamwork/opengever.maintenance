"""
Script to generate statistics of objects in catalog.
USAGE: bin/instance run <path_to>/analyze_catalog.py --start 01-2013 --months 12
"""
from DateTime import DateTime
from datetime import datetime
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_debug_mode
from optparse import OptionParser
import calendar
from opengever.maintenance.utils import join_lines


INPUT_DATEFMT = "%m-%Y"


def parse_options():
    parser = OptionParser()
    parser.add_option("-s", "--site-root", dest="site_root", default=u'/Plone')
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False)
    parser.add_option("-D", "--debug", action="store_true", dest="debug", default=False)
    parser.add_option("--start", dest="start")
    parser.add_option("--months", dest="months")
    (options, args) = parser.parse_args()
    return options


class BrainStatsBuilder(object):
    def __init__(self):
        self.stats = dict()
        self.options = parse_options()
        self.start_month, self.start_year = self.options.start.split('-')
        self.start_date = datetime(year=int(self.start_year),
                                   month=int(self.start_month),
                                   day=1)
        self.plone = globals()['plone']
        self.catalog = self.plone.portal_catalog
        self.site_id = self.plone.id

    def generate_stats(self):
        current_month = self.start_date.month
        current_year = self.start_date.year
        month_delta = 0
        while not month_delta >= int(self.options.months):
            month_key = "%s-%02d" % (current_year, current_month)
            self.stats[month_key] = dict()
            start_range = datetime(current_year, current_month, 1)
            end_day = calendar.monthrange(int(current_year), int(current_month))[1]
            end_range = datetime(current_year, current_month, end_day, 23, 59)

            start_range = DateTime(start_range)
            end_range = DateTime(end_range)


            date_range_query = {'query': (start_range ,end_range), 'range': 'min:max'}

            # Dossiers
            dossiers = self.catalog({"created" : date_range_query,
                                     "object_provides": IDossierMarker.__identifier__})
            self.stats[month_key]['dossiers']= len(dossiers)

            # Documents
            docs = self.catalog({"created" : date_range_query,
                                 "portal_type": 'opengever.document.document'})
            self.stats[month_key]['docs'] = len(docs)

            # Tasks
            tasks = self.catalog({"created" : date_range_query,
                                  "portal_type": 'opengever.task.task'})
            self.stats[month_key]['tasks'] = len(tasks)

            # Mails
            mails = self.catalog({"created" : date_range_query,
                                  "portal_type": 'ftw.mail.mail'})
            self.stats[month_key]['mails'] = len(mails)

            # Total
            total = self.catalog({"created" : date_range_query})
            self.stats[month_key]['total'] = len(total)


            month_delta += 1
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1


        self.stats['TOTAL'] = dict()
        # Dossiers
        all_dossiers = self.catalog({"object_provides": IDossierMarker.__identifier__})
        self.stats['TOTAL']['dossiers']= len(all_dossiers)

        # Documents
        all_docs = self.catalog({"portal_type": 'opengever.document.document'})
        self.stats['TOTAL']['docs'] = len(all_docs)

        # Tasks
        all_tasks = self.catalog({"portal_type": 'opengever.task.task'})
        self.stats['TOTAL']['tasks'] = len(all_tasks)

        # Mails
        all_mails = self.catalog({"portal_type": 'ftw.mail.mail'})
        self.stats['TOTAL']['mails'] = len(all_mails)

        # Total
        all_total = self.catalog()
        self.stats['TOTAL']['total'] = len(all_total)

    @join_lines
    def generate_csv(self):
        site_id = self.site_id
        directorate = site_id.split('-')[0]
        yield "SITE;DIRECTORATE;MONTH;DOSSIERS;DOCS;TASKS;MAILS;TOTAL"
        for month_key in sorted(self.stats.keys()):
            line = "%s;%s;%s;%s;%s;%s;%s;%s" % (
                site_id,
                directorate,
                month_key,
                self.stats[month_key]['dossiers'],
                self.stats[month_key]['docs'],
                self.stats[month_key]['tasks'],
                self.stats[month_key]['mails'],
                self.stats[month_key]['total'],
            )
            yield line


def main():
    setup_debug_mode()
    builder = BrainStatsBuilder()
    builder.generate_stats()

    # Write out catalog stats to CSV
    csv_filename = 'catalog_stats.csv'
    csv_data = builder.generate_csv()
    csv_file = open(csv_filename, 'w')
    csv_file.write(csv_data)
    csv_file.close()
    print "Wrote catalog stats to '%s'." % csv_filename


if __name__ == '__main__':
    main()