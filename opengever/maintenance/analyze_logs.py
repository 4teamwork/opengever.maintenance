from collections import Counter
from datetime import datetime
from datetime import timedelta
from opengever.maintenance.utils import join_lines
from subprocess import CalledProcessError
import apachelog
import os
import re
import subprocess
import sys


VERBOSE = False

OLD_BUILDOUT_NAMING_SCHEME = re.compile(r'plone[0-9]*-(.*)$')
NEW_BUILDOUT_NAMING_SCHEME = re.compile(r'[0-9]*plone-(.*)$')

OG_CLIENT_SCHEMA = re.compile(r'(.*)-(.*)')



stats = dict()

INPUT_DATEFMT = "%d-%m-%Y"
LOG_DATEFMT = "%d/%b/%Y:%H:%M:%S"
FILTER_TYPES = ['.css', '.js', '.png', '.gif', '.kss']
FILTER_TERMS = ['livesearch_reply', '@@search?SearchableText', 'transition-resolve', 'transition-archive', 'listing?ajax_load']
FILTER_USERS = ['TE1COC', 'Anonymous', 'zopemaster', 'lukas.graf@4teamwork.ch', 'philippe.gross@4teamwork.ch']


class LogParser(apachelog.parser):
    def alias(self, name):
        aliases = {'%h': 'host',
                   '%l': 'ident',
                   '%u': 'userid',
                   '%t': 'time',
                   '%r': 'request',
                   '%>s': 'status',
                   '%b': 'bytes',
                   '%{User-agent}i': 'user_agent',
                   '%{Referer}i': 'referer'}
        return aliases.get(name, name)


def merge_logs(start_date, logdir):
    start_date_str = start_date.strftime("%b/%Y")
    pkg_dir = os.path.dirname(__file__)
    script_path = os.path.join(pkg_dir, 'scripts', 'logresolvemerge.pl')
    print "Merging logs in %s..." % logdir
    cmd = "perl %s %s/instance?-Z2.log > %s/merged.log" % (script_path, logdir, logdir)
    out = subprocess.check_output(cmd, shell=True)
    print out

    # Truncate logs
    print "Truncating logs..."

    partial_logfile = False
    cmd = "grep '%s' %s/merged.log" % (start_date_str, logdir)
    try:
        out = subprocess.check_output(cmd, shell=True)
    except CalledProcessError:
        # No match -> logfile probably starts after start_date
        partial_logfile = True

    if not partial_logfile:
        cmd = "grep -A 99999999 '%s' %s/merged.log > %s/merged_truncated.log" % (start_date_str, logdir, logdir)
        out = subprocess.check_output(cmd, shell=True)
        print out

        # Move truncated file back, overwriting original
        os.rename("%s/merged_truncated.log" % logdir, "%s/merged.log" % logdir)


def analyze_log(start_date, end_date, logdir, site_id, directorate):
    """Parse Z2 log (common log format).
    Example line:
    10.11.111.43 - Anonymous [31/Dec/2012:17:11:28 +0200] "GET /di-kes/portal_css/some.css HTTP/1.1" 200 2204 "http://0000oglx10:14301/di-kes/referer" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:17.0) Gecko/20100101 Firefox/17.0"
    """
    format = apachelog.formats['extended']
    parser = LogParser(format)

    lineno = 0
    logfile = open("%s/merged.log" % logdir, 'r')
    for line in logfile:
        lineno += 1
        if lineno % 10000 == 0:
            sys.stderr.write("Parsing line %s...\n" % lineno)
        filter_line = False
        try:
            data = parser.parse(line.strip())
        except apachelog.ApacheLogParserError:
            if VERBOSE:
                print "WARNING: Couldn't parse line: \n%s" % line
            continue

        request_pattern = re.compile(r'([A-Z]*) (.*) (HTTP/.*)')
        method, path, http = request_pattern.match(data['request']).groups()

        # Strip UTC offset because using %z with strptime doesn't work
        time = re.match('\[(.*?) \+0200\]', data['time']).group(1)
        log_time = datetime.strptime(time, LOG_DATEFMT)

        # Filter users we don't care about
        if any([u in data['userid'] for u in FILTER_USERS]):
            continue

        # Stop parsing at end of date range
        if log_time > end_date:
            break

        # Filter by date range
        if not(log_time > start_date and log_time < end_date):
            continue

        # Filter types we're not interested in
        for filter_type in FILTER_TYPES:
            if path.endswith(filter_type) or "%s?" % filter_type in path:
                filter_line = True
        for filter_term in FILTER_TERMS:
            if filter_term in path:
                filter_line = True

        if filter_line:
            continue


        month_key = log_time.strftime('%Y-%m')
        if not month_key in stats:
            stats[month_key] = dict(users=Counter(), views=0, top_user_name='', top_user_views=0, ee=0)

        stats[month_key]['users'][data['userid']] += 1
        stats[month_key]['views'] += 1

        ee_match = re.match('.*externalEdit.*$', path)
        if ee_match and not data['status'] == 302:
            stats[month_key]['ee'] += 1

    logfile.close()

    # Write out stats to CSV
    csv_filename = 'log_stats.csv'
    csv_data = generate_csv(stats, site_id, directorate)
    csv_file = open(csv_filename, 'w')
    csv_file.write(csv_data)
    csv_file.close()
    print "Wrote logfile stats to '%s'." % csv_filename


@join_lines
def generate_csv(stats, site_id, directorate):
    yield "SITE;DIRECTORATE;MONTH;VIEWS;USERS;TOP_USER_1_NAME;TOP_USER_1_VIEWS;TOP_USER_2_NAME;TOP_USER_2_VIEWS;TOP_USER_3_NAME;TOP_USER_3_VIEWS;EE"
    for month_key in sorted(stats.keys()):
        top_users = stats[month_key]['users'].most_common(3)
        while not len(top_users) == 3:
            top_users.append(('',0))
        stats[month_key]['top_user_1_name'] = top_users[0][0]
        stats[month_key]['top_user_1_views'] = top_users[0][1]
        stats[month_key]['top_user_2_name'] = top_users[1][0]
        stats[month_key]['top_user_2_views'] = top_users[1][1]
        stats[month_key]['top_user_3_name'] = top_users[2][0]
        stats[month_key]['top_user_3_views'] = top_users[2][1]
        #print month_key
        num_users = len(stats[month_key]['users'].keys())
        values = [
            site_id,
            directorate,
            month_key,
            stats[month_key]['views'],
            num_users,
            stats[month_key]['top_user_1_name'],
            stats[month_key]['top_user_1_views'],
            stats[month_key]['top_user_2_name'],
            stats[month_key]['top_user_2_views'],
            stats[month_key]['top_user_3_name'],
            stats[month_key]['top_user_3_views'],
            stats[month_key]['ee'],
            ]
        for i, value in enumerate(values):
            values[i] = str(value)
        yield ';'.join(values)



def get_names():
    """
    XXX: Refactor me!
    """
    cwd = os.getcwd()
    bin_script_path = os.path.join(cwd, sys.argv[0])
    buildout_dir = os.path.dirname(os.path.dirname(bin_script_path))
    logdir = os.path.join(buildout_dir, 'var', 'log')

    buildout_name = os.path.basename(buildout_dir)

    match = OLD_BUILDOUT_NAMING_SCHEME.match(buildout_name)
    if match is None:
        # Try new buildout naming scheme (01-plone-foo-bar)
        match = NEW_BUILDOUT_NAMING_SCHEME.match(buildout_name)
        if match is None:
            raise Exception("Could not determine directorate from buildout name '%s'" %
                            buildout_name)

    site_id = match.group(1)
    directorate = OG_CLIENT_SCHEMA.match(site_id).group(1)
    return (logdir, site_id, directorate)


def main():
    if not len(sys.argv) == 3:
        print "Usage: bin/analyze-logs <start date> <end date>"
        print "Example: bin/analze-logs 01-03-2013 01-07-2013"
        sys.exit(1)

    start = sys.argv[1]
    end = sys.argv[2]

    start = datetime.strptime(start, INPUT_DATEFMT)
    end = datetime.strptime(end, INPUT_DATEFMT) + timedelta(days=1)

    logdir, site_id, directorate = get_names()

    merge_logs(start, logdir)
    analyze_log(start, end, logdir, site_id, directorate)

