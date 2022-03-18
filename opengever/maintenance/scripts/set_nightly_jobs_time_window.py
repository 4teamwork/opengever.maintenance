"""
Allows to set start and/or end time of nightly jobs time window.

The <value> is supposed to be specified as 'hh:mm', e.g. '23:45'

Example Usage:

    bin/instance run set_nightly_jobs_time_window.py --start '01:00' --end '05:00'
"""
from datetime import timedelta
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.nightlyjobs.interfaces import INightlyJobsSettings
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
import argparse
import re
import sys
import transaction


HOURS_MINUTES = re.compile(r'^([0-9]{2}):([0-9]{2})$')


def to_timedelta(value):
    match = HOURS_MINUTES.match(value)
    if not match:
        raise ValueError('Value %r does not match pattern %r' % (
            value, HOURS_MINUTES.pattern))

    hours, minutes = map(int, match.groups())
    return timedelta(hours=hours, minutes=minutes)


def set_time_window(plone, args):
    registry = getUtility(IRegistry)
    nightly_settings = registry.forInterface(INightlyJobsSettings)

    if args.start:
        nightly_settings.start_time = to_timedelta(args.start)
        print("Set start time to %s" % nightly_settings.start_time)

    if args.end:
        nightly_settings.end_time = to_timedelta(args.end)
        print("Set end time to %s" % nightly_settings.end_time)


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Start time")
    parser.add_argument("--end", help="End time")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    set_time_window(plone, args)
    transaction.commit()
