"""
Stores all documents since the given date in Bumblebee.

    bin/instance run bumblebee_store_since.py -n 2018-03-28

"""
from datetime import datetime
from ftw.bumblebee.interfaces import IBumblebeeable
from ftw.bumblebee.interfaces import IBumblebeeConverter
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import getUtility
import logging
import sys
import transaction


log = logging.getLogger('opengever.maintenance')
log.setLevel(logging.INFO)
log.root.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


def get_documents_to_store(date_range):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document', created=date_range)
    return brains


def store_since(since, options):
    date_range = {'query': (since,), 'range': 'min'}
    brains = get_documents_to_store(date_range)
    print "Considering %s documents total" % len(brains)

    for brain in brains:
        print "Considering: %s - %s" % (brain.created, brain.getPath())

    if not options.dryrun:
        converter = getUtility(IBumblebeeConverter)

        # Patch GeverBumblebeeConverter's batch_query so we can limit
        # by date range instead of storing ALL documents
        query = {'object_provides': IBumblebeeable.__identifier__}
        query['created'] = date_range
        converter.batch_query = query

        converter.store(deferred=True, reset_timestamp=True)
    return


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if len(args) != 1:
        print "Must supply exactly one argument (<since>, in %Y-%m-%d)"
        print "Usage: bin/instance run bumblebee_store_since.py -n <since>"
        sys.exit(1)

    since = datetime.strptime(args[0], '%Y-%m-%d')
    print "Considering documents since %s" % since

    setup_plone(app, options)

    if options.dryrun:
        print 'dryrun ...'
        transaction.doom()

    store_since(since, options)

    if not options.dryrun:
        transaction.commit()
