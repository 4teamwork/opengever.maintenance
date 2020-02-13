"""
Script to fix a particular incident on a customer site where 1500 copies
of a document have accidentally been created and checked out by a single
user.

This script will
  - detect the duplicates
  - cancel the checkout for them
  - move them to the trash


Usage procedure:

  - Update the global constants in this script with the appropriate values
    (except the EXPECTED_HASH)

  - Run the script in dry run mode:
      bin/instance run trash_duplicate_documents.py -n

  - Validate that the list of duplicate documents is correct

  - Update note the displayed hash, and update EXPECTED_HASH

  - Run the script again (non-dry-run):
      bin/instance run trash_duplicate_documents.py
"""
from Acquisition import aq_parent
from DateTime import DateTime
from hashlib import sha224
from opengever.document.interfaces import ICheckinCheckoutManager
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.trash.trash import ITrashable
from plone import api
from zope.component import getMultiAdapter
from zope.globalrequest import getRequest
import argparse
import sys
import transaction


AFFECTED_USERID = 'REPLACEME'
PATH_TO_ORIGINAL = '/REPLACE/ME'

TIME_RANGE_START = DateTime('2020-02-13 11:20 GMT+1')
TIME_RANGE_END = DateTime('2020-02-13 11:30 GMT+1')

EXPECTED_HASH = None


def checked_out_by(userid):
    catalog = api.portal.get_tool('portal_catalog')
    return catalog.unrestrictedSearchResults(checked_out=userid)


def find_duplicates(plone, options):
    original_doc = plone.restrictedTraverse(PATH_TO_ORIGINAL)
    original_title = original_doc.title
    affected_dossier = aq_parent(original_doc)

    checked_out_by_affected_user = checked_out_by(AFFECTED_USERID)

    duplicates = []
    for brain in checked_out_by_affected_user:
        if brain.getPath() == PATH_TO_ORIGINAL:
            print "Skipping original: %r (%r)" % (brain.getPath(), brain.Title)
            continue

        obj = brain.getObject()
        if obj.title != original_title:
            print "Skipping document with non-matching title: %r (%r)" % (obj, brain.Title)
            continue

        if not TIME_RANGE_START < obj.created() < TIME_RANGE_END:
            print "Skipping document outside time range: %r (%r)" % (obj, obj.created())
            continue

        if aq_parent(obj) != affected_dossier:
            print "Skipping document outside affected dossier: %r" % obj
            continue

        duplicates.append(obj)

    return duplicates


def trash_and_cancel_checkouts(duplicates):
    for doc in duplicates:
        path = '/'.join(doc.getPhysicalPath())

        print "\nCancelling checkout for %r" % path
        manager = getMultiAdapter((doc, getRequest()), ICheckinCheckoutManager)
        manager.cancel()

        print "Trashing %r" % path
        ITrashable(doc).trash()


def fix_duplicate_document_mess(plone, options):
    print "User ID: %r" % AFFECTED_USERID
    print "Plone: %r" % plone
    print "Original: %r" % PATH_TO_ORIGINAL

    num_checked_out = len(checked_out_by(AFFECTED_USERID))
    print "Affected user has %r checked out documents." % num_checked_out

    duplicates = find_duplicates(plone, options)

    # Build sorted list of paths and hash it
    duplicate_paths = ['/'.join(dup.getPhysicalPath()) for dup in duplicates]
    duplicate_paths.sort()
    duplicate_paths_hash = sha224('|'.join(duplicate_paths)).hexdigest()

    # Display identified duplicates, their hash and count
    print "\nIdentified duplicates:"
    for dup in duplicates:
        escaped_title = dup.title.replace(',', '\\,')
        path = '/'.join(dup.getPhysicalPath())
        print '%s,"%s"' % (path, escaped_title)
    print
    print "Hash of duplicate_paths: %s" % duplicate_paths_hash
    print "%s duplicates" % len(duplicates)

    if options.dry_run:
        print "DRY-RUN"
        print "You can now copy the hash above and update EXPECTED_HASH"
        return

    if not EXPECTED_HASH:
        print "ERROR: Must specify an expected hash of duplicate paths"
        sys.exit(1)

    if duplicate_paths_hash != EXPECTED_HASH:
        print "ERROR: duplicate_paths_hash does not match EXPECTED_HASH"
        sys.exit(1)

    trash_and_cancel_checkouts(duplicates)

    print "All done."

    num_checked_out = len(checked_out_by(AFFECTED_USERID))
    print "Affected user has %r checked out documents." % num_checked_out


if __name__ == '__main__':
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="Dry run")

    options = parser.parse_args(sys.argv[3:])

    if options.dry_run:
        print "DRY RUN"
        transaction.doom()

    plone = setup_plone(app, options)

    fix_duplicate_document_mess(plone, options)

    if not options.dry_run:
        transaction.get().note(
            "Cancel checkout for and trash accidentally duplicated documents.")
        transaction.commit()
