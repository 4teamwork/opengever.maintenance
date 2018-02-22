"""
Script to fix mails where the cached message attribute 'mail._message' is
missing.

    bin/instance run ./scripts/fix_mail_message_caches.py

"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.namedfile.file import NamedBlobFile
from Products.CMFPlone.utils import base_hasattr
import transaction


def fix_mail_message_caches(plone, options):
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(portal_type='ftw.mail.mail')

    missing_cache = []
    fixed = []

    for brain in brains:
        obj = brain.getObject()
        try:
            obj._message
        except AttributeError:
            # Attribute for cached message is not present, fix it
            path = '/'.join(obj.getPhysicalPath())
            print "Missing cached _message: %s" % path
            missing_cache.append(obj)

            if 'message' not in obj.__dict__:
                print "WARNING: %s is missing message" % path
                continue

            message = obj.__dict__['message']

            if not isinstance(message, NamedBlobFile):
                print "WARNING: message is not a NamedBlobFile"
                continue

            if not options.dryrun:
                # Do exactly what the OGMail.message setter does...
                obj._message = message
                obj._update_attachment_infos()
                obj._reset_header_cache()

                # ... and update the title attribute if necessary.
                # (Don't overwrite it though it custom title has been set)
                if not base_hasattr(obj, '_title'):
                    obj._update_title_from_message_subject()

                print "Fixed _message cache for %s" % path
                fixed.append(obj)

                # Reindex mail if necessary
                if brain.Title == 'no_subject':
                    obj.reindexObject(
                        idxs=['Title', 'sortable_title', 'getContentType'])
                    print "Reindexed %s" % path

    print "%s mails total missing cached _message" % len(missing_cache)
    print "%s mails fixed" % len(fixed)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    if options.dryrun:
        print 'dryrun ...'
        transaction.doom()

    fix_mail_message_caches(plone, options)

    if not options.dryrun:
        transaction.get().note("Fix mail message caches")
        transaction.commit()
