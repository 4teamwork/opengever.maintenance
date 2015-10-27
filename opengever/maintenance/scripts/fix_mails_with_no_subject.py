from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


SEPARATOR = '-' * 78


def fix_mails_with_no_subject(options):
    """Find and reindex all mails with `no_subject` in any of the relevant
    indexes or metadata.
    """
    commit_needed = False

    affected_mail_brains = []

    catalog = api.portal.get_tool('portal_catalog')
    mail_brains = catalog.unrestrictedSearchResults(
        portal_type='ftw.mail.mail')

    # Check metadata
    for brain in mail_brains:
        if 'no_subject' in brain.Title:
            affected_mail_brains.append(brain)
        if 'no_subject' in str(brain.breadcrumb_titles):
            affected_mail_brains.append(brain)

    # Check indexes
    affected_mail_brains.extend(
        catalog.unrestrictedSearchResults(sortable_title='no_subject'))
    affected_mail_brains.extend(
        catalog.unrestrictedSearchResults(Title='no_subject'))
    affected_mail_brains.extend(
        catalog.unrestrictedSearchResults(SearchableText='no_subject'))

    affected_urls = set([b.getURL() for b in set(affected_mail_brains)])

    print "Mails with 'no_subject':"
    for url in affected_urls:
        print url

    if options.dry_run:
        return

    affected_mails = set([b.getObject() for b in set(affected_mail_brains)])

    if len(affected_mails) > 0:
        commit_needed = True
        print ("Reindexing indexes 'sortable_title', 'Title', "
               "'breadcrumb_titles', 'SearchableText' for mails with "
               "'no_subject':")

    for obj in affected_mails:
        print "Reindexing mail {}".format(obj.absolute_url())
        catalog.reindexObject(
            obj,
            idxs=['sortable_title', 'Title', 'breadcrumb_titles',
                  'SearchableText'],
            update_metadata=1)

    if commit_needed:
        if not options.dry_run:
            transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    print "Date: {}".format(datetime.now().isoformat())
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    fix_mails_with_no_subject(options)


if __name__ == '__main__':
    main()
