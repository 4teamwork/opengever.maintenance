"""
Provides functionality to fix the resolvability of a dossier with already
resolved subdossiers that are now in an 'invalid' state.
Invalid means that some of the documents in a dossier have a document date
outside its dossiers start-end range. This most likely happens when we generate
a journal pdf while closing a dossier with an already defined end-date.

    bin/instance run ./scripts/fix_dossier_resolvability.py <path>

"""
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.dossier.behaviors.dossier import IDossier
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


def fix_dossier_resolvability(plone, path):
    dossier = plone.unrestrictedTraverse(path)
    if not dossier:
        print "Could not find dossier at {}".format(path)
        sys.exit(1)

    catalog = api.portal.get_tool('portal_catalog')
    query = {
        'path': '/'.join(dossier.getPhysicalPath()),
        'portal_type': 'opengever.document.document'
    }

    for brain in catalog.unrestrictedSearchResults(**query):
        fix_document_end_date(brain)


def fix_document_end_date(brain):
    document = brain.getObject()
    dossier_end_date = IDossier(document.get_parent_dossier()).end

    print "Fixing document_date for {} at {}".format(
        brain.Title, brain.getPath())
    if not dossier_end_date:
        return  # uhm, may be not closed yet. not sure what to do.

    document_date = IDocumentMetadata(document).document_date
    if not document_date or document_date > dossier_end_date:
        IDocumentMetadata(document).document_date = dossier_end_date


def main():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print "Expecting exactly one argument: path to dossier"
        sys.exit(1)
    path = args[0]

    if options.dryrun:
        print "dry-run ..."
        transaction.doom()

    app = setup_app()
    plone = setup_plone(app, options)
    fix_dossier_resolvability(plone, path)

    if not options.dryrun:
        print "committing ..."
        transaction.commit()

    print "done."


if __name__ == '__main__':
    main()
