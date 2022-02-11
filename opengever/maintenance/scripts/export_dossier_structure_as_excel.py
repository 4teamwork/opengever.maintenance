from opengever.base.interfaces import IReferenceNumber
from opengever.base.reporter import XLSReporter
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import LogFilePathFinder
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.repository.interfaces import IRepositoryFolder
from plone import api
from zope.globalrequest import getRequest
import sys

"""
This script exports the dossier structure of a leaf repofolder
to an excel file.
    bin/instance run opengever/maintenance/scripts/export_dossier_structure_as_excel.py path_to_repo
"""


class RepoOrFolderItem(object):

    def __init__(self, obj):
        reference_number = IReferenceNumber(obj)
        self.number = reference_number.get_number()
        self.title = obj.Title()
        self.responsible = getattr(obj, "responsible_label", "")
        refnum = reference_number.get_numbers()
        self.depth = (len(refnum['repository']) +
                      len(refnum.get('dossier', [])))

        self.url = self.get_url(obj)

        # we cannot use IReferenceNumberFormatter.sorter, as it does not sort
        # correctly for a mix of repository folders and dossiers. Instead we
        # make our own sorting key
        self.sorting_key = (map(int, refnum.get('repository', tuple())),
                            map(int, refnum.get('dossier', tuple())))

    def get_url(self, obj):
        url_tool = api.portal.get_tool('portal_url')
        public_url = get_current_admin_unit().public_url
        path = "/".join(url_tool.getRelativeContentPath(obj))
        return "/".join([public_url, path])


def generate_report(request, context):

    reference_number = IReferenceNumber(context)
    reference_numbers = reference_number.get_numbers()
    ref_depth = len(reference_numbers['repository']) + len(reference_numbers.get('dossier', []))

    column_map = (
        {'id': 'number', 'title': 'Aktenzeichen'},
        {'id': 'title', 'title': 'Title'},
        {'id': 'responsible', 'title': u'Federfuhrung'},
        {'id': 'url', 'title': u'Pfad'},
        # It seems that 8 folding depths is the limit in excel. To make the most
        # of it we put the root element at the same folding depth as its first
        # children
        {'id': 'depth', 'title': '',
         'fold_by_method': lambda x: max(0, min(x - ref_depth - 1, 7))}
        )

    # We sort these by reference number to preserve user experienced ordering
    brains = api.content.find(context,
                              object_provides=[IDossierMarker.__identifier__,
                                               IRepositoryFolder.__identifier__])
    folders = (brain.getObject() for brain in brains
               if not brain.review_state == 'dossier-state-inactive')
    items = map(RepoOrFolderItem, folders)
    items = sorted(items, key=lambda item: item.sorting_key)

    return XLSReporter(
        request,
        column_map,
        items,
        sheet_title=context.Title().decode('utf-8')
        )()


def main():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print "Missing argument, please provide a path to a leaf repofolder"
        sys.exit(1)

    path = args[0]

    app = setup_app()
    setup_plone(app)

    filename = LogFilePathFinder().get_logfile_path(
        'dossier_structure_export', extension="xlsx")

    context = app.unrestrictedTraverse(path)
    request = getRequest()
    with open(filename, "w") as logfile:
        logfile.write(generate_report(request, context))

    print "done."


if __name__ == '__main__':
    main()
