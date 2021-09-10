"""
Initializes the title_fr field for all concerned portal_types. The script can
build up a glossary from a repository excel file and use it to translate the
titles of repository roots and folders. For other portal_types it uses a default
glossary. When it does not find a given title in the glossary it simply uses the
German title instead. If a French title is already set, it skips that object.

    bin/instance run ./scripts/initialize_title_fr.py -i xls_path

optional arguments:
  -i : path to a repository excel file. This will be used to build up
       a glossary to translate titles for repository root and folders
  -n : dry-run.

"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.setup.sections.xlssource import XlsSource
from plone import api
import os
import sys
import transaction


class SingleXlsSource(XlsSource):

    def __init__(self, xls_path):
        self.xls_path = xls_path

    def __iter__(self):
        if not os.path.isfile(self.xls_path):
            print "invalid file", self.xls_path
            return

        locals()['__traceback_info__'] = self.xls_path

        keys, sheet_data = self.read_excel_file(self.xls_path)
        for rownum, row in enumerate(sheet_data):
            yield self.process_row(row, rownum, keys, '')


class TitleInitializor(object):

    portal_types = [
        "opengever.private.root",
        "opengever.inbox.container",
        "opengever.inbox.inbox",
        "opengever.meeting.committeecontainer",
        "opengever.dossier.templatefolder",
        "opengever.contact.contactfolder",
        "opengever.workspace.root",
        ]

    repo_portal_types = [
        "opengever.repository.repositoryroot",
        "opengever.repository.repositoryfolder",
        ]

    glossary = {
            u"Meine Ablage": u'Mon d\xe9p\xf4t',
            u"Eingangskorb": u'Bo\xeete de r\xe9ception',
            u"Vorlagen": u'Mod\xe8les',
            u"Kontakte": u"Contactes",
            u'Teamr\xe4ume': u"Espaces de travail",
            u"Sitzungen": u'S\xe9ances',
        }

    def __init__(self, path_to_excel):
        self.path_to_excel = path_to_excel

    def __call__(self):
        self.prepare_glossary()
        self.initialize_titles()

    def prepare_glossary(self):
        self.repo_glossary = {}

        if not self.path_to_excel:
            return

        for item in SingleXlsSource(self.path_to_excel):
            title_de = item.get('effective_title')
            title_fr = item.get('effective_title_fr')
            if title_de and title_fr:
                self.repo_glossary[title_de] = title_fr

    def initialize_titles(self):
        catalog = api.portal.get_tool('portal_catalog')
        for brain in catalog.unrestrictedSearchResults(portal_type=self.portal_types):
            obj = brain.getObject()
            if obj.title_fr:
                continue
            obj.title_fr = self.glossary.get(obj.title_de, obj.title_de)
            obj.reindexObject(idxs=["UID", "title_fr"])

        for brain in catalog.unrestrictedSearchResults(portal_type=self.repo_portal_types):
            obj = brain.getObject()
            if obj.title_fr:
                continue
            obj.title_fr = self.repo_glossary.get(obj.title_de, obj.title_de)
            obj.reindexObject(idxs=["UID", "title_fr"])


def main():
    parser = setup_option_parser()
    parser.add_option('-i', dest='xls_path', default=None,
                      help='Path to the repository xlsx')
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)

    (options, args) = parser.parse_args()

    if len(args) != 0:
        print "Script does not take any positional arguments"
        sys.exit(1)

    if options.dryrun:
        print "Dry run, dooming transaction"
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    print '\nStarting title initialization...\n'
    TitleInitializor(options.xls_path)()

    if not options.dryrun:
        print '\nCommitting transaction...\n'
        transaction.commit()
        print '\nDone!\n'


if __name__ == '__main__':
    main()
