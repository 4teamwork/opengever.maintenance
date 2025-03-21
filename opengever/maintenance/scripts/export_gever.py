"""
Gever Site Exporter

This script is designed to export a gever, including its repofolder and dossier
structure and documents, to a specified directory and package it into a ZIP file.

Here is a list of all portal types which will be exported:

- 'opengever.repository.repositoryroot'
- 'opengever.repository.repositoryfolder'
- 'opengever.dossier.businesscasedossier'
- 'opengever.meeting.meetingdossier'
- 'opengever.document.document'
- 'opengever.meeting.proposal'
- 'ftw.mail.mail'

And here the remaining, not managed portal types:

- 'opengever.inbox.forwarding'
- 'opengever.dossier.templatefolder'
- 'opengever.workspace.meeting'
- 'opengever.meeting.proposal'
- 'opengever.workspace.folder'
- 'opengever.task.task'
- 'opengever.meeting.committeecontainer'
- 'opengever.inbox.yearfolder'
- 'opengever.meeting.proposaltemplate'
- 'opengever.meeting.committee'
- 'opengever.dossier.dossiertemplate'
- 'opengever.tasktemplates.tasktemplate'
- 'opengever.meeting.sablontemplate'
- 'opengever.meeting.meetingtemplate'
- 'opengever.meeting.submittedproposal'
- 'opengever.tasktemplates.tasktemplatefolder'
- 'opengever.private.root'
- 'opengever.workspace.root'
- 'opengever.private.folder'
- 'opengever.disposition.disposition'
- 'opengever.meeting.paragraphtemplate'
- 'opengever.meeting.period'
- 'opengever.workspace.workspace'
- 'opengever.inbox.inbox'
- 'opengever.private.dossier'
- 'opengever.inbox.container'

Usage:

Help: bin/instance run export_gever.py -h


"""
from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.i18n.normalizer.interfaces import IFileNameNormalizer
from time import time
from zope.component import getUtility
import argparse
import os
import shutil
import sys
import transaction


class GeverExporter:
    def __init__(
            self,
            export_base_dir,
            root_node_path,
            create_zip=False,
            just_stats=False):
        self.portal = api.portal.get()
        self.export_base_dir = export_base_dir
        self.root_node_path = root_node_path

        ts = datetime.now().strftime('%Y-%d-%m_%H_%M_%S')
        self.export_dir_name = 'export_{}'.format(ts)
        self.export_dir = os.path.join(self.export_base_dir,
                                       self.export_dir_name)
        self.create_folder(self.export_dir)

        self.zip_filename = self.export_dir_name
        self.create_zip = create_zip
        self.just_stats = just_stats

        self.catalog = api.portal.get_tool('portal_catalog')
        self.path_mapping = {}

        self.skipped_documents_by_missing_blob = []
        self.skipped_documents_by_missing_parent = []

    def query_folderish_brains(self):
        return self.catalog.unrestrictedSearchResults(
            sort_on="path",
            path=self.root_node_path,
            portal_type=[
                'opengever.repository.repositoryroot',
                'opengever.repository.repositoryfolder',
                'opengever.dossier.businesscasedossier',
                'opengever.meeting.meetingdossier',
                'opengever.meeting.proposal',
            ])

    def query_document_brains(self):
        return self.catalog.unrestrictedSearchResults(
            sort_on="path",
            path=self.root_node_path,
            trashed=False,
            portal_type=[
                'opengever.document.document',
                'ftw.mail.mail',
            ])

    def print_statistics(self, folders, documents):
        total_folders = len(folders)
        total_documents = len(documents)

        total_size = 0
        for document in documents:
            # Use the filesize index to get the size of the document
            total_size += document.filesize

        total_size_mb = total_size / (1024 * 1024)  # MB

        print("Total Folders: {}".format(total_folders))
        print("Total Documents: {}".format(total_documents))
        print("Total Size of Documents: {:.2f} MB".format(total_size_mb))

    def copy_document_to_export_dir(self, doc_obj):
        doc_path = '/'.join(doc_obj.getPhysicalPath())
        parent_path = os.path.join(
            self.path_mapping.get(os.path.dirname(doc_path)))

        if not parent_path:
            self.skipped_documents_by_missing_parent.append(doc_path)
            return

        if hasattr(doc_obj, 'file') and doc_obj.file:
            blob = doc_obj.file
            document_title = self.normalize_title(blob.filename)
        elif hasattr(doc_obj, 'message') and doc_obj.message:
            # Mails
            blob = doc_obj.message
            document_title = self.normalize_title(blob.filename)
        else:
            self.skipped_documents_by_missing_blob.append(doc_path)
            return

        with open(os.path.join(parent_path, document_title), 'wb') as f:
            f.write(blob.data)

    def create_export(self, folders, documents):
        total_folders = len(folders)
        total_documents = len(documents)

        print("Start exporing structure...")

        # Export folders
        exported_items = 0
        for folder in folders:
            folder_title = self.normalize_title(folder.Title or "Untitled")
            folder_path = os.path.join(
                self.path_mapping.get(os.path.dirname(folder.getPath()),
                                      self.export_dir),
                folder_title)
            self.path_mapping[folder.getPath()] = folder_path

            self.create_folder(folder_path)

            # Update progress
            exported_items += 1
            if exported_items % 100 == 0:
                print("Exported {}/{} folders...".format(exported_items,
                                                         total_folders))

        print("Exported {}/{} folders".format(exported_items, total_folders))
        print("Stucture exported.")
        print("Start exporing documents...")

        # Export documents
        exported_items = 0
        for document in documents:
            document_obj = document.getObject()
            self.copy_document_to_export_dir(document_obj)

            # Update progress
            exported_items += 1
            if exported_items % 100 == 0:
                print("Exported {}/{} documents...".format(exported_items,
                                                           total_documents))

        print("Exported {}/{} documents".format(exported_items, total_documents))
        print("Path to the export: {}".format(self.export_dir))

    def normalize_title(self, title):
        normalizer = getUtility(IFileNameNormalizer,
                                name='gever_filename_normalizer')
        name = normalizer.normalize(title)
        return name

    def create_folder(sefl, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def create_zip_from_export(self):
        """
        Create a zip file from the exported files using shutil.
        """
        shutil.make_archive(self.zip_filename,
                            'zip',
                            self.export_base_dir,
                            os.path.abspath(self.export_base_dir))
        print("ZIP archive created: {}.zip".format(
            os.path.join(self.export_base_dir, self.zip_filename)
        ))

    def export(self):
        """
        Main method to export the Plone site and optionally create a ZIP file.
        """
        start_time = time()

        print("Path to the export: {}".format(self.export_dir))

        folders = self.query_folderish_brains()
        documents = self.query_document_brains()

        self.print_statistics(folders, documents)

        if self.just_stats:
            return

        self.create_export(folders, documents)

        if self.create_zip:
            self.create_zip_from_export()

        end_time = time()
        print("Export completed in {:.2f} seconds.".format(
            end_time - start_time))

        self.print_warnings()

    def print_warnings(self):
        if self.skipped_documents_by_missing_blob or self.skipped_documents_by_missing_parent:
            print("WARNING")
            print("-" * 20)

        if self.skipped_documents_by_missing_blob:
            print("Skipped documents due to missing blobs:\n\n{}".format(
                '\n'.join(self.skipped_documents_by_missing_blob)))

        if self.skipped_documents_by_missing_parent:
            print("Skipped documents due to missing parents:\n")
            for path in self.skipped_documents_by_missing_parent:
                print(path)
                self.print_last_konwn_parent(path)
                print("")

    def print_last_konwn_parent(self, path):
        if not path:
            print("Last known parent: No path found for {}".format(path))
        exported_path = self.path_mapping.get(path)

        if not exported_path:
            self.print_last_konwn_parent(os.path.dirname(path))
        else:
            print('Last known parent:')
            print(exported_path)
            print(path)


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser(
        description="Export a gever into a directory and optionally create a ZIP file")
    parser.add_argument(
        '--site-root',
        dest='site_root',
        default=None,
        help='Absolute path to the Plone site')
    parser.add_argument(
        '--root-node-path',
        default='ordnungssystem',
        help='Path to root node of subtree to be exported')
    parser.add_argument(
        '--export-base-dir',
        default='var/gever_export',
        help="Directory to export the files")
    parser.add_argument(
        '--just-stats',
        action='store_true',
        help="Only print statistics without exporting")
    parser.add_argument(
        '--create-zip',
        action='store_true',
        help="Create a ZIP file after exporting (default is False)")

    options = parser.parse_args(sys.argv[3:])
    transaction.doom()

    plone = setup_plone(app, options)

    root_node_path = '/'.join(
        plone.unrestrictedTraverse(options.root_node_path).getPhysicalPath())

    exporter = GeverExporter(
        export_base_dir=options.export_base_dir,
        root_node_path=root_node_path,
        create_zip=options.create_zip,
        just_stats=options.just_stats
    )
    exporter.export()
