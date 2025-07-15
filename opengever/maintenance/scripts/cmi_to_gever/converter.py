"""Converts a cmi dossier export to a gever bundle.

Usage:

bin/instance run src/opengever.maintenance/opengever/maintenance/scripts/cmi_to_gever/converter.py ./var/GeschaefteExport

then import it with the default bundle importer

bin/instance import path/to/bundle.oggbundle
"""

import argparse
import codecs
import errno
import json
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from jsonschema import FormatChecker
from jsonschema import validate
from opengever.bundle.loader import BUNDLE_JSON_TYPES
from pkg_resources import resource_filename as rf


class TI2036Config(object):
    """Configuration object for https://4teamwork.atlassian.net/browse/TI-2036
    """
    REVIEW_STATE_MAPPING = {
        u'Offen': u"dossier-state-active",
        u'Restriktiv': u"dossier-state-active",
    }
    PARENT_REFERENCE_MAPPING = {
        u"Auftrag": [[12, 4], ],
        u"Sessionsabl\xe4ufe": [[9, 2], ],
        u"Sitzungsabrechnung KR": [[11, 2, 1], ],
        u"Auftrag dringlich": [[12, 4], ],
        u"Allgemeines": [[10, 4], ],
        u"Beschwerde": [[12, 4], ],
        u"Berichte": [[2, 1, 1], ],
        u"Begnadigung": [[2, 3, 1], ],
        u"Div. Gesch\xe4fte / Ansprachen": [[9, 2], ],
        u"FC Kantonsrat": [[10, 2], ],
        u"Fraktionen": [[10, 4], ],
        u"Gerichtsverwaltung": [[14, ], ],
        u"Gesch\xe4ftspr\xfcfungskommission (Dossiers)": [[2, 2, 1], ],
        u"Interpellation": [[12, 4], ],
        u"Kleine Anfrage": [[12, 4], ],
        u"Kantonsratsmitglieder": [[10, 1], ],
        u"Gesch\xe4ftspr\xfcfungskommission (Meldungen)": [[2, 2, 1], ],
        u"Anl\xe4sse / Ausfl\xfcge / Besuche": [[10, 3], ],
        u"Parlamentsdienste (interne Dossiers)": [[12, 4], ],
        u"Petition": [[12, 4], ],
        u"KR-Pr\xe4sident/KR-Pr\xe4sidentin": [[6, ], ],
        u"Post Parlamentsdienste": [[10, 4], ],
        u"Rechtsetzungsgesch\xe4ft": [[12, 4], ],
        u"Ratsleitung (Dossiers)": [[4, 1], ],
        u"Session": [[9, 1], ],
        u"Sachgesch\xe4ft / Beschl\xfcsse": [[12, 4], ],
        u"Smart Parlament (Projekte)": [[12, 2], ],
        u"Vereidigung": [[12, 4], ],
        u"Volksauftrag": [[12, 4], ],
        u"Vernehmlassungen an Bundesbeh\xf6rden": [[8, 3], ],
        u"Verordnungsveto": [[8, 1], ],
        u"Volksinitiative": [[12, 4], ],
        u"Wahlgesch\xe4ft": [[12, 4], ],
        u"": [[12, 4], ],
    }

    DOCUMENTS_FOLDER_NAME = u"Dokumente"
    MAIN_JSON_FILE_NAME = u"GeschaefteExport.json"

    EXPECTED_DOSSIER_KEYS = {
        u"guid",
        u"titel",
        u"bemerkung",
        u"zugriffssteuerung",
        u"parentKey",
        u"beginn",
        u"ende",
        u"signatur",
        u"gesch\xe4ftseigner",
        u"gesch\xe4ftsart",
        u"hauptdossier",
        u"ordner",
        u"dokumente",
    }

    EXPECTED_DOCUMENT_KEYS = {
        u"guid",
        u"titel",
        u"bemerkung",
        u"fileName",
        u"fileContent",
        u"geschaeft",
        u"parentordner",
    }

    def __init__(self, cmi_bundle_path, output_dir):
        self.cmi_bundle_path = cmi_bundle_path
        self.cmi_bundle_json_path = os.path.join(cmi_bundle_path, self.MAIN_JSON_FILE_NAME)
        self.cmi_bundle_documents_path = os.path.join(cmi_bundle_path, self.DOCUMENTS_FOLDER_NAME)
        self.output_dir = output_dir

        ts = datetime.now().strftime('%Y-%d-%m_%H_%M_%S')
        self.bundle_dir = os.path.join(output_dir, 'cmi-converted-%s.oggbundle' % ts)

        self.dossiers_json_file_name = 'dossiers.json'
        self.dossiers_json_file_path = os.path.join(self.bundle_dir, self.dossiers_json_file_name)
        self.documents_json_file_name = 'documents.json'
        self.documents_json_file_path = os.path.join(self.bundle_dir, self.documents_json_file_name)


class CmiToBundleConverter(object):
    def __init__(self, config):
        self.config = config
        self.cmi_bundle_path = self.config.cmi_bundle_path
        self.cmi_bundle_json_path = self.config.cmi_bundle_json_path
        self.output_dir = self.config.output_dir
        self.bundle_dir = self.config.bundle_dir
        self.bundle_files_dir = os.path.join(self.bundle_dir, 'files')

        self.bundle_data = defaultdict(list)
        self.bundle_schema_validator = SchemaValidator()

        self.raw_data = []
        self.data = []

    def run(self):
        print(u"Start bundle conversion")
        print(u"--------------------------------------------")
        print(u"")

        self.load_data()
        self.parse_entries()
        self.print_stats()
        self.validate()

        print(u"Converting the cmi export to a gever bundle:")
        if not self.data.is_valid():
            print(u"CMI export data is not valid. Abort convertion.")
            return

        self.create_bundle_dir()
        self.write_json_files()
        self.validate_json_schemas()

        print(u"=======================================================")
        print(u"All done! Bundle successfully created at: %s" % self.bundle_dir)
        print(u"=======================================================")
        print("")

    def load_data(self):
        print(u"Load bundle from folder: %s" % self.cmi_bundle_path)

        with open(self.cmi_bundle_json_path) as json_file:
            self.raw_data = json.load(json_file)

        print(u"Successfully loaded the bundle data from: %s" % self.cmi_bundle_json_path)
        print(u"The bundle contains %s main dossiers" % len(self.raw_data))
        print(u"--------------------------------------------")
        print(u"")

    def parse_entries(self):
        print(u"Parse the cmi data and flatten it")
        self.data = CmiExportData(self.raw_data, self.config)
        self.data.flatten()
        print(u"Successfully parsed and flattended")
        print(u"--------------------------------------------")
        print(u"")

    def print_stats(self):
        self.data.print_stats()

    def validate(self):
        print(u"Validating the exported data:")
        self.data.validate()
        if self.data.is_valid():
            print(u"Everything is valid!")
            print(u"-----------------------------")
        else:
            print(u"The data is invalid! Please fix it before you continue!")
            print(u" -----------------------------")

        print(u"")

    def write_json_files(self):
        print(u"Write gever bundle data json files")
        print(u"----------------------------------")
        print(u"")

        self.write_dossiers_json()
        self.write_documents_json()

        print(u"Successfully written json files")
        print(u"-------------------------------")
        print(u"")

    def write_dossiers_json(self):
        print(u"Write %s" % self.config.dossiers_json_file_path)
        json_name = self.config.dossiers_json_file_name

        for item in self.data.dossiers:
            self.bundle_data[json_name].append(item.convert())

        self.dump_to_jsonfile(self.bundle_data[json_name],
                              self.config.dossiers_json_file_path)

    def write_documents_json(self):
        print(u"Write %s" % self.config.documents_json_file_path)
        json_name = self.config.documents_json_file_name

        for item in self.data.documents:
            item.copy_file_to_bundle()
            self.bundle_data[json_name].append(item.convert())

        self.dump_to_jsonfile(self.bundle_data[json_name],
                              self.config.documents_json_file_path)

    def validate_json_schemas(self):
        print(u"Validate written bundle json schema")
        print(u"-------------------------------")
        for json_name, items in self.bundle_data.items():
            self.bundle_schema_validator.validate_schema(json_name, items)

        print(u"Successfully validated")
        print(u"-------------------------------")
        print(u"")

    def dump_to_jsonfile(self, data, json_path):
        with open(json_path, 'wb') as jsonfile:
            json.dump(
                data,
                codecs.getwriter('utf-8')(jsonfile),
                ensure_ascii=False,
                indent=4,
                separators=(',', ': ')
            )

    def create_bundle_dir(self):
        self.mkdir_p(self.bundle_dir)
        self.mkdir_p(self.bundle_files_dir)

        print(u"Crating the bundle at: %s" % self.bundle_dir)

    def mkdir_p(self, path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


class CmiExportData(object):
    def __init__(self, data, config):
        self.data = data
        self.config = config
        self.dossiers = []
        self.documents = []
        self.is_data_valid = False

    def flatten(self):
        for item in self.data:
            cmi_dossier = CmiDossierData(item, self.config)

            self.dossiers.append(cmi_dossier)
            self.flatten_cmi_dossier(cmi_dossier)

    def flatten_cmi_dossier(self, cmi_dossier):
        for folder in cmi_dossier.folders():
            raise NotImplemented

        for document in cmi_dossier.documents():
            self.documents.append(document)

    def contents(self):
        for dossier in self.dossiers:
            yield dossier

        for document in self.documents:
            yield document

    def print_stats(self):
        print(u"")
        print(u"Content stats:")
        print(u"--------------")
        print(u"Total dossiers: %s" % len(self.dossiers))
        print(u"Total documents: %s" % len(self.documents))
        print(u"--------------")
        print(u"")

    def is_valid(self):
        return self.is_data_valid

    def validate(self):
        is_valid = True
        for content in self.contents():
            content.validate()
            if not content.is_valid():
                is_valid = False
            content.print_validation_errors()

        self.is_data_valid = is_valid
        return self.is_data_valid


class CmiDossierData(object):
    FOLDERS_KEY = u'ordner'
    DOCUMENTS_KEY = u'dokumente'
    REVIEW_STATE_KEY = u'zugriffssteuerung'
    PARENT_REFERENCE_KEY = u'gesch\xe4ftsart'

    def __init__(self, data, config, parent_path='', parent=None):
        self.data = data
        self.config = config
        self.parent_path = parent_path
        self.parent = parent
        self.errors = []

    @property
    def path(self):
        return os.path.join(self.parent_path, self.data.get('guid', ''))

    @property
    def reference(self):
        return self.data.get('guid')

    @property
    def end_date(self):
        end_date = self.data.get('ende')
        if not end_date:
            return None

        return datetime.strptime(end_date, '%d.%m.%Y').date().isoformat()

    @property
    def start_date(self):
        start_date = self.data.get('beginn')
        if not start_date:
            return None

        return datetime.strptime(start_date, '%d.%m.%Y').date().isoformat()

    def is_valid(self):
        return not self.errors

    def validate(self):
        self.errors = []
        if not self.data.get('guid'):
            self.errors.appned(u'"guid" is missing for item')

        if not set(self.data.keys()) == set(self.config.EXPECTED_DOSSIER_KEYS):
            self.errors.append(u"Expected keys changed: missing: %s, additional %s" % (
                set(self.config.EXPECTED_DOSSIER_KEYS) - set(self.data.keys()),
                set(self.data.keys()) - set(self.config.EXPECTED_DOSSIER_KEYS)
            ))

        if self.data.get('zugriffssteuerung') not in self.config.REVIEW_STATE_MAPPING:
            self.errors.append(u"Unhandled review state: '%s'" % self.data.get(self.REVIEW_STATE_KEY))

        if self.data.get(u'gesch\xe4ftsart') not in self.config.PARENT_REFERENCE_MAPPING:
            self.errors.append(u"Unhandled parent reference: '%s'" % self.data.get(self.PARENT_REFERENCE_KEY))

        if self.data.get(self.FOLDERS_KEY):
            self.errors.append(u"not implemented '%s'" % self.FOLDERS_KEY)

    def print_validation_errors(self):
        if self.is_valid():
            return

        print(u"Validation errors for folder item with guid: '%s':" % self.data.get('guid'))
        for error in self.errors:
            print(error)

        print(u"------------------------------------------------")
        print(u"")

    def folders(self):
        # subdossiers
        for item in self.data.get(self.FOLDERS_KEY, []):
            raise NotImplemented
            yield item

    def documents(self):
        for item in self.data.get(self.DOCUMENTS_KEY, []):
            yield CmiDocumentData(item, self.config, self.path, self)

    def convert(self):
        return {
            u"_creator": u"zopemaster",
            u"guid": self.data.get('guid'),
            u"description": self.data.get('bemerkung'),
            u"title": self.data.get('titel'),
            u"end": self.end_date,
            u"start": self.start_date,
            u"review_state": self.config.REVIEW_STATE_MAPPING.get(self.data.get(self.REVIEW_STATE_KEY)),
            u"parent_reference": self.config.PARENT_REFERENCE_MAPPING.get(self.data.get(self.PARENT_REFERENCE_KEY)),
            u"responsible": u"zopemaster",
        }


class CmiDocumentData(object):

    def __init__(self, data, config, parent_path, parent):
        self.data = data
        self.config = config
        self.parent_path = parent_path
        self.parent = parent
        self.errors = []

    @property
    def path(self):
        return os.path.join(self.parent_path, self.data.get('guid', ''))

    @property
    def file_name(self):
        return self.data.get('fileContent', '')

    @property
    def file_extension(self):
        return self.file_name.split('.')[-1]

    @property
    def bundle_file_name(self):
        return '%s.%s' % (self.data.get('guid'), self.file_extension)

    @property
    def bundle_file_path(self):
        return os.path.join('./files', self.bundle_file_name)

    @property
    def file_path(self):
        return os.path.join(self.config.cmi_bundle_documents_path, self.parent_path, self.data.get('fileContent', ''))

    def is_valid(self):
        return not self.errors

    def validate(self):
        self.errors = []
        if not os.path.exists(self.file_path):
            self.errors.append(u'file does not exists: %s' % self.file_path)

        if not set(self.data.keys()) == self.config.EXPECTED_DOCUMENT_KEYS:
            self.errors.append(u"Expected keys changed")

        if self.data.get('parentordner'):
            self.errors.append(u"not implemented 'parentordner'")

    def print_validation_errors(self):
        if self.is_valid():
            return

        print(u"Validation errors for document item with guid: '%s':" % self.data.get('guid'))
        for error in self.errors:
            print(error)

        print(u"------------------------------------------------")
        print(u"")

    def copy_file_to_bundle(self):
        shutil.copy(self.file_path, os.path.join(self.config.bundle_dir, 'files', self.bundle_file_name))

    def convert(self):
        return {
            u"_creator": u"zopemaster",
            u"guid": self.data.get('guid'),
            u"description": self.data.get('bemerkung'),
            u"title": self.data.get('titel'),
            u"review_state": u"document-state-draft",
            u"parent_guid": self.parent.reference,
            u"filepath": self.bundle_file_path
        }


class SchemaValidator(object):
    def __init__(self):
        self.json_schemas = self.load_schemas()

    def load_schemas(self):
        schema_dir = rf('opengever.bundle', 'schemas/')
        schemas = {}
        filenames = os.listdir(schema_dir)
        for schema_filename in filenames:
            short_name = schema_filename.replace('.schema.json', '')
            if '%s.json' % short_name in BUNDLE_JSON_TYPES:
                schema_path = os.path.join(schema_dir, schema_filename)

                with codecs.open(schema_path, 'r', 'utf-8-sig') as schema_file:
                    schema = json.load(schema_file)
                schemas['%s.json' % short_name] = schema
        return schemas

    def validate_schema(self, json_name, items):
        schema = self.json_schemas[json_name]

        # Prevent our temporary meta infos from failing validation
        if json_name == 'documents.json':
            props = schema['definitions']['document']['properties']
            props['_source_file_path'] = {'type': ['null', 'string']}
            props['_source_file_filename'] = {'type': ['null', 'string']}
            props['_source_original_message_path'] = {'type': ['null', 'string']}
            props['_source_original_message_filename'] = {'type': ['null', 'string']}

        if json_name == 'dossiers.json':
            props = schema['definitions']['dossier']['properties']
            props['_no_completeness_validation'] = {'type': 'boolean'}

        validate(items, schema, format_checker=FormatChecker())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('cmi_bundle_path',
                        help='Path to the root folder of the cmi bundle exmport')

    parser.add_argument('-o', '--output-dir', default='var/bundles/',
                        help='Path to output directory in which to create the bundle')

    options = parser.parse_args(sys.argv[3:])

    converter = CmiToBundleConverter(TI2036Config(options.cmi_bundle_path, options.output_dir))
    converter.run()
