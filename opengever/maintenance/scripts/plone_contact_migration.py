# -*- coding: utf-8 -*-
"""
This script is used to export plone contacts into a bundle and migrate
the participations.

bin/instance run ./scripts/plone_contact_migration.py

optional arguments:
  -k : url for the KuB deployment
  -p : skip the migration of dossier participations
  -r : reindex participations for all dossiers, which has kub-participations.
  -D : delete all plone contact data including the contact folder.
  -n : dry-run.
"""

from ftw.upgrade.progresslogger import ProgressLogger
from opengever.contact.contact import IContact
from opengever.contact.interfaces import IContactFolder
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.participations import KuBParticipationHandler
from opengever.dossier.participations import PloneParticipationHandler
from opengever.kub.interfaces import IKuBSettings
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import TextTable
from opengever.ogds.base.actor import ActorLookup
from opengever.ogds.base.actor import ContactActor
from plone import api
from uuid import uuid4
from zope.annotation.interfaces import IAnnotations
import argparse
import json
import logging
import os
import re
import sys
import time
import transaction

logger = logging.getLogger('opengever.maintenance')
logger.setLevel(logging.INFO)
logger.root.setLevel(logging.INFO)
stream_handler = logger.root.handlers[0]
stream_handler.setLevel(logging.INFO)


class PloneContactExporter(object):

    def __init__(self, bundle_directory):
        self.bundle_directory = bundle_directory
        self.contact_mapping = {}
        self.kub_url = None
        self.addresses_table = TextTable(col_max_width=60)
        self.addresses_table.add_row([
            "contactid", "skipped", "unmapped country", "default country",
            "street", "house number"])
        self.unmapped_table = TextTable(col_max_width=60)
        self.unmapped_table.add_row(["contactid", "company",
                                     "department", "function"])

    def run(self, skip_participations=False, kub_url=None,
            reindex_participations=False, delete_contacts=False):

        self.kub_url = kub_url
        os.mkdir(self.bundle_directory)

        self.export()

        if not skip_participations:
            self.migrate_participations()

        if reindex_participations:
            self.reindex_dossier_participations()

        if delete_contacts:
            self.delete_contacts()

    def export(self):
        contacts = list(self.get_contacts())
        self.export_json('people.json', contacts)

    def migrate_participations(self):
        if not self.kub_url:
            raise Exception(
                u'Enabling KUB is required for participations migration, '
                u'kub_url necessary.')

        api.portal.set_registry_record(
            name='base_url', interface=IKuBSettings,
            value=self.kub_url.decode('utf-8'))

        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__)
        brains = ProgressLogger('Migrate dossier participations.', brains)
        for brain in brains:
            dossier = brain.getObject()
            self.migrate_participation(dossier)

    def migrate_participation(self, dossier):
        plone_handler = PloneParticipationHandler(dossier)
        kub_handler = KuBParticipationHandler(dossier)
        participations = plone_handler.get_participations()

        for participation in participations:
            # To avoid reindexing the dossier after each participation we
            # add the participation manually
            actor = ActorLookup(participation.contact).lookup()
            if isinstance(actor, ContactActor):
                participant_id = u'person:{}'.format(
                    self.contact_mapping[participation.contact])
            else:
                participant_id = participation.contact

            roles = [role for role in participation.roles]

            if kub_handler.has_participation(participant_id):
                existing = kub_handler.get_participation(participant_id)
                roles = list(set(roles + existing.roles))
                existing.roles = roles
            else:
                kub_participation = kub_handler.create_participation(
                    participant_id=participant_id,
                    roles=roles)
                kub_handler.append_participation(kub_participation)

    def serialize_contact(self, kub_uid, contact):
        data = {
            "id": kub_uid,
            "third_party_id": contact.contactid(),
            "salutation": contact.salutation,
            "title": contact.academic_title,
            "official_name": contact.lastname,
            "first_name": contact.firstname,
            "description": contact.description,
        }
        # Add fields that do not get mapped to anything in KuB. We add them to
        # the bundle nonetheless in case we need the data later.
        unmapped_fields = {
            "company": contact.company,
            "department": contact.department,
            "function": contact.function
            }
        data.update(unmapped_fields)
        if any(unmapped_fields.values()):
            self.unmapped_table.add_row([
                contact.contactid(), contact.company,
                contact.department, contact.function])

        self._serialize_mail_addresses(data, contact)
        self._serialize_url(data, contact)
        self._serialize_phone_numbers(data, contact)
        self._serialize_address(data, contact)

        # Remove all attributes that have no value
        data = {key: value for key, value in data.items() if value}
        return data

    def _serialize_mail_addresses(self, data, contact):
        mails = []
        if contact.email:
            mails.append({"email": contact.email})
        if contact.email2:
            mails.append({"email": contact.email2})
        data["email_addresses"] = mails

    def _serialize_url(self, data, contact):
        if contact.url:
            data["urls"] = [{"url": contact.url}]

    def _serialize_phone_numbers(self, data, contact):
        phone_mapping = {
            "phone_office": 6,
            "phone_fax": 3,
            "phone_mobile": 2,
            "phone_home": 1
        }
        numbers = []
        for attr_name, category in phone_mapping.items():
            if getattr(contact, attr_name):
                numbers.append(
                    {"phone_number": getattr(contact, attr_name),
                     "phone_category": category}
                    )
        data["phone_numbers"] = numbers

    def _get_country_code(self, country):
        if country in KUB_COUNTRY_CHOICES:
            return country
        elif country in COUNTRY_MAPPING:
            return COUNTRY_MAPPING[country]

    def _maybe_extract_house_number(self, street):
        """
        Taken from https://github.com/4teamwork/kub-migration-stabs/blob/master/transform.py
        Try to extract a house number from the street line
        We make the following assumptions:
        - house numbers are at the start or end of the street line
        - any comma before or after a house number can be eliminated
        - the house number matches one of the regexes provided by oliver
        Any number matching above criteria is extracted. This will create false
        positives in rare cases but we have decided we can live with that.
        """
        house_number = None
        if not street:
            return street, house_number
        # try to extract housenumber from street

        parts = street.split()
        # try house number from last part
        maybe_house_number = parts[-1]
        if (
            re.match("([\d]{1,}[/-][\d]{1,})", maybe_house_number)
            or re.match("([\d]{1,}[a-z]{1,3})", maybe_house_number)
            or re.match("([\d]{1,})", maybe_house_number)
        ):
            house_number = maybe_house_number
            street = " ".join(parts[:-1])
        # try house number from first part
        else:
            maybe_house_number = parts[0]
            if (
                re.match("([\d]{1,}[/-][\d]{1,})", maybe_house_number)
                or re.match("([\d]{1,}[a-z]{1,3})", maybe_house_number)
                or re.match("([\d]{1,})", maybe_house_number)
            ):
                house_number = maybe_house_number
                street = " ".join(parts[1:])

        if house_number:
            # get rid of any leading/trailing leftover commas after splitting
            house_number = house_number.lstrip(",").rstrip(",").strip()
            street = street.lstrip(",").rstrip(",").strip()

        return street, house_number

    def _serialize_address(self, data, contact):
        # Town and country are mandatory.
        is_swiss = True
        unmapped_country = ''
        default_country = ''
        skipped = None
        address = {}
        if contact.address1:
            street, house_number = self._maybe_extract_house_number(contact.address1)
            address["street"] = street
            if house_number is not None:
                address["house_number"] = house_number

        if contact.country:
            country_code = self._get_country_code(contact.country)
            if country_code is None:
                unmapped_country = contact.country
                skipped = True
            elif country_code != "CH":
                is_swiss = False
            address["countryIdISO2"] = country_code

        attr_mapping = {
            "address2": "address_line_1",
            "zip_code": "swiss_zip_code" if is_swiss else "foreign_zip_code",
            "city": "town",
        }
        for plone_attr, kub_attr in attr_mapping.items():
            if getattr(contact, plone_attr):
                address[kub_attr] = getattr(contact, plone_attr)
        if address:
            if not address.get("town"):
                skipped = True

            elif not skipped and not address.get("countryIdISO2"):
                default_country = "CH"
                address["countryIdISO2"] = "CH"

            self.addresses_table.add_row(
                [data["third_party_id"], 'x' if skipped else '',
                 unmapped_country, default_country,
                 address.get("street"), address.get("house_number", '')])

            if not skipped:
                data["addresses"] = [address]

    def get_contacts(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IContact.__identifier__)
        for brain in brains:
            contact = brain.getObject()
            kub_uid = str(uuid4())
            self.contact_mapping[contact.contactid()] = kub_uid
            yield self.serialize_contact(kub_uid, contact)

    def export_json(self, filename, items):
        with open('/'.join((self.bundle_directory, filename)), 'w') as outfile:
            json.dump(items, outfile, indent=4)

    def reindex_dossier_participations(self):
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__)
        brains = ProgressLogger('Reindex dossier participations.', brains)
        for brain in brains:
            dossier = brain.getObject()
            kub_handler = KuBParticipationHandler(dossier)
            if kub_handler.get_participations():
                dossier.reindexObject(idxs=["participations", "UID"])

    def delete_contacts(self):
        """Delete all plone contacts and plone contact participations
        """
        # We first remove all plone participations
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IDossierMarker.__identifier__)
        brains = ProgressLogger('Delete plone participations.', brains)
        for brain in brains:
            dossier = brain.getObject()
            annotations = IAnnotations(dossier)
            if PloneParticipationHandler.annotation_key in annotations:
                annotations.pop(PloneParticipationHandler.annotation_key)

        # Now we delete all plone contacts
        catalog = api.portal.get_tool('portal_catalog')
        brains = catalog.unrestrictedSearchResults(
            object_provides=IContact.__identifier__)
        brains = ProgressLogger('Delete plone contacts.', brains)
        for brain in brains:
            contact = brain.getObject()
            api.content.delete(contact)

        # Now we delete the contact folder
        results = catalog.unrestrictedSearchResults(
            object_provides=IContactFolder.__identifier__)
        if len(results) == 0:
            raise Exception(u'ContactFolder is missing.')
        if len(results) > 1:
            raise Exception(u'Found more than one ContactFolder.')

        contactfolder = results[0].getObject()
        if contactfolder.values():
            raise Exception(u'ContactFolder is not empty.')

        logger.info("Deleting Contactfolder.")
        api.content.delete(contactfolder)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')
    parser.add_argument("-n", "--dry-run", action="store_true",
                        dest="dryrun", default=False)
    parser.add_argument("-k", "--kub-url", dest="kub_url", default=None)
    parser.add_argument("-p", "--skip-participations",
                        action="store_true",
                        dest="skip_participations", default=False)
    parser.add_argument("-r", "--reindexing-participations", action="store_true",
                        dest="reindex_participations", default=False)
    parser.add_argument("-D", "--delete-contacts", action="store_true",
                        dest="delete_contacts", default=False)

    options = parser.parse_args(sys.argv[3:])

    if options.dryrun:
        logger.info("Dry run, dooming transaction")
        transaction.doom()

    app = setup_app()
    setup_plone(app, options)

    bundle_directory = u'var/kub-bundle-{}'.format(time.strftime('%d%m%Y-%H%M%S'))
    exporter = PloneContactExporter(bundle_directory)
    exporter.run(skip_participations=options.skip_participations,
                 kub_url=options.kub_url,
                 reindex_participations=options.reindex_participations,
                 delete_contacts=options.delete_contacts)

    print("\nTable of exported addresses\n")
    print(exporter.addresses_table.generate_output())
    print("\n")

    with open(os.path.join(bundle_directory, "address_transformations.csv"), "w") as logfile:
        exporter.addresses_table.write_csv(logfile)

    print("\nTable of contacts with unmapped fields\n")
    print(exporter.unmapped_table.generate_output())
    print("\n")

    with open(os.path.join(bundle_directory, "unmapped_fields.csv"), "w") as logfile:
        exporter.unmapped_table.write_csv(logfile)

    if not options.dryrun:
        transaction.commit()
        logger.info("Transaction committed")


# get list from KuB in `shell_plus`:
# `Person.country.field.countries.countries`
KUB_COUNTRY_CHOICES = {
    "AF": "Afghanistan",
    "AX": "Åland-Inseln",
    "AL": "Albanien",
    "DZ": "Algerien",
    "AS": "Amerikanisch-Samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarktis",
    "AG": "Antigua und Barbuda",
    "AR": "Argentinien",
    "AM": "Armenien",
    "AW": "Aruba",
    "AU": "Australien",
    "AT": "Österreich",
    "AZ": "Aserbaidschan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesch",
    "BB": "Barbados",
    "BY": "Weißrussland",
    "BE": "Belgien",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivien",
    "BQ": "Bonaire, Sint Eustatius und Saba",
    "BA": "Bosnien und Herzegowina",
    "BW": "Botswana",
    "BV": "Bouvetinsel",
    "BR": "Brasilien",
    "IO": "Britisches Territorium im Indischen Ozean",
    "BN": "Brunei",
    "BG": "Bulgarien",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "CV": "Kap Verde",
    "KH": "Kambodscha",
    "CM": "Kamerun",
    "CA": "Kanada",
    "KY": "Kaimaninseln",
    "CF": "Zentralafrikanische Republik",
    "TD": "Tschad",
    "CL": "Chile",
    "CN": "China",
    "CX": "Weihnachtsinsel",
    "CC": "Kokosinseln (Keelinginseln)",
    "CO": "Kolumbien",
    "KM": "Komoren",
    "CG": "Kongo",
    "CD": "Kongo (Demokratische Republik)",
    "CK": "Cookinseln",
    "CR": "Costa Rica",
    "CI": "Côte d'Ivoire",
    "HR": "Kroatien",
    "CU": "Kuba",
    "CW": "Curaçao",
    "CY": "Zypern",
    "CZ": "Tschechien",
    "DK": "Dänemark",
    "DJ": "Dschibuti",
    "DM": "Dominica",
    "DO": "Dominikanische Republik",
    "EC": "Ecuador",
    "EG": "Ägypten",
    "SV": "El Salvador",
    "GQ": "Äquatorialguinea",
    "ER": "Eritrea",
    "EE": "Estland",
    "SZ": "Eswatini",
    "ET": "Äthiopien",
    "FK": "Falklandinseln (Malwinen)",
    "FO": "Faröerinseln",
    "FJ": "Fidschi",
    "FI": "Finnland",
    "FR": "Frankreich",
    "GF": "Französisch Guinea",
    "PF": "Französisch-Polynesien",
    "TF": "Französische Süd- und Antarktisgebiete",
    "GA": "Gabun",
    "GM": "Gambia",
    "GE": "Georgien",
    "DE": "Deutschland",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Griechenland",
    "GL": "Grönland",
    "GD": "Granada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GG": "Guernsey",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HT": "Haiti",
    "HM": "Heard und McDonaldinseln",
    "VA": "Vatikanstadt",
    "HN": "Honduras",
    "HK": "Hong Kong",
    "HU": "Ungarn",
    "IS": "Island",
    "IN": "Indien",
    "ID": "Indonesien",
    "IR": "Iran",
    "IQ": "Irak",
    "IE": "Irland",
    "IM": "Isle of Man",
    "IL": "Israel",
    "IT": "Italien",
    "JM": "Jamaika",
    "JP": "Japan",
    "JE": "Jersey",
    "JO": "Jordanien",
    "KZ": "Kasachstan",
    "KE": "Kenia",
    "KI": "Kirivati",
    "KP": "Nordkorea",
    "KR": "Südkorea",
    "KW": "Kuwait",
    "KG": "Kirgisistan",
    "LA": "Laos",
    "LV": "Lettland",
    "LB": "Libanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libyen",
    "LI": "Liechtenstein",
    "LT": "Litauen",
    "LU": "Luxemburg",
    "MO": "Macao",
    "MG": "Madagaskar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Malediven",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshallinseln",
    "MQ": "Martinique",
    "MR": "Mauretanien",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexiko",
    "FM": "Mikronesien (Föderierte Staaten von)",
    "MD": "Moldawien",
    "MC": "Monaco",
    "MN": "Mongolei",
    "ME": "Montenegro",
    "MS": "Montserrat",
    "MA": "Marokko",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Niederlande",
    "NC": "Neukaledonien",
    "NZ": "Neuseeland",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolkinsel",
    "MK": "North Macedonia",
    "MP": "Commonwealth der Nördlichen Marianen",
    "NO": "Norwegen",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "Palästina",
    "PA": "Panama",
    "PG": "Papua Neu Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippinen",
    "PN": "Pitcairn",
    "PL": "Polen",
    "PT": "Portugal",
    "PR": "Puerto Rico",
    "QA": "Katar",
    "RE": "Réunion",
    "RO": "Rumänien",
    "RU": "Russland",
    "RW": "Ruanda",
    "BL": "Saint-Barthélemy",
    "SH": "St. Helena, Ascension und Tristan da Cunha",
    "KN": "St. Kitts und Nevis",
    "LC": "St. Lucia",
    "MF": "St. Martin (französischer Teil)",
    "PM": "Saint-Pierre und Miquelon",
    "VC": "St. Vincent und die Grenadinen",
    "WS": "Samoa",
    "SM": "San Marino",
    "ST": "São Tomé und Príncipe",
    "SA": "Saudi Arabien",
    "SN": "Senegal",
    "RS": "Serbien",
    "SC": "Seychellen",
    "SL": "Sierra Leone",
    "SG": "Singapur",
    "SX": "Sint Maarten (niederländischer Teil)",
    "SK": "Slowakei",
    "SI": "Slowenien",
    "SB": "Salomonen",
    "SO": "Somalia",
    "ZA": "Südafrika",
    "GS": "Südgeorgien und die Südlichen Sandwichinseln",
    "SS": "Südsudan",
    "ES": "Spanien",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SR": "Surinam",
    "SJ": "Spitzbergen und Jan Mayen",
    "SE": "Schweden",
    "CH": "Schweiz",
    "SY": "Syrien",
    "TW": "Taiwan",
    "TJ": "Tadschikistan",
    "TZ": "Tansania",
    "TH": "Thailand",
    "TL": "Osttimor",
    "TG": "Togo",
    "TK": "Tokelau",
    "TO": "Tonga",
    "TT": "Trinidad und Tobago",
    "TN": "Tunesien",
    "TR": "Türkei",
    "TM": "Turkmenistan",
    "TC": "Turks- und Caicosinseln",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "Vereinigte Arabische Emirate",
    "GB": "Vereinigtes Königreich",
    "UM": "USA - Sonstige Kleine Inseln",
    "US": "Vereinigte Staaten von Amerika",
    "UY": "Uruguay",
    "UZ": "Usbekistan",
    "VU": "Vanuatu",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "VG": "Britische Jungferninseln",
    "VI": "Amerikanische Jungferninseln",
    "WF": "Wallis und Futuna",
    "EH": "Westsahara",
    "YE": "Jemen",
    "ZM": "Sambia",
    "ZW": "Simbabwe",
    "XK": "Kosovo",
}
COUNTRY_MAPPING = {name: label for label, name in KUB_COUNTRY_CHOICES.items()}
# additional values
COUNTRY_MAPPING.update(
    {
        "Suisse": "CH",
        "Svizzera": "CH",
        "France": "FR",
        "Äquatorial-Guinea": "GQ",
        "Ausland allgemein": None,
        "Brit. Jungferninseln": "VG",
        "Elfenbeinküste": "CI",
        "Grenada": "GD",
        "Grossbritannien": "GB",
        "Hongkong": "HK",
        "Iran, Islamische Republik": "IR",
        "Kokosinseln": "CC",
        "Kongo, Demokratische Republik": "CD",
        "Korea, Demo. Volksrepublik": "KP",
        "Libysch-Arabische Dschamahirija": "LY",
        "Lybien": "LY",  # typo :)
        "Macau": None,  # 5 affected records which seem incorrectly used (albanisch/mazedonisch)
        "Mazedonien, die ehemalige jugoslawische Republik": "MK",
        "Moldawien (Republik Moldau)": "MD",
        "nicht Zugewiesen": None,
        "Niederländische Antillen": None,  # does not exist anymore, 4 affected records
        "Russische Föderation": "RU",
        "Saudi-Arabien": "SA",
        "Serbien und Montenegro": None,  # split in two countries, 1272 affected record
        "Staatenlos": None,
        "Tansania, Vereinigte Republik": "TZ",
        "Tschechische Republik": "CZ",
        "Weissrussland (Belarus)": "BY",
        "Zentralafrik. Republik": "CF",
        None: None,
    }
)

if __name__ == '__main__':
    main()
