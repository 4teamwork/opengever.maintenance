"""
Set document types not contained in vocabulary to `None`.
"""

from ftw.solr.interfaces import ISolrConnectionManager
from ftw.solr.interfaces import ISolrSearch
from ftw.upgrade.progresslogger import ProgressLogger
from opengever.document.behaviors.metadata import IDocumentMetadata
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.component import getUtility
from zope.intid.interfaces import IIntIds
import transaction
import logging

logger = logging.getLogger("migrate_doucment_types")
logging.getLogger().setLevel(logging.INFO)
for handler in logging.getLogger().handlers:
    handler.setLevel(logging.INFO)


TYPES_MAPPING = {
    u'Begehren': u'Begehren',
    u'Bericht': u'Bericht',
    u'Gutachten': u'Bericht',
    u'Studie': u'Bericht',
    u'Botschaft (NEU)': u'Bericht',
    u'Mitbericht (NEU)': u'Bericht',
    u'Abgebot / Verhandlung': u'Beschaffungswesen',
    u'Abgrenzung': u'Beschaffungswesen',
    u'Auftrag / Aufforderung': u'Beschaffungswesen',
    u'Auftragsbest\xe4tigung': u'Beschaffungswesen',
    u'Nachtrag': u'Beschaffungswesen',
    u'Offertanfrage': u'Beschaffungswesen',
    u'Offerte': u'Beschaffungswesen',
    u'Pr\xe4qualifikation': u'Beschaffungswesen',
    u'Preiseingabe': u'Beschaffungswesen',
    u'Publikation': u'Beschaffungswesen',
    u'Sch\xe4tzung / Voranschlag': u'Beschaffungswesen',
    u'Vergabe / Zuschlag': u'Beschaffungswesen',
    u'Vergabeantrag': u'Beschaffungswesen',
    u'Vergabekriterien': u'Beschaffungswesen',
    u'Wettbewerbsbeitrag': u'Beschaffungswesen',
    u'Zu- / Absage': u'Beschaffungswesen',
    u'Beschluss': u'Beschluss',
    u'Anlagenbeschrieb': u'Beschrieb',
    u'Beschrieb': u'Beschrieb',
    u'Betriebs-/ Wartungsanleitung': u'Beschrieb',
    u'Handbuch': u'Beschrieb',
    u'Leistungsbeschreibung': u'Beschrieb',
    u'Risikoverzeichnis': u'Beschrieb',
    u'Bewilligung': u'Bewilligung',
    u'Gesetz / Verordnung': u'Erlass',
    u'Abrechnung': u'Finanzen',
    u'Budget / Planung / Prognose': u'Finanzen',
    u'Kostenkontrolle': u'Finanzen',
    u'Kostenzusammenstellung': u'Finanzen',
    u'Rechnung': u'Finanzen',
    u'BIM': u'L\xf6schen',
    u'Teilnahmebest\xe4tigung': u'L\xf6schen',
    u'Verhandlung': u'L\xf6schen',
    u'Foto-Dokumentation': u'Multimedia',
    u'Video': u'Multimedia',
    u'Plan': u'Plan',
    u'Planverzeichnis': u'Plan',
    u'Aktennotiz (NEU)': u'Protokoll',
    u'Abnahmeprotokoll': u'Protokoll',
    u'Ereignisprotokoll': u'Protokoll',
    u'Messprotokoll': u'Protokoll',
    u'Protokoll': u'Protokoll',
    u'Pr\xfcfprotokoll': u'Protokoll',
    u'Email (NEU)': u'Schreiben',
    u'Begleitbrief': u'Schreiben',
    u'Schreiben (NEU)': u'Schreiben',
    u'Stellungnahme (NEU)': u'Schreiben',
    u'Adressverzeichnis': u'Verschiedenes',
    u'Allgemeine Mitteilung': u'Verschiedenes',
    u'Analyse': u'Verschiedenes',
    u'Anforderungen': u'Verschiedenes',
    u'Antrag': u'Verschiedenes',
    u'Bedingung': u'Verschiedenes',
    u'Beleg': u'Verschiedenes',
    u'Benchmark': u'Verschiedenes',
    u'Berechnung': u'Verschiedenes',
    u'Betreibung': u'Verschiedenes',
    u'Bewertung': u'Verschiedenes',
    u'Checkliste': u'Verschiedenes',
    u'Deckblatt': u'Verschiedenes',
    u'Dokumentation': u'Verschiedenes',
    u'Dokumentenverzeichnis': u'Verschiedenes',
    u'Einladung': u'Verschiedenes',
    u'Empfehlung': u'Verschiedenes',
    u'Erg\xe4nzende Bestimmungen': u'Verschiedenes',
    u'Evaluation': u'Verschiedenes',
    u'Factsheet': u'Verschiedenes',
    u'Formular': u'Verschiedenes',
    u'Kennzahlen': u'Verschiedenes',
    u'Kontrollplan': u'Verschiedenes',
    u'Konzept': u'Verschiedenes',
    u'Lieferschein': u'Verschiedenes',
    u'Liste': u'Verschiedenes',
    u'Massnahme': u'Verschiedenes',
    u'Mutationen': u'Verschiedenes',
    u'Nachweis': u'Verschiedenes',
    u'Organigramm': u'Verschiedenes',
    u'Pressemappe': u'Verschiedenes',
    u'Quittungen / Abnahme': u'Verschiedenes',
    u'Report': u'Verschiedenes',
    u'Selbstdeklaration': u'Verschiedenes',
    u'Terminplan': u'Verschiedenes',
    u'Verzeichnis': u'Verschiedenes',
    u'Schema': u'Verschiedenes',
    u'Baurechtsvertrag': u'Vertragswesen',
    u'Dienstbarkeitsvertrag': u'Vertragswesen',
    u'Garantie': u'Vertragswesen',
    u'Geb\xe4udebetriebsvertrag': u'Vertragswesen',
    u'Gebrauchsleihvertrag': u'Vertragswesen',
    u'Grundbuchauszug': u'Vertragswesen',
    u'Kaufrechtsvertrag': u'Vertragswesen',
    u'Kaufvertrag': u'Vertragswesen',
    u'K\xfcndigung': u'Vertragswesen',
    u'Liefervertrag': u'Vertragswesen',
    u'Mahnung': u'Vertragswesen',
    u'Mietvertrag': u'Vertragswesen',
    u'Mobiliarservicevertrag': u'Vertragswesen',
    u'Nutzerservicevertrag': u'Vertragswesen',
    u'Nutzungsvereinbarung': u'Vertragswesen',
    u'Pachtvertrag': u'Vertragswesen',
    u'Planervertrag': u'Vertragswesen',
    u'Police': u'Vertragswesen',
    u'R\xfcge': u'Vertragswesen',
    u'Service- und Wartungsvertrag': u'Vertragswesen',
    u'Vertrag': u'Vertragswesen',
    u'Vertragsoption': u'Vertragswesen',
    u'Verwaltungsvertrag': u'Vertragswesen',
    u'Vorkaufsvertrag': u'Vertragswesen',
    u'Werkvertrag': u'Vertragswesen',
    u'Dienstleistungsvertrag': u'Vertragswesen',
    u'Untermietvertrag': u'Vertragswesen',
    u'Vereinbarung': u'Vertragswesen',
    u'Verf\xfcgung': u'Vertragswesen'
}


def migrate_document_types(portal):
    catalog = api.portal.get_tool('portal_catalog')
    field = IDocumentMetadata['document_type']

    solr_search = getUtility(ISolrSearch)
    # solr_connection = getUtility(ISolrConnectionManager).connection
    res = solr_search.search(
        query=u'object_provides:"opengever.document.behaviors.metadata.IDocumentMetadata" && document_type:[* TO *]')

    uids = [doc.get('UID') for doc in res.docs]

    catalog = api.portal.get_tool('portal_catalog')
    document_brains = catalog.unrestrictedSearchResults(UID=uids)

    for brain in ProgressLogger(u'migrate document_type', document_brains, logger=logger):
        obj = brain.getObject()
        voc = field.vocabulary(obj)
        title_by_token = {term.title: term.token for term in voc}

        document_type = IDocumentMetadata(obj).document_type
        document_type_title = voc.getTerm(document_type).title

        new_title = TYPES_MAPPING[document_type_title]

        if new_title == u'L\xf6schen':
            IDocumentMetadata(obj).document_type = None
        else:
            IDocumentMetadata(obj).document_type = title_by_token[new_title]

        obj.reindexObject(idxs=['document_type'])


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    migrate_document_types(plone)
    # transaction.commit()


if __name__ == '__main__':
    main()
