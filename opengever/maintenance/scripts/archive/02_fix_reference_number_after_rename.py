"""Script for fixing the reference number after renaming it via the
rename_repository_folders script.
"""

from Acquisition import aq_inner
from Acquisition import aq_parent
from datetime import datetime
from opengever.dossier.behaviors.dossier import IDossier
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.task.task import ITask
from plone import api
from zope.annotation.interfaces import IAnnotations
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
import transaction


SEPARATOR = '-' * 78

FIXED = [
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/beratung-im-laendlichen-raum/landwirtschaftliche-beratung/privatberatung-2',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/baulicher-tierschutz-2',
    '/ai/ordnungssystem/verkehr/strassen/strassen/flurstrassen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/energie/beziehungen/enfk-ch-1',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/energie/beziehungen/enfk-o-1',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/energie/beziehungen/bfe',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/statistik/zusammenstellungen-analysen',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/stiftungen-2',
    '/ai/ordnungssystem/kultur-sport-und-freizeit-kirche/kultur/kantonsbibliothek/beziehungen/schweiz-konferenz-der-kantonsbibliotheken',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/kommunikation/medien',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/grosser-rat/buero-grosser-rat/geschafte',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/grosser-rat/sessionen/protokolle',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rechtsverfahren-gesetzgebung/weitere-rechtsmittelverfahren',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rechtsverfahren-gesetzgebung/weitere-rechtsmittelverfahren',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rechtsverfahren-gesetzgebung/gesetzgebung',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/kommunikation-information',

    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/externe-interne-kommunikation-information/internetauftritt',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/schriftgutverwaltung-archivierung/ueberlieferungsbildung/offentl-korperschaften-vereine-unternehmen-usw',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/schriftgutverwaltung-archivierung/ueberlieferungsbildung/personen',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/allgemeines',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/bau-umweltdepartement/jagd-fischereiverwaltung',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/finanzdepartement/steuerverwaltung',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/gesundheits-und-sozialdepartement/burgerheim',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/gesundheits-und-sozialdepartement/soziale-dienste',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/land-forstwirtschaftsdepartement',

    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines/departementsfuehrung/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/fuehrung-organisation/controlling-statistik',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/beziehungen/ostpol-opk',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/einsatzzentrale',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/kriminalpolizei/brandmeldeanlagen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/kriminalpolizei/alarmanlagen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/bilder-einsatze',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/untersuchungsbehoerden-strafvollzug/staatsanwaltschaft/beziehungen/vereinigung-der-schweizer-staatsanwalte-mit-fuhrungsfunktion',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/untersuchungsbehoerden-strafvollzug/staatsanwaltschaft/beziehungen/schweizerische-staatsanwaltekonferenz-ssk',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/untersuchungsbehoerden-strafvollzug/staatsanwaltschaft/beziehungen/ostschweizer-staatsanwaltekonferenz',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/untersuchungsbehoerden-strafvollzug/staatsanwaltschaft/beziehungen/schweiz-konferenz-der-informationsbeauftragten-der-staatsanwaltschaften-skis',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/untersuchungsbehoerden-strafvollzug/staatsanwaltschaft/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/rechtssprechung/schlichtungsstelle-fuer-mietverhaeltnisse/fallfuhrung',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-i/kindes-und-erwachsenenschutz/uebrige-massnahmen/fursorgerische-unterbringungen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/erbschaftswesen/aufbewahrung-testamente',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/betreibungs-und-konkurswesen/betreibungswesen/zahlungsverkehr',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/registerfuehrung/belegkontrolle',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/notariat/vertrage-rechtsgeschafte',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/notariat/grundpfandrechte',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/guter-erbrechtliche-rechtsgeschafte',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/handaenderungssteuern/burgschaften',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/handelsregister/unternehmen/genossenschaften',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/handelsregister/unternehmen/stiftungen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/handelsregister/unternehmen/vereine',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/verteidigung/militaerische-verteidigung/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/verteidigung/zivile-verteidigung/kommunikation-offentlichkeitsarbeit',

    '/ai/ordnungssystem/bildung/allgemeines/organisation-bildungswesen/schulgemeinden/beziehungen',
    '/ai/ordnungssystem/bildung/allgemeines/organisation-bildungswesen/volksschule/lehrerinnen-lehrerverein-appenzell-innerrhoden',
    '/ai/ordnungssystem/bildung/allgemeines/organisation-bildungswesen/beziehungen/d-edk',
    '/ai/ordnungssystem/bildung/allgemeines/organisation-bildungswesen/beziehungen/edk-ost',
    '/ai/ordnungssystem/bildung/allgemeines/organisation-bildungswesen/beziehungen/kds-d-kds-dsk-o',
    '/ai/ordnungssystem/bildung/allgemeines/schulinspektorat/schulrate',
    '/ai/ordnungssystem/bildung/allgemeines/schulinspektorat/weiterbildung',
    '/ai/ordnungssystem/bildung/allgemeines/stipendienwesen/stipendien-darlehensbezuger',
    '/ai/ordnungssystem/bildung/paedagogisch-therapeutische-dienste/grundlagen',
    '/ai/ordnungssystem/bildung/volksschulamt',
    '/ai/ordnungssystem/bildung/volksschule/grundlagen',
    '/ai/ordnungssystem/bildung/volksschule/volksschule',
    '/ai/ordnungssystem/bildung/volksschule/allgemeines-volksschule/klassenlisten',

    # XXXX

    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortmanagement/wf-projekte',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortmanagement/einheimische-unternehmen',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortmanagement/dienstleistungen',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortpromotion-kantonal',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/innovations-und-kooperationsfoerderung/wtt-projekte',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/landwirtschaft/ldw-projekte',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortpromotion-st-gallenbodenseearea-sgba',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/sgba-projekte',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/prasentationen-aktivitaten',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/institutionen-partner-scouts',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/departementsfuehrung/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/organisation-landwirtschaft/beziehungen/kolas-kolas-o',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/organisation-landwirtschaft/beziehungen/sachbearbeitertagung-bodenrecht',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/organisation-landwirtschaft/beziehungen/beratungsforum-schweiz',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/strukturverbesserungen/energieversorgung',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/strukturverbesserungen/erschliessungsanlagen',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/beratung-im-laendlichen-raum/landwirtschaftliche-beratung/gruppenberatung',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/beratung-im-laendlichen-raum/landwirtschaftliche-beratung/betriebsberatung',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/pflanzenbau/pflanzenschutz',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/tierproduktion/viehzucht',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/fuhrung-organisation',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/ressourcen-support',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/beziehungen',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/kommissionen',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/veterinaeramt/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/alpwirtschaft/gemeine-alpen',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/alpwirtschaft/ubrige-alpen',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldplanung/wald-wild-konzept',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/holznutzung',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldschutz/forststatistik',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/schutzwald',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldpflege-waldnutzung/nais',
    '/ai/ordnungssystem/volkswirtschaft/tourismus/tourismusfoerderung/to-projekte',
    '/ai/ordnungssystem/volkswirtschaft/industrie-gewerbe-handel/gewerbe-und-marktwesen/vollzug-alkoholgesetz',

    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/terminaufgaben',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/staatsrechnung',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/debitorenbuchhaltung',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/fibu-mwst-kreditoren-kasse-etc',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/spezialrechnungen',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanz-und-rechnungswesen-operativ/gymnasium-appenzell',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/nfa-bund',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/geldfluss-zu-korperschaften',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/chinderhort',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/studiendarlehen',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanzcontrolling/grosser-rat-staatswirtschaftliche-kommission',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/finanzcontrolling/weitere-dienstleistungen',
    '/ai/ordnungssystem/finanzen-und-steuern/steuern/registerfuehrung/korrespondenz-mit-nicht-in-ai-registrierten-personen',
    '/ai/ordnungssystem/finanzen-und-steuern/steuern/registerfuehrung/ahv-listen',

    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/organisation-bau-umwelt/rechtsdienst/rechtsentwicklungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/organisation-bau-umwelt/rechtsdienst/dokumentationen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/organisation-bau-umwelt/rechtsdienst/abklarungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/organisation-bau-umwelt/rechtsdienst/ausnahmebewilligungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/verkehrs-einsatzpolizei/fachbereich-verkehr/vsi-jupo',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldschutz',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/vermessungswesen/kommissionen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/vermessungswesen/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/grundlagen/stiftung-internat',
    '/ai/ordnungssystem/kultur-sport-und-freizeit-kirche/kultur/stiftung-landammann-dr-albert-broger/sitzungen',
    '/ai/ordnungssystem/kultur-sport-und-freizeit-kirche/kultur/stiftung-pro-innerrhoden/sitzungen',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/gesundheits-und-sozialdepartement/altersheim-torfnest',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/verteidigung/militaerische-verteidigung/angehoerige-der-armee/orientierungstag',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/support-kantonspolizei',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kriminalpolizei/kommunikationsuberwachung',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kriminalpolizei/staatsschutz',
    '/ai/ordnungssystem/volkswirtschaft/industrie-gewerbe-handel/eichwesen/messmittel/gewichtsmessgerate',
    '/ai/ordnungssystem/bildung/allgemeines/schulsozialarbeit/klasseninterventionen'
]

REPO_PATHS = [

    '/ai/ordnungssystem/bildung/volksschule/allgemeines-volksschule/lehrmittel',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/fuhrung-organisation',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/ressourcen-support',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/personal',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/infrastruktur',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/finanzen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/informatik',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/berufsberatung/berufs-und-studieninformationen/dokumentation',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/berufsberatung/berufs-und-studieninformationen/informationsveranstaltungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/berufsberatung/berufs-und-studieninformationen/nachholbildung-erwachsene',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/berufsberatung/berufs-und-studieninformationen/berufsmaturitat',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/berufsberatung/berufs-und-studieninformationen/statistik',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/beziehungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lehrvertrage',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/lernende',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/nachteilsausgleich',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/aktion-offentlicher-verkehr',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lehrbetriebe',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lernendenangebote/bildungsbewilligungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lernendenangebote/berufsbildner',

    '/ai/ordnungssystem/kultur-sport-und-freizeit-kirche/kultur/stiftung-pro-innerrhoden/preisverleihungen',
    '/ai/ordnungssystem/kultur-sport-und-freizeit-kirche/freizeit-und-sport/erwachsenenbildung/angebote-kommission',
    '/ai/ordnungssystem/gesundheit/spitalwesen/spital-appenzell/tarife',
    '/ai/ordnungssystem/gesundheit/gesundheits-alterswesen',
    '/ai/ordnungssystem/gesundheit/gesundheitswesen/ambulante-gesundheitsdienste/bewilligungs-aufsichtswesen',
    '/ai/ordnungssystem/gesundheit/gesundheitsfoerderung-und-praevention/gesundheitsfoerderung-allgemein/zzz-freie-position',
    '/ai/ordnungssystem/gesundheit/gesundheitsfoerderung-und-praevention/gesundheitsfoerderung-allgemein/ubertragbare-krankheiten',
    '/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/sozialhilfe/sozialhilfeleistungen',
    '/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/sozialhilfe/fuersorge/inkasso-sozialhilfe',
    '/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/sozialhilfe/nothilfe',
    '/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/asylwesen/asylkoordination/jahresrechnung',
    '/ai/ordnungssystem/verkehr/strassen/strassen/staatsstrassen',
    '/ai/ordnungssystem/verkehr/strassen/langsamverkehr/velowege/velo',
    '/ai/ordnungssystem/verkehr/strassen/langsamverkehr/mountainbikewege/mountainbike',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/grundlagen',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsplanung/bund-bav',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsplanung/kantone-konferenzen',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/angebotsumsetzung',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsumsetzung/bund-bav',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsumsetzung/kantone-konferenzen',

    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/kantonaler-richtplan',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/kantonaler-nutzungsplan',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/raumbeobachtung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/uberkantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungen-der-bezirke',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/regionalplanung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/nutzungsplanung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/quartierplanungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/bundes-ausserkantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-nutzungsplan/bundesplanungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-nutzungsplan/ausserkantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/genemigungen-konzessionen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/bewilligungsbehorden',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren/inneres-land',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren/oberegg',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/naturgefahren/pravention',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/gewaesser/organisation-gewaesserbau/schutzbautenkataster',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/projekte/projekteausbauten-hochwasserschutz/projekte',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/organisation-umweltschutz/kommunikation-offentlichkeitsarbeit',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/organisation-umweltschutz/bewilligungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/gewaesserschutz/konzessionen',
    '/ai/ordnungssystem/bau-raumordnung-siedlungsentwasserung/umwelt-und-landschaftsschutz/siedlungsentwasserung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/bauten',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/perimeter',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kanalanschlussgebuhren',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kanalbenutzungsgebuhren',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kleinklaranlagen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/finanzen',
]


PATH_FALLBACK = {
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/kommunikation/medien':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/kommunikation/medien',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rekurse-weitere-rechtsmittelverfahren/weitere-rechtsmittelverfahren':'/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rechtsverfahren-gesetzgebung/weitere-rechtsmittelverfahren',
    '/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rekurse-rechtsverfahren/gesetzgebung':'/ai/ordnungssystem/allgemeine-verwaltung/legislative-und-exekutive/standeskommission/rechtsverfahren-gesetzgebung/gesetzgebung',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/externe-interne-kommunikation-information/internetauftritt':'/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/kommunikation-information/internetauftritt',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/kriminalpolizei/brandmeldeanlagen':'/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/einsatzzentrale/brandmeldeanlagen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/kriminalpolizei/alarmanlagen':'/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kantonspolizei/einsatzzentrale/alarmanlagen',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/handaenderungssteuern/burgschaften':'/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/grundbuchwesen/guter-erbrechtliche-rechtsgeschafte/burgschaften',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/handelsregister/unternehmen/genossenschaften':'/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/allgemeines-rechtswesen-ii/handelsregister/unternehmen/vereine',
    '/ai/ordnungssystem/bildung/volksschule/grundlagen':'/ai/ordnungssystem/bildung/volksschulamt/grundlagen',
    '/ai/ordnungssystem/bildung/volksschule/volksschule':'/ai/ordnungssystem/bildung/volksschulamt/volksschule',
    '/ai/ordnungssystem/bildung/volksschule/allgemeines-volksschule/klassenlisten':'/ai/ordnungssystem/bildung/volksschulamt/volksschule/klassenlisten',
    '/ai/ordnungssystem/bildung/volksschule/allgemeines-volksschule/lehrmittel':'/ai/ordnungssystem/bildung/volksschulamt/volksschule/lehrmittel',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/fuhrung-organisation':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/fuhrung-organisation',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/ressourcen-support':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/personal':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support/personal',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/infrastruktur':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support/infrastruktur',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/finanzen':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support/finanzen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen/informatik':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support/informatik',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/allgemeines/beziehungen':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/amtsleitung/ressourcen-support',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/beziehungen':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/beziehungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lehrvertrage':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrvertrage',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/lernende':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrvertrage/lernende',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/nachteilsausgleich':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrvertrage/nachteilsausgleich',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/qualifikationsverfahren/aktion-offentlicher-verkehr':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrvertrage/aktion-offentlicher-verkehr',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lehrbetriebe':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrbetriebe',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lernendenangebote/bildungsbewilligungen':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrbetriebe/bildungsbewilligungen',
    '/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehrlingswesen/lernendenangebote/berufsbildner':'/ai/ordnungssystem/bildung/berufsbildung-berufsberatung/lehraufsicht-ausbildungsberatung/lehrbetriebe/berufsbildner',
    '/ai/ordnungssystem/gesundheit/gesundheitswesen/ambulante-gesundheitsdienste/bewilligungs-aufsichtswesen':'/ai/ordnungssystem/gesundheit/gesundheits-alterswesen/ambulante-gesundheitsdienste/bewilligungs-aufsichtswesen',
    '/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/sozialhilfe/fuersorge/inkasso-sozialhilfe':'/ai/ordnungssystem/soziale-sicherheit/sozialhilfe-und-asylwesen/sozialhilfe/sozialhilfeleistungen/inkasso-sozialhilfe',
    '/ai/ordnungssystem/verkehr/strassen/langsamverkehr/velowege/velo':'/ai/ordnungssystem/verkehr/strassen/langsamverkehr/velowege/mountainbike',
    '/ai/ordnungssystem/verkehr/strassen/langsamverkehr/mountainbikewege/mountainbike':'/ai/ordnungssystem/verkehr/strassen/langsamverkehr/velowege/mountainbike',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsplanung/bund-bav':'/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/grundlagen/bund-bav',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsplanung/kantone-konferenzen':'/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/grundlagen/kantone-konferenzen',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsumsetzung/bund-bav':'/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/angebotsumsetzung/bund-bav',
    '/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/verkehrs-angebotsumsetzung/kantone-konferenzen':'/ai/ordnungssystem/verkehr/oeffentlicher-verkehr/angebotsumsetzung/kantone-konferenzen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/kantonaler-richtplan':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonale-planungen/kantonaler-richtplan',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/kantonaler-nutzungsplan':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonale-planungen/kantonaler-nutzungsplan',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/raumbeobachtung':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonale-planungen/kantonaler-richtplan',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungsgrundlagen/uberkantonale-planungen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonale-planungen/uberkantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/regionalplanung':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungen-der-bezirke/regionalplanung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/nutzungsplanung':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungen-der-bezirke/nutzungsplanung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-richtplan/quartierplanungen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/planungen-der-bezirke/quartierplanungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-nutzungsplan/bundesplanungen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/bundes-ausserkantonale-planungen/bundesplanungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/kantonaler-nutzungsplan/ausserkantonale-planungen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/raumplanung/bundes-ausserkantonale-planungen/ausserkantonale-planungen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/bewilligungsbehorden':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/genemigungen-konzessionen/bewilligungsbehorden',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren/inneres-land':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/genemigungen-konzessionen/bewilligungsbehorden/inneres-land',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren/oberegg':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/genemigungen-konzessionen/bewilligungsbehorden/oberegg',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/projekte/projekteausbauten-hochwasserschutz/projekte':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/gewaesser/gewaesserausbauten-hochwasserschutz/gewaesser',
    '/ai/ordnungssystem/bau-raumordnung-siedlungsentwasserung/umwelt-und-landschaftsschutz/siedlungsentwasserung':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/bauten':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/bauten',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/perimeter':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/perimeter',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kanalanschlussgebuhren':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/kanalanschlussgebuhren',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kanalbenutzungsgebuhren':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/kanalbenutzungsgebuhren',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/kleinklaranlagen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/kleinklaranlagen',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/umweltschutz/finanzen':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/umwelt-und-landschaftsschutz/siedlungsentwasserung/finanzen',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/sgba-projekte':'/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortpromotion-st-gallenbodenseearea-sgba/sgba-projekte',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/prasentationen-aktivitaten':'/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortpromotion-st-gallenbodenseearea-sgba/prasentationen-aktivitaten',
    '/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/st-gallen-bodensee-area/institutionen-partner-scouts':'/ai/ordnungssystem/volkswirtschaft/allgemeines/wirtschaftsfoerderung/standortpromotion-st-gallenbodenseearea-sgba/institutionen-partner-scouts',
    '/ai/ordnungssystem/volkswirtschaft/landwirtschaft/organisation-landwirtschaft/beziehungen/kolas-kolas-o':'/ai/ordnungssystem/volkswirtschaft/landwirtschaft/organisation-landwirtschaft/beziehungen/sachbearbeitertagung-bodenrecht',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldschutz/forststatistik':'/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/holznutzung/forststatistik',
    '/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/waldpflege-waldnutzung/nais':'/ai/ordnungssystem/volkswirtschaft/forstwirtschaft/schutzwald/nais',
    '/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/nfa-bund':'/ai/ordnungssystem/finanzen-und-steuern/finanzen/staendige-aufgaben/geldfluss-zu-korperschaften',
    '/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/baugesuche/baugesuchsverfahren':'/ai/ordnungssystem/bau-raumordnung-umweltschutz/allgemeines/genemigungen-konzessionen/bewilligungsbehorden',
    '/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/grundlagen/stiftung-internat':'/ai/ordnungssystem/allgemeine-verwaltung/allgemeine-verwaltung/personal/personaladministration/allgemeines/stiftung-internat',
    '/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kriminalpolizei/kommunikationsuberwachung':'/ai/ordnungssystem/oeffentliche-ordnung-sicherheit-verteidigung/oeffentliche-sicherheit/kriminalpolizei/staatsschutz',
}


class FixReferenceNumbers(object):

    def __init__(self, options):
        self.options = options
        self.catalog = api.portal.get_tool('portal_catalog')
        self.intid_utility = getUtility(IIntIds)

    def fix_reference_numbers(self):
        for path in REPO_PATHS:
            try:
                repo = api.portal.get().unrestrictedTraverse(path)
            except KeyError:
                path = PATH_FALLBACK.get(path)
                repo = api.portal.get().unrestrictedTraverse(path)

            if not repo or not path:
                import pdb; pdb.set_trace()


            print 'Start fixing dossiers for {}'.format(repo)
            print SEPARATOR
            brains = self.catalog.unrestrictedSearchResults(
                object_provides=IDossierMarker.__identifier__,
                path=path, sort_on='path')

            for brain in brains:
                self.fix_dossier(brain.getObject())

    def fix_dossier(self, dossier):
        intid = self.intid_utility.getId(dossier)
        parent = aq_parent(aq_inner(dossier))

        former_reference_number = IDossier(dossier).former_reference_number
        if not former_reference_number:
            print 'SKIPPED Dosier ({}{}) created at '.format(
                '/'.join(dossier.getPhysicalPath()), dossier.created())
            return
        former_prefix = former_reference_number.split('-')[-1].split('.')[-1]

        ref_mapping = IAnnotations(parent).get('dossier_reference_mapping')
        old_prefix = former_prefix
        new_prefix = ref_mapping.get('reference_prefix').get(intid)

        # check if former_reference_number is registered for current dossier
        if ref_mapping.get('reference_numbers').get(former_prefix) != intid:
            print 'Check failed'
            import pdb; pdb.set_trace()
        else:
            # free new prefix (remove it from the numbers list)
            ref_mapping['reference_numbers'].pop(new_prefix)

        # set old prefix as current
        ref_mapping['reference_prefix'][intid] = old_prefix.decode('utf-8')

        print '{} --> {} ({})'.format(new_prefix, old_prefix,
                                      '/'.join(dossier.getPhysicalPath()))

        self.reindex_dossier_and_children(dossier)

    def reindex_dossier_and_children(self, dossier):
        children = self.catalog(path='/'.join(dossier.getPhysicalPath()))
        for child in children:
            obj = child.getObject()
            obj.reindexObject(idxs=['reference'])

            if ITask.providedBy(obj):
                obj.get_sql_object().sync_with(obj)


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    print SEPARATOR
    print "Date: {}".format(datetime.now().isoformat())
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    fixer = FixReferenceNumbers(options)
    fixer.fix_reference_numbers()
    if not options.dry_run:
        import pdb; pdb.set_trace()
        transaction.commit()

    print "Done."
    print SEPARATOR
    print SEPARATOR


if __name__ == '__main__':
    main()
