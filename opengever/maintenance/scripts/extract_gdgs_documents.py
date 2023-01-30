from Acquisition import aq_base
from opengever.base.interfaces import IOpengeverBaseLayer
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from os.path import join as pjoin
from plone.restapi.interfaces import ISerializeToJson
from zope.component import queryMultiAdapter
from zope.globalrequest import getRequest
from zope.interface import alsoProvides
import argparse
import errno
import hashlib
import json
import os
import shutil
import sys
import transaction


# Modified objects according to this Solr query:
# q=modified%3A%5B2023-01-23T02%3A55%3A00Z%20TO%202023-01-25T14%3A20%3A00Z%5D&rows=1000

CHANGED_OBJS = [
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/kommunikation-und-oeffentlichkeitsarbeit/webauftritt/dossier-20042",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/kommunikation-und-oeffentlichkeitsarbeit/webauftritt/dossier-20042/document-184900",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/kommunikation-und-oeffentlichkeitsarbeit/webauftritt/dossier-20042/document-184919",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19523",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19523/document-184929",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073/document-184884",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073/document-184885",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073/document-184930",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073/document-184931",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19522/dossier-19524/dossier-20073/document-184932",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19870",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19870/document-184939",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19870/document-184940",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19870/document-184942",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-19899/dossier-20232",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-19899/dossier-20232/document-184863",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-20238",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-20238/document-184853",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-20238/document-184988",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/operative-fuehrung/aufbauorganisation/administratives-und-organisatorisches/dossier-19898/dossier-20238/document-184989",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/parlamentarische-vorstoesse/dossier-20202",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/parlamentarische-vorstoesse/dossier-20202/document-184935",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/regierungsgeschaefte/dossier-19988/dossier-20045/document-184708",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20248",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20248/document-184933",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20248/document-184934",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20255",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20255/document-184980",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20255/task-1233",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20255/task-1233/document-184981",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/document-184999",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234/document-184991",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234/document-184992",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234/document-184993",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234/document-184994",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20266/task-1234/document-184995",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20267",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20267/document-185002",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20267/task-1235",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20267/task-1235/document-185000",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-departement/politische-mitwirkung/stellungnahmen-und-mitberichte/dossier-20267/task-1235/document-185001",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/kantonsapotheke/wissensmanagement/dossier-12260/document-154467",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/kantonsapotheke/wissensmanagement/dossier-19832",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/kantonsapotheke/wissensmanagement/dossier-19832/document-184947",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/kantonsapotheke/wissensmanagement/dossier-19832/document-184949",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/kantonsapotheke/wissensmanagement/dossier-19832/document-184950",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/lernende/dossier-17097/dossier-17098/dossier-17532/document-161656",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/document-168621",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/document-168625",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/document-170427",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/document-184892",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/task-1231",
    "/gdgs/ordnungssystem/fuehrung-und-koordination-generalsekretariat/operative-fuehrung/aufbauorganisation/rechtsdienst/wissensmanagement/dossier-16043/dossier-18089/task-1232",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/planung/dossier-17659/dossier-17661/dossier-17681",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/planung/dossier-17659/dossier-17661/dossier-17681/document-184881",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184665",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184854",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184855",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184856",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184857",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184858",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184859",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-15874/document-184902",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184847",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184848",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184849",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184906",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184907",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184908",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184909",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184910",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184911",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184912",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184913",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184914",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184915",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-18093/document-184916",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-20251",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-20251/document-184961",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-20251/document-184962",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/rechnungsfuehrung-und-zahlungsabwicklung/dossier-20251/document-184963",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-16468/dossier-17171/dossier-18161/document-165165",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184679",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184921",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184922",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184923",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184924",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184964",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184977",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184996",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184997",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-184998",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-185003",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20220/document-185004",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20252",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20252/document-184965",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20252/document-184966",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20253",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-18656/dossier-20253/document-184968",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20127",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20127/document-184597",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20127/document-184985",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20127/document-184986",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20129",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20129/document-184984",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/finanzen/staatsrechnung/dossier-18585/dossier-19954/dossier-20129/document-184987",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/informatik-und-telefonie/it-security/dossier-20116/document-184042",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/informatik-und-telefonie/it-security/dossier-20116/document-184595",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/allgemeines-und-uebergreifendes/dossier-2789/dossier-19861/document-184162",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalaustritte/dossier-3780/dossier-3781",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalaustritte/dossier-3780/dossier-3781/document-184869",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalaustritte/dossier-3780/dossier-3784",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalaustritte/dossier-3780/dossier-3784/document-184868",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3482/dossier-19791",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3482/dossier-19791/document-184973",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3482/dossier-19791/document-184974",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3482/dossier-19792",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3482/dossier-19792/document-184972",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3523/dossier-3553",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3523/dossier-3553/document-184936",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3558/dossier-3582",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3558/dossier-3582/document-184585",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalbetreuung/dossier-3558/dossier-3582/document-184867",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalentloehnung/dossier-10904/dossier-12270/document-112638",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalentloehnung/dossier-14557/document-184586",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalentwicklung/dossier-3603",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalentwicklung/dossier-3603/document-184956",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-17918/dossier-17938/dossier-20016",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-17918/dossier-17938/dossier-20016/document-173150",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-20101/dossier-20143/dossier-20144",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-20101/dossier-20143/dossier-20144/document-184886",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-20101/dossier-20143/dossier-20144/document-184887",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-20101/dossier-20143/dossier-20144/document-184888",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-20101/dossier-20143/dossier-20144/document-185005",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/personal/personaladministration/personalgewinnung/dossier-3220/dossier-20110/document-184184",
    "/gdgs/ordnungssystem/support-und-ressourcen-departement/politische-planung/geschaeftsbericht/dossier-19809/dossier-19938/document-183934",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/document-184851",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/document-184897",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/document-184905",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/document-184975",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/document-184976",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/dossier-20098",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/kundendienstleistungen-kantonsapotheke/dossier-19437/dossier-19821/dossier-20098/document-184978",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/weitere-beratungen-und-auskuenfte/dossier-17024/dossier-20053",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/beratung-und-auskuenfte/weitere-beratungen-und-auskuenfte/dossier-17024/dossier-20053/document-184866",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-10805/dossier-19273",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-10805/dossier-19273/document-184904",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-15787/dossier-19846",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-15787/dossier-19846/document-184891",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19275/dossier-19415",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19275/dossier-19415/document-184928",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19296/dossier-20001",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19296/dossier-20001/document-184979",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19698/dossier-19700",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19698/dossier-19700/document-184861",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19698/dossier-19700/document-184882",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19710/dossier-19711",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19710/dossier-19711/document-184927",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19781/dossier-19782",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19781/dossier-19782/document-184926",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19783/dossier-19784",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19783/dossier-19784/document-184925",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19812/dossier-19813",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19812/dossier-19813/document-184903",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/document-184879",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885/document-184938",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885/document-184941",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885/document-184943",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885/document-184944",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/aerztliche-privatapotheken/dossier-19884/dossier-19885/document-184946",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-10358",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-10358/dossier-20265",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-10358/dossier-20265/document-184982",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-19862",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-19862/dossier-20242",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-19862/dossier-20242/document-184893",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-19862/dossier-20242/document-184895",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-5754/dossier-12858",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/apotheken/dossier-5754/dossier-12858/document-184917",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-13693",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-13693/dossier-20240",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-13693/dossier-20240/document-184865",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-13693/dossier-20240/document-184898",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-13693/dossier-20240/document-184899",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184864",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184951",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184952",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184953",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184954",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184955",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184959",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20239/document-184960",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20250",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/drogerien/dossier-18685/dossier-20260",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-10278",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-10278/document-184873",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-10278/document-184874",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-14970",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-14970/document-184875",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-16076",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-16076/document-184876",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-16076/document-184878",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-18899",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-18899/document-184877",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19630",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19630/document-184872",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797/document-181003",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797/document-181463",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797/document-184852",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797/document-184894",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/swissmedic/dossier-19797/document-184948",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/zahnaerztliche-privatapotheken/dossier-18465",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-kantonsapotheke/zahnaerztliche-privatapotheken/dossier-18465/dossier-20262",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/bewilligungen-spitex/dossier-17585/document-161773",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184862",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184883",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184896",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184957",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184958",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18144/document-184990",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-170472",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184822",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184841",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184842",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184843",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184844",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184845",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184846",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184850",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184871",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184889",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184890",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184901",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184918",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184967",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184969",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184971",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18275/document-184983",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18299",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18299/document-184937",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-18299/document-184945",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-20254",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/bewilligungswesen-und-aufsicht/weitere-bewilligungen/dossier-20254/document-184970",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/pflege-und-entwicklung/ausbildungsverpflichtungen/dossier-19091/dossier-19096/document-183988",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/rechtspflege/rechtsauskuenfte/dossier-18087/dossier-18121",
    "/gdgs/ordnungssystem/weitere-kernaufgaben/rechtspflege/rechtsauskuenfte/dossier-18087/dossier-18121/document-184920",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/gremien/dossier-18003",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/gremien/dossier-18003/dossier-20241",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/gremien/dossier-18003/dossier-20241/document-184870",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/gremien/dossier-20119/dossier-20231/document-184761",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179/document-184860",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179/dossier-20180",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179/dossier-20180/document-184302",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179/dossier-20180/document-184315",
    "/gdgs/ordnungssystem/zusammenarbeit-mit-dritten/projekte/dossier-20179/dossier-20180/document-184880",
]

EXTRACTION_PATH = '/home/zope/01-gever-gdgs-prod/extracted_documents'


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class DocumentsExctractor(object):

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.request = getRequest()
        alsoProvides(self.request, IOpengeverBaseLayer)
        mkdir_p(EXTRACTION_PATH)

    def run(self):
        for path in CHANGED_OBJS:
            obj = self.portal.unrestrictedTraverse(path)
            portal_type = obj.portal_type

            print('Getting metadata for %s' % path)
            uid = obj.UID()
            metadata = self.get_metadata(obj)
            metadata_filename = '%s.json' % uid

            namedfile = None
            if portal_type == 'opengever.document.document':
                namedfile = aq_base(obj).file
            elif portal_type == 'ftw.mail.mail':
                namedfile = aq_base(obj).message
            else:
                # No blob to extract for these types
                assert portal_type in (
                    'opengever.dossier.businesscasedossier',
                    'opengever.task.task',
                    'opengever.repository.repositoryfolder',
                )

            if namedfile:
                filename = namedfile.filename
                data = namedfile.data
                content_type = namedfile.contentType
                zodb_blob_path = namedfile._blob.committed()
                claimed_length = len(data)
                checksum = hashlib.md5(data).hexdigest()

                blob_fn = '%s.blob' % uid
                extracted_blob_path = pjoin(EXTRACTION_PATH, blob_fn)
                shutil.copy2(src=zodb_blob_path, dst=extracted_blob_path)

                metadata.update({
                    '_blob_filename': filename,
                    '_blob_content_type': content_type,
                    '_blob_md5_checksum': checksum,
                    '_blob_path': extracted_blob_path,
                    '_blob_claimed_length': claimed_length,
                })

            metadata_path = pjoin(EXTRACTION_PATH, metadata_filename)
            with open(metadata_path, 'w') as outfile:
                json.dump(metadata, outfile, indent=4)

    def get_metadata(self, obj):
        serializer = queryMultiAdapter((obj, self.request), ISerializeToJson)
        data = serializer()
        return data


if __name__ == '__main__':
    transaction.doom()
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest='site_root', default=None,
                        help='Absolute path to the Plone site')

    options = parser.parse_args(sys.argv[3:])

    plone = setup_plone(app, options)

    extractor = DocumentsExctractor(plone, options)
    extractor.run()
