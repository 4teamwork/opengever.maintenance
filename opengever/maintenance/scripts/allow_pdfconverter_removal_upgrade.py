"""
This script allows execution of the og.pdfconverter removal upgrade (ZUG)
for this deployment, by setting a flag in the Plone site annotations.

Example Usage:

    bin/instance run allow_pdfconverter_removal_upgrade.py
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from zope.annotation import IAnnotations
import transaction


ALLOW_UPGRADESTEP_FLAG = 'opengever_pdfconverter_removal_upgrade_allowed'


def allow_pdfconverter_removal_upgrade(plone, options):
    IAnnotations(plone)[ALLOW_UPGRADESTEP_FLAG] = True
    print "%r: pdfconverter removal upgrade is now allowed to run" % plone


def parse_options():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    return options, args


if __name__ == '__main__':
    app = setup_app()

    options, args = parse_options()
    plone = setup_plone(app, options)

    allow_pdfconverter_removal_upgrade(plone, options)
    transaction.commit()
