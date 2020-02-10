"""
Show default for 'preserved_as_paper_default'

    bin/instance run show_preserved_as_paper_default.py

"""
from opengever.document.interfaces import IDocumentSettings
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api


def show_preserved_as_paper_default(plone):
    value = api.portal.get_registry_record(
        'preserved_as_paper_default', interface=IDocumentSettings)
    print "client: %s | preserved_as_paper_default: %r" % (plone.id, value)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    show_preserved_as_paper_default(plone)
