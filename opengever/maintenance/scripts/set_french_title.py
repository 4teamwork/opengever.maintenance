from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
import transaction
import json


SEPARATOR = "-" * 46


def count_objects(portal, options):

    _file = open('/home/zope/vlmrms.hepvs.ch/01-plone-gever/mapping_french_title_2.json', 'r')
    data = json.loads(_file.read())

    assert len(data) == 1548

    for item in data:
        repository = portal.unrestrictedTraverse(item.get('path').encode('utf-8'))
        if repository.title_de != item.get('title_de'):
            raise Exception('German title does not match')

        repository.title_fr = item.get('title_fr')
        repository.reindexObject()
        print u'Repository at {} fixed, french title: {}'.format(item.get('path'), item.get('title_fr'))

    if not options.dry_run:
        transaction.commit()

def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    print SEPARATOR
    plone = setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    count_objects(plone, options)

    if not options.dry_run:
        transaction.commit()

    print "Done."
    print SEPARATOR
    print SEPARATOR


if __name__ == '__main__':
    main()
