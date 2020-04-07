from opengever.base.ip_range import valid_ip_range
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.officeconnector.interfaces import IOfficeConnectorSettings
from plone import api
from zope.interface import Invalid
import sys
import transaction


def set_emm_ip_ranges(ip_ranges):
    """Search all MS OneNote documents and fixes the contentType.
    """
    try:
        valid_ip_range(ip_ranges)
        api.portal.set_registry_record(
            'office_connector_disallowed_ip_ranges',
            ip_ranges,
            interface=IOfficeConnectorSettings)
        print('EMM IP ranges set')
    except Invalid as err:
        print(err)
        print("Aborting")
        sys.exit(1)


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    if not len(args) == 1:
        print "Missing argument, please provide the IP ranges in CIDR notation"
        sys.exit(1)

    ip_ranges = unicode(args[0])

    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    set_emm_ip_ranges(ip_ranges)

    if not options.dry_run:
        transaction.commit()


if __name__ == '__main__':
    main()
