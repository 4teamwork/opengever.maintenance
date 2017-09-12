"""
Unifies casing (lower vs. upper case) of inbox and repofolder
responsible org units.
"""

from opengever.base.model import create_session
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.models.org_unit import OrgUnit
from opengever.repository.behaviors.responsibleorg import IResponsibleOrgUnit
from plone import api
import logging
import transaction


log = logging.getLogger('opengever.maintenance')
log.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


TYPES_TO_CHECK = (
    'opengever.inbox.inbox',
    'opengever.repository.repositoryfolder',
)


def parse_options():
    parser = setup_option_parser()
    parser.add_option("-n", "--dry-run", action="store_true",
                      dest="dryrun", default=False)
    (options, args) = parser.parse_args()
    return options, args


def unify_casing_for_inbox_responsible_org_units(plone, options):
    catalog = api.portal.get_tool('portal_catalog')
    session = create_session()

    # Create a mapping from lowercase OU ID to actual case in SQL
    ou_ids = [ou.unit_id for ou in session.query(OrgUnit).all()]
    ou_id_mapping = dict(zip([ou_id.lower() for ou_id in ou_ids], ou_ids))

    # Keep track of objects that need fixing
    to_fix = dict((portal_type, []) for portal_type in TYPES_TO_CHECK)

    for portal_type in TYPES_TO_CHECK:
        objects = [b.getObject() for b in catalog(portal_type=portal_type)]

        for obj in objects:
            behavior = IResponsibleOrgUnit(obj)
            plone_value = behavior.responsible_org_unit
            if not plone_value:
                # No responsible_org_unit set
                continue

            key = plone_value.lower()
            if key not in ou_id_mapping:
                log.warn('OU not in OGDS: %r (obj: %r)' % (
                    plone_value, obj))
                continue

            ogds_value = ou_id_mapping[key]
            if ogds_value == plone_value:
                # Casing matches, nothing to do
                continue

            to_fix[portal_type].append((ogds_value, obj))

    # List objects to fix, and fix them (if not dry-run)
    changed = False
    for portal_type in TYPES_TO_CHECK:

        log.info('')
        log.info(portal_type)
        log.info('=' * 80)

        for ogds_value, obj in to_fix[portal_type]:
            msg = ("Needs fixing: %r (responsible_org_unit == %r, "
                   "should be %r)" % (
                       obj,
                       IResponsibleOrgUnit(obj).responsible_org_unit,
                       ogds_value))
            log.info(msg)

            if not options.dryrun:
                IResponsibleOrgUnit(obj).responsible_org_unit = ogds_value
                log.info('Fixed: %r' % obj)
                changed = True

        if changed:
            transaction.commit()


def main():
    app = setup_app()

    options, args = parse_options()
    plone = setup_plone(app, options)

    if options.dryrun:
        transaction.doom()

    unify_casing_for_inbox_responsible_org_units(plone, options)

if __name__ == '__main__':
    main()
