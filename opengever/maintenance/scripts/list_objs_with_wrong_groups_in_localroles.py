"""
"""
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import transaction


WRONG_GROUP_IDS = [
    "R_ADM_FIN ",
    "R_ADM_MOB ",
    "R_ZSA_WAH ",
    "R_GA_LV_7_M12 ",
    "R_DIR_ADJ_ICT ",
    "R_GA_LV_6_42 ",
    "R_FOR_25 ",
    "philippe.gross",
    "lukas.graf",
]

DEBUG_URL_PREFIX = 'http://nohost/fd/'
PROPER_URL_PREFIX = 'http://localhost:8080/fd/'
PORTAL_TYPES = ['opengever.repository.repositoryfolder']


def output_obj(obj, wrong_group_ids):
    wrong_ids = '|'.join(wrong_group_ids)
    url = obj.absolute_url()
    url = url.replace(DEBUG_URL_PREFIX, PROPER_URL_PREFIX)
    title = obj.Title()
    row = '"%s","%s","%s"' % (url, title, wrong_ids)
    print row.decode('utf-8').encode('windows-1252')


def list_objs_with_wrong_groups_in_localroles():
    catalog = catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type=PORTAL_TYPES)
    objects = [brain.getObject() for brain in brains]
    for obj in objects:
        wrong_group_ids = []
        local_roles = obj.__ac_local_roles__
        for wrong_id in WRONG_GROUP_IDS:
            if wrong_id in local_roles:
                wrong_group_ids.append(wrong_id)

        if wrong_group_ids:
            output_obj(obj, wrong_group_ids)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()
    plone = setup_plone(app, options)

    transaction.doom()
    list_objs_with_wrong_groups_in_localroles()
