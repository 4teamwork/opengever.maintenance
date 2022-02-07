"""
Counts all plone contacts

    bin/instance run ./scripts/count_contacts.py

"""
from opengever.contact.contact import IContact
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import sys


class ContactsCounter(object):

    def __init__(self):
        self.catalog = api.portal.get_tool("portal_catalog")

    def count_contacts(self):
        return len(self.catalog.unrestrictedSearchResults({
            'object_provides': IContact.__identifier__,
            }))


def main():
    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    if not len(args) == 0:
        print "Not expecting any argument"
        sys.exit(1)

    app = setup_app()
    setup_plone(app)

    contacts_counter = ContactsCounter()
    n_contacts = contacts_counter.count_contacts()
    if n_contacts == 0:
        print("Not a single plone contact")
    else:
        print("Contains {} plone contacts".format(n_contacts))


if __name__ == '__main__':
    main()
