from ftw.journal.config import JOURNAL_ENTRIES_ANNOTATIONS_KEY
from ftw.journal.interfaces import IAnnotationsJournalizable
from ftw.upgrade import ProgressLogger
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.annotation.interfaces import IAnnotations
from zope.i18nmessageid import Message
import logging
import sys
import transaction


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)

MAY_NEED_FIXING = ['title', 'repository']


class JournalMessageFixer(object):
    """Fixes encoding for zope.i18nmessageid.Message mapping values, for
    journal entries of objects which have been created with an OGGBundle
    import.

    See https://github.com/4teamwork/opengever.core/pull/3369 for more details.
    """

    def __init__(self, portal, options):
        self.portal = portal
        self.options = options
        self.counter = 0

    def get_journal_entries(self, obj):
        """Returns all journal entries for the given object.
        """
        if IAnnotationsJournalizable.providedBy(obj):
            annotations = IAnnotations(obj)
            return annotations.get(JOURNAL_ENTRIES_ANNOTATIONS_KEY, [])

        return []

    def fix_entries(self, obj):
        for entry in self.get_journal_entries(obj):
            message = entry['action'].get('title')
            if message and isinstance(message, Message) and message.mapping:
                if self.needs_fixing(message):
                    self.fix_message(entry, message)

    def needs_fixing(self, message):
        for key in MAY_NEED_FIXING:
            if key in message.mapping:
                value = message.mapping[key]
                if not isinstance(value, unicode):
                    return True
        return False

    def fix_message(self, entry, old_message):
        """Replaces the existing Messsage object with a new one,
        with mapping values in the correct encoding (unicode).

        Just replacing the mapping value is not enough, because the
        journal store (a persistent dict) is not marked as changed.
        """
        mapping = old_message.mapping
        for key in mapping.keys():
            if key in MAY_NEED_FIXING:
                value = mapping[key]
                if not isinstance(value, unicode):
                    mapping[key] = value.decode('utf-8')

        entry['action']['title'] = Message(
            unicode(old_message), domain=old_message.domain,
            default=old_message.default, mapping=mapping)

        self.counter += 1

    def run(self):
        catalog = api.portal.get_tool('portal_catalog')
        for brain in ProgressLogger(
                'Fix journal entries.', catalog(), logger=logger):
            self.fix_entries(brain.getObject())


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    fixer = JournalMessageFixer(plone, options)
    fixer.run()

    transaction.commit()
    print '{} journal entries fixed'.format(fixer.counter)


if __name__ == '__main__':
    main()
