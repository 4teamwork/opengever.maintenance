"""
Will crop titles that exceed the respective SQL limit for some types of
objects (dossiers and tasks as of now).

- If the object has a description field and its empty, the old title will
  be saved there.
- A journal entry will be generated on the object itself or the next parent
  that has a journal.
"""
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.globalindex.handlers.task import TaskSqlSyncer
from opengever.journal import _
from opengever.journal.handlers import journal_entry_factory
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
from plone import api
import transaction


SEPARATOR = '-' * 78

MAX_TASK_LEN = 255
MAX_DOSSIER_LEN = 511

commit_needed = False


def create_cropped_title_journal_entry(obj, type_, in_desc=False):
    # German translations as a default. Hack, because we don't want to
    # maintain translations in opengever.maintenance
    if in_desc:
        title = _(u'label_title_cropped_saved_in_desc',
                  default=u'Titel gek\xfcrzt: ${obj_id}; Urspr\xfcnglicher '
                          u'Titel wurde in Feld "Beschreibung" gespeichert.',
                  mapping={'obj_id': obj.id})
    else:
        title = _(u'label_title_cropped',
                  default=u'Titel gek\xfcrzt: ${obj_id}',
                  mapping={'obj_id': obj.id})

    if type_ == 'Task':
        journal_target = obj.get_containing_dossier()
    elif type_ == 'Dossier':
        journal_target = obj
    else:
        raise NotImplementedError

    journal_entry_factory(
        journal_target, 'Title cropped', title, actor='SYSTEM')


def get_public_url(obj):
    admin_unit = get_current_admin_unit()
    path = '/'.join(obj.getPhysicalPath()[2:]).decode('ascii')
    url = u'/'.join((admin_unit.public_url, path.decode('ascii')))
    return url


def crop_title(title, max_len):
    assert isinstance(title, unicode)
    crop_marker = u' [...]'
    crop_len = max_len - len(crop_marker)
    cropped_title = title[:crop_len] + crop_marker
    return cropped_title


def crop_long_task_titles():
    """Find all tasks whose title is too long,
    and crop their titles.
    """
    _crop_long_object_titles(
        type_='Task',
        query=dict(portal_type=[
            'opengever.task.task',
            'opengever.inbox.forwarding']),
        max_len=MAX_TASK_LEN)


def crop_long_dossier_titles():
    """Find all dossiers whose title is too long,
    and crop their titles.
    """
    _crop_long_object_titles(
        type_='Dossier',
        query=dict(object_provides=IDossierMarker.__identifier__),
        max_len=MAX_DOSSIER_LEN)


def _crop_long_object_titles(type_, query, max_len):
    global commit_needed
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(**query)

    for brain in brains:
        obj = brain.getObject()

        old_title = obj.title
        old_length = len(old_title)

        if old_length > max_len:
            commit_needed = True
            cropped_title = crop_title(old_title, max_len)
            obj.title = cropped_title

            in_desc = False
            if type_ == 'Dossier':
                if not obj.description:
                    obj.description = old_title
                    in_desc = True
            elif type_ == 'Task':
                if not obj.text:
                    obj.text = old_title
                    in_desc = True
            else:
                raise NotImplementedError

            obj.reindexObject()

            url = get_public_url(obj)
            print u"Cropped title for {} {} ({} -> {}) [in_desc: {}]".format(
                type_, url, old_length, max_len, in_desc).encode('utf-8')

            create_cropped_title_journal_entry(obj, type_, in_desc=in_desc)

            if type_ == 'Task':
                # Tasks need to be synced to SQL after updating Plone obj
                TaskSqlSyncer(obj, None).sync()
                print "Synced Task."
            print u"Old Title: {}".format(old_title).encode('utf-8')
            print '-' * 80


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    (options, args) = parser.parse_args()

    print SEPARATOR
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        print "DRY-RUN"

    crop_long_dossier_titles()
    crop_long_task_titles()

    if commit_needed:
        if not options.dry_run:
            transaction.commit()

if __name__ == '__main__':
    main()
