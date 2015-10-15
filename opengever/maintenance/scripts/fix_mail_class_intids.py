"""
A bin/instance run script which checks and fixes the class of
all mail objects that are references from the intid catalog.

bin/instance0 run fix_mail_class_intids.py
"""

from Acquisition import aq_base
from Acquisition import aq_parent
from ftw.mail.mail import Mail
from opengever.mail.mail import OGMail
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from Products.BTreeFolder2.BTreeFolder2 import BTreeFolder2Base
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
from zope.interface import directlyProvidedBy
from zope.interface import directlyProvides
import transaction


def find_key_references_with_wrong_class(intids):
    wrong_ids = [keyref for keyref in intids.ids
                 if keyref.object.__class__ == Mail]
    wrong_refs = [keyref for keyref in intids.refs.values()
                  if keyref.object.__class__ == Mail]
    assert set(wrong_ids) == set(wrong_refs)

    return wrong_refs


def fix_intids(intids, mail):
    """Fix reference in intids btree and keyreference objects.
    """

    intid = intids.getId(mail)
    reference_to_persistent = intids.refs[intid]

    del intids.refs[intid]
    del intids.ids[reference_to_persistent]

    intids.refs[intid] = reference_to_persistent
    intids.ids[reference_to_persistent] = intid

    reference_to_persistent._p_changed = True
    return intid


def fix_mail_class(plone):
    catalog = plone.portal_catalog
    intids = getUtility(IIntIds)

    wrong_refs = find_key_references_with_wrong_class(intids)
    print '{} wrong references in intid utility'.format(len(wrong_refs))

    for reference in wrong_refs:
        unwrapped_mail = reference.object

        # query mail from catalog for correct acquisition wrapping
        brains = catalog.unrestrictedSearchResults(UID=unwrapped_mail.UID())
        if len(brains) < 1:
            # some mails seem to be stuck in the intid catalog but cannot be
            # accessed otherwise. Also migrate them to be on the safe side.
            unwrapped_mail.__class__ = OGMail
            unwrapped_mail._ofs_migrated = True
            unwrapped_mail._p_changed = True
            fix_intids(intids, unwrapped_mail)
            continue

        mail = brains[0].getObject()
        # copied from ftw.upgrade.step.migrate_class
        mail.__class__ = OGMail
        base = aq_base(mail)
        base._ofs_migrated = True
        base._p_changed = True

        parent = aq_base(aq_parent(mail))
        id_ = base.getId()

        # fix entry in parent's btree
        if isinstance(parent, BTreeFolder2Base):
            del parent._tree[id_]
            parent._tree[id_] = base
        else:
            parent._p_changed = True

        intid = fix_intids(intids, mail)

        # Refresh provided interfaces cache
        directlyProvides(base, directlyProvidedBy(base))
        catalog.reindexObject(mail, idxs=['object_provides'],
                              update_metadata=False)

        print 'Mail {}:{} fixed new class: {}'.format(mail, intid, mail.__class__)
        print '/'.join(mail.getPhysicalPath())

    transaction.commit()


def main():
    plone = setup_plone(setup_app())
    fix_mail_class(plone)


if __name__ == '__main__':
    main()
