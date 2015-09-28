"""
A bin/instance run script which checks and fixes the class of
all mail objects based on querying the catalog.

bin/instance0 run fix_mail_class_query.py
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


def fix_mail_class(plone):
    catalog = plone.portal_catalog
    intids = getUtility(IIntIds)

    counter = 0
    mails_to_fix = []

    mails = catalog.unrestrictedSearchResults(
        {"portal_type": 'ftw.mail.mail'})

    for brain in mails:
        mail = brain.getObject()
        if mail.__class__.__name__ != 'OGMail':
            assert mail.__class__ == Mail
            counter = counter + 1
            mails_to_fix.append(mail)

    print '{} / {} with wrong class queried'.format(counter, len(mails))

    for mail in mails_to_fix:
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

        # fix reference in intids btree and keyreference object
        intid = intids.getId(mail)
        reference_to_persistent = intids.refs[intid]

        del intids.refs[intid]
        del intids.ids[reference_to_persistent]

        intids.refs[intid] = reference_to_persistent
        intids.ids[reference_to_persistent] = intid

        reference_to_persistent._p_changed = True

        # Refresh provided interfaces cache
        directlyProvides(base, directlyProvidedBy(base))

        mail.reindexObject()
        print 'Mail {}:{} fixed new class: {}'.format(mail, intid, mail.__class__)
        print '/'.join(mail.getPhysicalPath())

    transaction.commit()


def main():
    plone = setup_plone(setup_app())
    fix_mail_class(plone)

if __name__ == '__main__':
    main()
