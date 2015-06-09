from AccessControl.SecurityManagement import newSecurityManager
from optparse import OptionParser
from Testing.makerequest import makerequest
from zc.relation.interfaces import ICatalog
from zope.component import getUtility
from zope.component.hooks import setSite
import transaction


def log(msg):
    print msg


def setup_plone_site(app, options):
    # setup request and get plone site
    app = makerequest(app)
    plone = app.unrestrictedTraverse(options.site_root)

    # setup user context
    user = app.acl_users.getUser('zopemaster')
    user = user.__of__(app.acl_users)
    newSecurityManager(app, user)

    #setup site
    setSite(plone)
    return plone


def check_relations(site):
    rel_catalog = getUtility(ICatalog)

    relations = rel_catalog.findRelations(
        {'from_attribute': 'relatedItems'})

    relations = list(relations)
    for rel in relations:
        for attr in ['from_id', 'to_id', 'from_object', 'from_id']:
            try:
                dummy = getattr(rel, attr)
            except KeyError:
                print "Relation %s is damaged. KeyError when accessing 'rel.%s'" % (rel, attr)
        if rel.isBroken():
            print "Relation %s is marked as broken"
    print "Checked %s relations" % len(relations)


def main():
    # check if we have a zope environment aka 'app'
    mod = __import__(__name__)
    if not ('app' in dir(mod) or 'app' in globals()):
        print "Must be run with 'zopectl run'."
        return

    parser = OptionParser()
    parser.add_option("-s", "--site-root", dest="site_root", default='mandant1')
    (options, args) = parser.parse_args()

    plone = setup_plone_site(app, options)
    check_relations(plone)

    transaction.commit()


if __name__ == '__main__':
    main()
