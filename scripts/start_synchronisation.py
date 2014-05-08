from opengever.ogds.base.ldap_import.sync_ldap import run_import
from optparse import OptionParser


def main():
    # check if we have a zope environment aka 'app'
    mod = __import__(__name__)
    if not ('app' in dir(mod) or 'app' in globals()):
        print "Must be run with 'bin/instance run'."
        return

    parser = OptionParser()
    parser.add_option("-s", "--site-root",
                      dest="site_root", default=u'/Plone')
    parser.add_option('-u', "--update-syncstamp",
                      dest="update_syncstamp", default=True)
    (options, args) = parser.parse_args()

    run_import(app, options)

if __name__ == '__main__':
    main()

