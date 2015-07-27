from AccessControl.SecurityManagement import newSecurityManager
from optparse import OptionParser
from Products.CMFPlone.Portal import PloneSite
from Testing.makerequest import makerequest
from zope.component.hooks import setSite
import sys


MAX_FRAMES = 5


def get_plone_site(app):
    """Returns exactly one Plone Site from a Zope application object. If none
    or multiple Plone Sites are found an error is thrown.
    """
    sites = []
    for item_id, item in app.items():
        if isinstance(item, PloneSite):
            sites.append(item_id)
    if len(sites) == 1:
        print "INFO: Using Plone Site '%s'." % sites[0]
        return app.unrestrictedTraverse(sites[0])
    elif len(sites) > 1:
        print "ERROR: Multiple Plone Sites found. Please use -s to specify" \
              "which one to use."
    elif len(sites) == 0:
        print "ERROR: No Plone site found."
    sys.exit(1)


def setup_plone(app, options=None):
    """Takes care of setting up a request, retrieving a Plone site from a Zope
    app, setting sup a Manager security context and setting up the site manager
    for the Plone site.
    Returns the Plone site root object.
    """
    # Set up request for debug / bin/instance run mode.
    app = makerequest(app)

    # If no site specified in options, assume there's exactly one and use that.
    if not options or not options.site_root:
        plone = get_plone_site(app)
    else:
        plone = app.unrestrictedTraverse(options.site_root)

    # Set up Manager security context
    user = app.acl_users.getUser('zopemaster')
    user = user.__of__(app.acl_users)
    newSecurityManager(app, user)

    # Set up site to make component registry work
    setSite(plone)
    return plone


def setup_app(globals_dict=None):
    """Checks for availability of the magically injected Zope environment
    known as 'app' and returns it.
    """
    # check if we have a zope environment aka 'app'
    mod = __import__(__name__)
    outer_frame = sys._getframe(1)
    outer_globals = outer_frame.f_globals
    if 'app' in outer_globals:
        return outer_globals['app']
    elif globals_dict and 'app' in globals_dict:
        return globals_dict['app']
    elif 'app' in dir(mod):
        return mod.app
    elif 'app' in globals():
        return globals()['app']
    else:
        print "Global 'app' not found. Must be run with 'bin/instance0 run'."
        sys.exit(1)


def setup_debug_mode(globals_dict=None):
    """Sets up debug mode for a zope instance.
    Walks up the call stack looking for 'app' in frame globals, and then
    calls setup_plone() with it.
    """
    if globals_dict:
        app = globals_dict['app']
        plone = setup_plone(app)
        globals_dict['plone'] = plone
    else:
        app = None
        n = 1
        # Walk up the stack looking for 'app' in frame globals
        while n <= MAX_FRAMES:
            cur_frame = sys._getframe(n)
            app = cur_frame.f_globals.get('app')
            if app is not None:
                break
            n += 1

        if app is None:
            raise KeyError("Could not find global 'app' after checking "
                           "%s stack frames." % MAX_FRAMES)
        plone = setup_plone(app)
        cur_frame.f_globals['plone'] = plone


def setup_option_parser():
    """Sets up an OptionParser with common default options, and returns the
    parser.
    """
    parser = OptionParser()
    # Add a fake '-c' option to work around an issue with recent versions of
    # plone.recipe.zope2instance's bin/interpreter script.
    # See https://dev.plone.org/ticket/13414
    parser.add_option("-c", "--fake-option", dest="fake_option", default=None)

    parser.add_option("-s", "--site-root", dest="site_root", default=None)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                      default=False)
    return parser
