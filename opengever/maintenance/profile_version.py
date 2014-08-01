from Products.CMFCore.utils import getToolByName


def check_profile_id(profile_id):
    """Do some basic sanity checks on a profile_id.
    """

    if profile_id.startswith('profile-'):
        raise Exception("Please specify the profile_id WITHOUT "
                        "the 'profile-' prefix!")

    if not len(profile_id.split(':')) == 2:
        raise Exception("Invalid profile id '%s'" % profile_id)


def set_profile_version(context, profile_id, version):
    """Set the DB version for a particular profile.
    """

    check_profile_id(profile_id)
    ps = getToolByName(context, 'portal_setup')

    ps.setLastVersionForProfile(profile_id, unicode(version))
    assert(ps.getLastVersionForProfile(profile_id) == (version, ))
    print "Set version for '%s' to '%s'." % (profile_id, version)


def get_profile_version(context, profile_id):
    """Get the DB version for a particular profile.
    """

    check_profile_id(profile_id)
    ps = getToolByName(context, 'portal_setup')

    version = ps.getLastVersionForProfile(profile_id)
    return version
