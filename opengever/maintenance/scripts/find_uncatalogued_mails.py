"""
An bin/instance run script to find mails that are not cataloged.

bin/instance0 run find_uncatalogued_mails.py
"""

from ftw.mail.mail import IMail
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone


def recursive_find_uncatalogued_mails(parent, catalog, max_depth=None, depth=0):
    """Recursively traverse content starting at parent and find
    uncatalogued mails.
    """

    if max_depth and depth > max_depth:
        return
    if not hasattr(parent, 'objectItems'):
        return

    try:
        items = parent.objectItems()
    except:
        return

    for ident, item in items:
        if IMail.providedBy(item):
            path = '/'.join(item.getPhysicalPath())
            nof_results = len(catalog.unrestrictedSearchResults({'path': path}))
            if nof_results != 1:
                print "{} result(s) for: {}".format(nof_results, path)
        recursive_find_uncatalogued_mails(item, catalog,
                                          max_depth=max_depth, depth=depth+1)


def main():
    plone = setup_plone(setup_app())
    roots = plone.portal_catalog.unrestrictedSearchResults(
        {'portal_type': ['opengever.repository.repositoryroot']})
    for root in roots:
        print 'checking repository {}'.format(root.id)
        recursive_find_uncatalogued_mails(root, plone.portal_catalog)


if __name__ == '__main__':
    main()
