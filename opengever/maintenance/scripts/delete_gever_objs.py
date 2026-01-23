# -*- coding: utf-8 -*-
#
# Delete objects by relative path passed as command-line arguments
# Made for Joel
#
# Usage:
# zopectl run delete_gever_objs.py [--dry-run] <path> [<path> ...]


from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from zope.globalrequest import setRequest
import transaction


def commit():
    print('Committing transaction...')
    transaction.commit()
    print('Done.')


def normalize_relative_path(raw_path, portal):
    """
    Normalize a path so it can be used with plone.unrestrictedTraverse().

    Accepts:
      - "ordnungssystem/foo"
      - "/ordnungssystem/foo"
      - "<site_id>/ordnungssystem/foo"
      - "/<site_id>/ordnungssystem/foo"

    Returns:
      - "ordnungssystem/foo"
    """
    path = raw_path.strip()

    if path.startswith('/'):
        path = path[1:]

    site_id = portal.getId()
    prefix = site_id + '/'

    if path.startswith(prefix):
        path = path[len(prefix):]

    return path


def delete_objects(options, paths):
    plone = api.portal.get()

    if not paths:
        print("ERROR: No paths provided.")
        return

    deleted = 0

    for i, raw_path in enumerate(paths, start=1):
        if i % 50 == 0:
            print("Progress: {}/{}".format(i, len(paths)))
            if not options.dry_run:
                commit()

        try:
            rel_path = normalize_relative_path(raw_path, plone)
            obj = plone.unrestrictedTraverse(rel_path, default=None)

            if not obj:
                print("Not found: {}".format(raw_path))
                continue

            if options.dry_run:
                print("[Dry-run] Would delete: {}".format(obj.absolute_url()))
            else:
                api.content.delete(obj=obj)
                print("Deleted: {}".format(obj.absolute_url()))
                deleted += 1

        except Exception as exc:
            print("ERROR processing {}: {}".format(raw_path, exc))

    if not options.dry_run:
        commit()

    print("\nDone.")
    print("Total paths: {}".format(len(paths)))
    print("Deleted: {}".format(deleted))
    print("Skipped or failed: {}".format(len(paths) - deleted))


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option(
        "-n", "--dry-run",
        action="store_true",
        dest="dry_run",
        default=False,
        help="Do not commit deletions"
    )

    (options, args) = parser.parse_args()

    if options.dry_run:
        print("Dry-run mode: no deletions will be committed.")
        transaction.doom()

    site = setup_plone(app, options)
    setRequest(site.REQUEST)

    delete_objects(options, args)
