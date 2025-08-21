import transaction
from opengever.base.behaviors.classification import (IClassification,
                                                     IClassificationMarker)
from opengever.maintenance.debughelpers import (setup_app, setup_option_parser,
                                                setup_plone)
from plone import api
from zope.globalrequest import setRequest


def commit():
    print("Committing transaction...")
    transaction.commit()
    print("Done.")


def update_classification(base_path, options):
    plone = api.portal.get()

    old_class = options.classification_from
    new_class = options.classification_to

    brains = plone.portal_catalog.unrestrictedSearchResults(
        object_provides=IClassificationMarker.__identifier__,
        path={"query": base_path, "depth": -1},
    )

    filtered_objs = []
    for brain in brains:
        obj = brain.getObject()
        if IClassification(obj).classification == old_class:
            filtered_objs.append(obj)

    print(
        "Filtered {} objects out of {} brains".format(len(filtered_objs), len(brains))
    )

    for i, obj in enumerate(filtered_objs, 1):
        IClassification(obj).classification = new_class
        print("Would update: {}".format(obj.absolute_url()))

        if not options.dry_run and i % 200 == 0:
            commit()
            print("Committed {}/{}  objects...".format(i, len(filtered_objs)))

    if not options.dry_run:
        commit()
        print("Committed all objects")


if __name__ == "__main__":
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option(
        "-n", "--dry-run", action="store_true", dest="dry_run", default=False
    )
    parser.add_option(
        "-f",
        "--from",
        dest="classification_from",
        help="Original classification token (e.g. 'public', 'confidential')",
    )
    parser.add_option(
        "-t",
        "--to",
        dest="classification_to",
        help="Target classification token (e.g. 'public', 'confidential')",
    )
    (options, args) = parser.parse_args()

    if options.dry_run:
        print("Dry-run mode: no updates will be committed.")
        transaction.doom()

    site = setup_plone(app, options)
    setRequest(site.REQUEST)

    base_path = "/{}/ordnungssystem".format(site.getId())

    update_classification(base_path, options)
