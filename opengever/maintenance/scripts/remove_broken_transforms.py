"""
Removes broken portal transforms.
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
import transaction


SEPARATOR = '-' * 78
TRANSFORMS_TO_CHECK = ['lynx_dump', 'pdf_to_html', 'pdf_to_text']

need_to_commit = False


def unregister_broken_transform(portal_transforms, transform):
    # Remove from internal structures of portal_transforms - we need to do
    # this ourselves because portal_transforms is fucking stupid:
    # Broken transforms get their `inputs` attribute set to
    # (u'BROKEN', ), and pt._unmapTransform (called by unregisterTransform())
    # chokes on that.
    for in_mt, out_map in portal_transforms._mtmap.items():
        for out_mt, out_transforms in out_map.items():
            if transform in out_transforms:
                print "Removing {!r} from {!r} -> {!r}".format(
                    transform, in_mt, out_mt)
                idx = out_transforms.index(transform)
                out_transforms.pop(idx)

    # Set the transform inputs and output to empty tuple in order to be able
    # to delete it (_delObject fires the manage_beforeDelete hook, which tries
    # _unmapTransform again. *sigh*)
    transform.inputs = ()
    transform.output = ()

    # Finally, remove the actual transform
    portal_transforms._delObject(transform.name())


def remove_broken_transforms(plone):
    global need_to_commit

    portal_transforms = plone.portal_transforms
    for transform_name in TRANSFORMS_TO_CHECK:
        transform = portal_transforms.get(transform_name)

        if transform is None:
            continue

        if transform.title == 'BROKEN':
            unregister_broken_transform(portal_transforms, transform)
            need_to_commit = True
            print 'Unregistered transform {!r}'.format(transform_name)

    if need_to_commit:
        transaction.commit()


def main():
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    print SEPARATOR
    plone = setup_plone(app, options)
    remove_broken_transforms(plone)


if __name__ == '__main__':
    main()
