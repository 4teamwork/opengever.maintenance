from Acquisition import aq_inner
from Acquisition import aq_parent
from csv import DictWriter
from datetime import datetime
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.maintenance.utils import elevated_privileges
from plone import api
from urlparse import urljoin
from zope.container.interfaces import INameChooser
import logging
import os
import sys
import transaction
import urllib


logger = logging.getLogger('opengever.maintenance')
handler = logging.StreamHandler(stream=sys.stdout)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)

SEPARATOR = '-' * 78

REPORT_PATH = os.path.abspath(
    os.path.join(__file__,
                 '..', '..', '..', '..',
                 'renamed_documents.csv'))


def use_canonical_document_id(options):
    """Rename documents that do not use the current naming-schema.

    Also write a short report of the renamed documents to a csv file.

    """
    catalog = api.portal.get_tool('portal_catalog')
    brains = catalog.unrestrictedSearchResults(
        portal_type='opengever.document.document')

    to_rename = []

    for brain in brains:
        name = brain.id
        if name.startswith('template-') or name.startswith('document-'):
            if options.verbose:
                logger.info("skipped: {}".format(brain.getPath()))
            continue

        to_rename.append(brain.getObject())

    if to_rename:
        logger.info(SEPARATOR)

    def build_url(fragment):
        fragment = urllib.quote(fragment)
        if not options.domain:
            return fragment

        # we assume a virtualhost and strip the plone site id
        assert fragment.startswith("/"), "path should start with /"
        visible_fragment = "/".join(fragment.split('/')[2:])
        base = "https://{}/".format(options.domain)
        return urljoin(base, visible_fragment)

    with open(REPORT_PATH, 'w+') as csvfile:
        writer = DictWriter(csvfile, fieldnames=["old_url", "new_url"])
        writer.writeheader()

        # elevated privileges are necessary to be able to modify stuff in
        # closed dossiers.
        with elevated_privileges():
            for obj in to_rename:
                parent = aq_parent(aq_inner(obj))
                old_name = obj.getId()
                old_url = '/'.join(obj.getPhysicalPath())
                new_name = INameChooser(parent).chooseName(None, obj)
                logger.info("renaming '{}' to '{}' ({})".format(
                    old_name, new_name, '/'.join(obj.getPhysicalPath())))
                api.content.rename(obj=obj, new_id=new_name)
                new_url = '/'.join(obj.getPhysicalPath())
                writer.writerow({
                    "old_url": build_url(old_url),
                    "new_url": build_url(new_url)
                })

    if not options.dry_run:
        transaction.commit()

    logger.info("done.")


def main():
    app = setup_app()

    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("--domain", dest="domain", default=None,
                      help="Domain used in links to renamed content in "
                           "report file.")
    (options, args) = parser.parse_args()

    if options.domain:
        assert not options.domain.startswith("http")
        assert not options.domain.endswith("/")

    logger.info(SEPARATOR)
    logger.info("Date: {}".format(datetime.now().isoformat()))
    setup_plone(app, options)

    if options.dry_run:
        transaction.doom()
        logger.info("DRY-RUN")

    use_canonical_document_id(options)


if __name__ == '__main__':
    main()
