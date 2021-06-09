from plone import api
from opengever.nightlyjobs.runner import NightlyJobRunner
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
import transaction


def main():
    app = setup_app()
    setup_plone(app)

    catalog = api.portal.get_tool("portal_catalog")
    brain = catalog.unrestrictedSearchResults(portal_type="opengever.document.document")[0]

    runner = NightlyJobRunner()
    provider = runner.get_job_providers()["nightly-maintenance-jobs-provider"]
    provider.add_to_queue(
        {"module_name": "opengever.maintenance.nightlies.add_reindex_document_job",
         "function_name": "reindex_object",
         "UID": brain.getObject().UID()})
    transaction.commit()
    print provider.get_queue()


def reindex_object(job):
    print "reindexing!!!!!!!"
    catalog = api.portal.get_tool("portal_catalog")
    brain = catalog.unrestrictedSearchResults(UID=job["UID"])[0]
    brain.getObject().reindexObject()


if __name__ == '__main__':
    main()
