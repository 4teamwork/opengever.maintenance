from ftw.mail.mail import IMail
from opengever.contact.contact import IContact
from opengever.document.behaviors import IBaseDocument
from opengever.document.document import IDocumentSchema
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.inbox.forwarding import IForwarding
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task.task import ITask
from opengever.tasktemplates.content.tasktemplate import ITaskTemplate
from plone import api


TYPES = {
    'dossiers': IDossierMarker,
    'documents': IDocumentSchema,
    'mails': IMail,
    'base_documents': IBaseDocument,
    'tasks': ITask,
    'repositoryfolders': IRepositoryFolder,
    'repositoryroots': IRepositoryRoot,
    'tasktemplates': ITaskTemplate,
    'contacts': IContact,
    'forwardings': IForwarding,
}


def get_contenttype_stats(plone):
    catalog_stats = {}
    catalog = api.portal.get_tool('portal_catalog')
    for type_key, iface in TYPES.items():
        brains = catalog.unrestrictedSearchResults(
            object_provides=iface.__identifier__)
        catalog_stats[type_key] = len(brains)
    return catalog_stats
