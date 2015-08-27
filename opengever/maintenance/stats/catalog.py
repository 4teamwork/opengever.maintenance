from ftw.mail.mail import IMail
from opengever.document.behaviors import IBaseDocument
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from opengever.task.task import ITask
from plone import api


TYPES = {
    'dossier': IDossierMarker,
    'document': IBaseDocument,
    'mail': IMail,
    'task': ITask,
    'repositoryfolder': IRepositoryFolder,
    'repositoryroot': IRepositoryRoot,
}


def get_contenttype_stats(plone):
    catalog_stats = {}
    catalog = api.portal.get_tool('portal_catalog')
    for type_key, iface in TYPES.items():
        brains = catalog.unrestrictedSearchResults(
            object_provides=iface.__identifier__)
        catalog_stats[type_key] = len(brains)
    return catalog_stats
