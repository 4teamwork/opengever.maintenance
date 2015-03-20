from five import grok
from opengever.usermigration.dossier import DossierMigrator
from Products.CMFPlone.interfaces import IPloneSiteRoot

MAPPING = {
    'erich.gollino': 'i777777',
    'vedat.akguel': 'i666666',
}

MAPPING2 = {
    'i777777': 'erich.gollino',
    'i666666': 'vedat.akguel',
}


class MigrateDossiersView(grok.View):
    """
    """

    grok.name('migrate-dossiers')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def render(self):
        migrator = DossierMigrator(self.context, MAPPING, 'move')
        return str(migrator.migrate())
