from Products.Five.browser import BrowserView
from opengever.maintenance.scripts.repository_migration_analyse import RepositoryExcelAnalyser
from opengever.maintenance.scripts.repository_migration_analyse import RepositoryMigrator


class RepositoryAnalyser(BrowserView):
    """Test view for the analyser, easier/faster than a script.
    """

    def __call__(self):
        self.diff_xlsx_path = '/Users/flipsi/Documents/GEVER/SG/hba_migration/hba_os_migration.xlsx'
        self.analyse_xlsx_path = '/Users/flipsi/Documents/GEVER/SG/hba_migration/analyse.xlsx'

        import transaction
        # transaction.doom()

        from plone.protect.interfaces import IDisableCSRFProtection
        from zope.interface import alsoProvides
        alsoProvides(self.request, IDisableCSRFProtection)
        analyser = RepositoryExcelAnalyser(self.diff_xlsx_path,
                                           self.analyse_xlsx_path)
        analyser.analyse()
        analyser.export_to_excel()

        migrator = RepositoryMigrator(analyser.analysed_rows)
        migrator.run()

        return 'DONE'
