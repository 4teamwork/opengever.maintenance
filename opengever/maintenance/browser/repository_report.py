from five import grok
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberFormatter
from opengever.base.interfaces import IReferenceNumberSettings
from opengever.repository.behaviors.responsibleorg import IResponsibleOrgUnit
from opengever.repository.repositoryroot import IRepositoryRoot
from plone import api
from StringIO import StringIO
from zope.component import queryAdapter
import csv


DELIMITER = ';'
QUOTECHAR = '"'
ENCODING = 'utf-8'

REFNUM_HEADER = u'Ordnungspositionsnummer'
TITLE_HEADER = u'Ordnungsposition'
ORG_UNIT_HEADER = u'Federf\xfchrendes Amt'


RESPONSIBLE_ORG_BEHAVIOR = (
    'opengever.repository.behaviors.responsibleorg.IResponsibleOrgUnit')


def grouped_by_three_sorter(brain):
    """Custom sort key in order to sort repository folder brains - only
    to be used with grouped_by_three formatter.
    """
    clientid_repository_separator = u' '

    value = brain.reference

    # 'AI-DEV 012.2' -->  'AI-DEV', '012.2'
    clientid, refnums_part = value.split(clientid_repository_separator, 1)
    parts = refnums_part.split('.')
    return (parts[0], map(int, parts[1:]))


class RepositoryReport(grok.View):
    """Produce a CSV report of the repository folders in the adapted
    repository root.
    """

    grok.name('repository-report')
    grok.context(IRepositoryRoot)
    grok.require('cmf.ManagePortal')

    def render(self):
        catalog = api.portal.get_tool('portal_catalog')

        brains = catalog(self._query())
        brains = self._sort(brains)

        header = [REFNUM_HEADER, TITLE_HEADER]
        if self._responsible_org_unit_enabled():
            header.append(ORG_UNIT_HEADER)

        rows = []
        for brain in brains:
            repofolder = brain.getObject()

            row = []
            row.append(self._get_reference_number(repofolder))
            row.append(self._get_title(repofolder))

            if self._responsible_org_unit_enabled():
                row.append(self._get_org_unit(repofolder))

            rows.append(row)

        csv = self._make_csv(rows, header)
        self.request.RESPONSE.setHeader('Content-Type', 'text/csv')
        return csv

    def _responsible_org_unit_enabled(self):
        types_tool = api.portal.get_tool('portal_types')
        fti = types_tool['opengever.repository.repositoryfolder']
        return RESPONSIBLE_ORG_BEHAVIOR in fti.behaviors

    @property
    def _refnum_format(self):
        fmt_name = api.portal.get_registry_record(
            'formatter', interface=IReferenceNumberSettings)
        return fmt_name

    def _assert_grouped_by_three(self):
        if not self._refnum_format == 'grouped_by_three':
            raise AssertionError(
                "This view only works with the GroupedByThree format, or a "
                "more recent opengever.core version, because otherwise "
                "sorting would be incorrect.")

    def _make_csv(self, rows, header=None):
        csv_file = StringIO()
        csv_writer = csv.writer(
            csv_file, delimiter=DELIMITER,
            quotechar=QUOTECHAR, quoting=csv.QUOTE_MINIMAL)

        if header is not None:
            csv_writer.writerow([value.encode(ENCODING) for value in header])

        for row in rows:
            csv_writer.writerow([value.encode(ENCODING) for value in row])

        return csv_file.getvalue()

    def _get_title(self, repofolder):
        if hasattr(repofolder, 'title_de'):
            title = repofolder.title_de
        else:
            title = repofolder.effective_title
        return title

    def _get_org_unit(self, repofolder):
        try:
            org_unit = IResponsibleOrgUnit(repofolder).responsible_org_unit
        except TypeError:
            org_unit = None

        if org_unit is None:
            org_unit = u''
        return org_unit

    def _get_reference_number(self, repofolder):
        refnum = IReferenceNumber(repofolder).get_repository_number()
        return refnum

    def _sort(self, brains):
        # XXX: Once og.core == 4.14.2 has been deployed everywhere, the
        # conditional below and the hardcoded grouped_by_three_sorter above
        # should be removed.

        formatter = queryAdapter(
            api.portal.get(), IReferenceNumberFormatter,
            name=self._refnum_format)
        try:
            sorted_brains = sorted(brains, key=formatter.sorter)
        except ValueError:
            # Old version of og.core - fall back to hardcoded / copied sorter
            self._assert_grouped_by_three()
            sorted_brains = sorted(brains, key=grouped_by_three_sorter)
        return sorted_brains

    def _query(self):
        interfaces = (
            'opengever.repository.repositoryfolder.IRepositoryFolderSchema',
        )

        return {'object_provides': interfaces,
                'path': '/'.join(self.context.getPhysicalPath())}
