from five import grok
from Products.CMFPlone.interfaces import IPloneSiteRoot
from sqlalchemy import MetaData

try:
    from opengever.base.model import create_session
except ImportError:
    # opengever.core < 4.2
    from opengever.ogds.base.utils import create_session


class ListTruncatedRowsView(grok.View):
    """This view lists all SQL rows that have a value that has the exact same
    length as the column's max length. These are rows that are likely to have
    had their values truncated.
    """

    grok.name('list-truncated-rows')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def display(self, msg):
        print msg
        self.result.append(msg)

    def render(self):
        self.result = []
        session = create_session()

        meta = MetaData()
        meta.reflect(bind=session.bind)

        for name, table in meta.tables.items():
            rows = session.execute(table.select())
            self.display("=" * 78)
            self.display("Checking table: {}".format(name))
            self.display("=" * 78)
            for row in rows:
                for value, column in zip(row, table.columns):
                    if hasattr(column.type, 'length'):
                        if value is None:
                            # NULL value
                            continue
                        if column.type.length is None:
                            # Infinite length
                            continue
                        if len(value) >= column.type.length:
                            self.display("COLUMN: {}".format(repr(column)))
                            self.display("VALUE: {}".format(value))
            self.display('')
        return '\n'.join(self.result)
