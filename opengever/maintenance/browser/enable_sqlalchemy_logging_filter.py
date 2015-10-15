from five import grok
from opengever.ogds.base.utils import create_session
from Products.CMFPlone.interfaces import IPloneSiteRoot
from threading import RLock
import logging


# Module globals to track whether the next bound SQL parameters should be
# logged or not, and the according lock to ensure thread safety
_log_next_param = False
_log_next_param_lock = RLock()


class TaskUpdateFilter(logging.Filter):
    """Filters records so that only UPDATE statements for the 'task' table
    and the corresponding parameters are logged
    """

    @staticmethod
    def filter(record):
        global _log_next_param

        if _log_next_param and TaskUpdateFilter.is_param_entry(record):
            with _log_next_param_lock:
                _log_next_param = False
            return True

        if 'UPDATE tasks' in record.msg:
            with _log_next_param_lock:
                _log_next_param = True
            return True

        return False

    @staticmethod
    def is_param_entry(record):
        return record.args != () and record.msg == '%r'


class EnableSQLAlchemyLoggingFilter(grok.View):
    """Registers a filter for the sqlalchemy.engine.base.Engine logger that
    only logs records related to task syncing.
    """

    grok.name('enable-sqlalchemy-logging-filter')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def render(self):
        session = create_session()
        engine = session.bind
        sqlalchemy_logger = engine.logger.logger

        if sqlalchemy_logger.filters != []:
            return "Filter already enabled."

        sqlalchemy_logger.addFilter(TaskUpdateFilter())
        return "Enabled filter."
