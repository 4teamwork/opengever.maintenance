import pkg_resources

try:
    pkg_resources.get_distribution('RelStorage')
except pkg_resources.DistributionNotFound:
    HAS_RELSTORAGE = False
else:
    HAS_RELSTORAGE = True
    from relstorage.storage import RelStorage
    from relstorage.adapters.stats import OracleStats


def get_object_count(db):
    """Returns the number of objects in the ZODB
    """

    count = db.objectCount()
    if count != 0:
        return count

    # Might be RelStorage with Oracle, where object count is approximate and
    # therefore has been disabled for the time being.
    if HAS_RELSTORAGE:
        if isinstance(db.storage, RelStorage):
            stats = db.storage._adapter.stats
            if isinstance(stats, OracleStats):
                return _get_object_count_oracle(stats.connmanager)

    return 0


def _get_object_count_oracle(connmanager):
    """Returns the number of objects in the database.

    See relstorage.adapters.stats.OracleStats @2df8f8df
    """

    conn, cursor = connmanager.open(
        connmanager.isolation_read_only)
    try:
        stmt = """
        SELECT NUM_ROWS
        FROM USER_TABLES
        WHERE TABLE_NAME = 'CURRENT_OBJECT'
        """
        cursor.execute(stmt)
        res = cursor.fetchone()[0]
        if res is None:
            res = 0
        else:
            res = int(res)
        return res
    finally:
        connmanager.close(conn, cursor)
