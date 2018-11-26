from Zope2 import app as App
from opengever.maintenance.browser import warmup


def health_check(connection):
    ok = True
    app = App()
    try:
        dbchooser = app.Control_Panel.Database
        for dbname in dbchooser.getDatabaseNames():
            storage = dbchooser[dbname]._getDB()._storage
            is_connected = getattr(storage, 'is_connected', None)
            if is_connected is not None and not is_connected():
                ok = False
                connection.write(
                    "Error: Database '{}'' disconnected.\n".format(dbname))
    finally:
        app._p_jar.close()

    if warmup.warmup_in_progress:
        ok = False
        connection.write("Warmup in progress\n")

    if ok:
        connection.write('OK\n')
