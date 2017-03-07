"""
Usage:

bin/instance run mysql2postgres.py --old-dsn mysql://olduser:oldpw@host/olddb

Tested for: KGS Version 3.4.8
"""

from opengever.base.hooks import create_models
from opengever.base.model import create_session
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.setup import create_sql_tables
from sqlalchemy import create_engine
from sqlalchemy import DDL
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
import logging
import transaction


log = logging.getLogger('mysql2postgres')
log.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


def create_schemas(plone):
    log.info("Creating schemas in new PostgreSQL DB...")
    create_sql_tables()
    create_models()


def get_postgres_connection():
    new_session = create_session()
    new_engine = new_session.bind

    if not new_engine.driver == 'psycopg2':
        raise Exception(
            'Please switch GEVER OGDS connection to PostgreSQL first before '
            'running this script!')

    new_meta = MetaData(bind=new_engine, reflect=True)

    new_engine.execute('SELECT 1')
    log.info('Successful PostgreSQL connection: %r' % new_engine)
    return new_session, new_engine, new_meta


def get_mysql_connection(options):
    old_engine = create_engine(options.old_dsn)
    old_session = sessionmaker(bind=old_engine)()

    if not old_engine.driver == 'mysqldb':
        raise Exception(
            'Please point old database DSN to a mysql:// connection! '
            '(got %r instead)' % options.old_dsn)

    old_meta = MetaData()
    old_meta.reflect(bind=old_engine)

    old_engine.execute('SELECT 1')
    log.info('Successful MySQL connection: %r' % old_engine)
    return old_session, old_engine, old_meta


def disable_triggers(new_engine, new_table):
    log.info("Disabling triggers for PG table '%s'" % new_table.name)
    stmt = DDL('ALTER TABLE %(table)s DISABLE TRIGGER ALL',
               context={'table': new_table.name})
    new_engine.execute(stmt)


def enable_triggers(new_engine, new_table):
    log.info("Enabling triggers for PG table '%s'" % new_table.name)
    stmt = DDL('ALTER TABLE %(table)s ENABLE TRIGGER ALL',
               context={'table': new_table.name})
    new_engine.execute(stmt)


def cast_row_values(rows, new_table):
    """Read each row into a Python dict, and cast values to the appropriate
    type, as defined by the new column's `python_type`.
    """
    records = []
    for row in rows:
        record = row._asdict()
        for fieldname, value in record.items():
            new_column = new_table.columns[fieldname]

            if value is not None:
                # Cast values to new columns type. This takes care of
                # converting values like tinyint 1 (MySQL) to bool true (PG)
                new_type = new_column.type.python_type
                if not isinstance(value, new_type):
                    log.info("Casting '%s' %r to %r" % (
                        new_column.name, value, new_type))
                    record[fieldname] = new_type(value)

        records.append(record)
    return records


def migrate_data(plone, options):
    new_session, new_engine, new_meta = get_postgres_connection()
    old_session, old_engine, old_meta = get_mysql_connection(options)
    log.info('')

    # Iterate over reflected old tables, and migrate them one by one
    for old_tblname in old_meta.tables:
        old_table = old_meta.tables[old_tblname]
        new_table = new_meta.tables[old_tblname]

        log.info("Querying MySQL table '%s'" % old_tblname)
        rows = old_session.query(old_table)

        # Cast values to new column's Python type
        records = cast_row_values(rows, new_table)

        if records:
            disable_triggers(new_engine, new_table)
            log.info("Migrating %s records..." % len(records))
            new_engine.execute(new_table.insert(), records)
            enable_triggers(new_engine, new_table)
            log.info("Done writing to PostgreSQL table '%s'" % new_table)
        else:
            log.info("(Empty table, nothing to migrate)")
        log.info('')

    log.info("Migration done.")


def parse_options():
    parser = setup_option_parser()
    parser.add_option("--old-dsn", dest="old_dsn", help="Old DSN (MySQL)")
    (options, args) = parser.parse_args()
    if not options.old_dsn:
        parser.error('--old-dsn is required!')
    return options, args


def main():
    app = setup_app()
    options, args = parse_options()

    plone = setup_plone(app, options)

    # Validate DB connections before we do anything else
    log.info('Checking old connection (MySQL)')
    get_mysql_connection(options)
    log.info('Checking new connection (PostgreSQL)')
    get_postgres_connection()

    create_schemas(plone)
    migrate_data(plone, options)
    transaction.commit()


if __name__ == '__main__':
    main()
