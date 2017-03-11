"""
Migrates a GEVER SQL DB from MySQL to PostgreSQL.


Usage:

bin/instance run mysql2postgres.py --old-dsn mysql://olduser:oldpw@host/olddb


Tested for: KGS Version 3.4.8

Detailed usage instructions:

- Create Backups as necessary
- Shut down your MySQL-based GEVER deployment
- Make sure PostgreSQL is running, and create the new PG DB using `createdb`
- Include psycopg2 in your ${buildout:instance-eggs}
- Change the OGDS connection string (<db:engine url="..." />) from MySQL to
  the new PostgreSQL DB to be migrated into
- Run bin/buildout
- Start ZEO using bin/zeo start
- Run this migration script:
  bin/instance0 run mysql2postgres.py --old-dsn $OLD_MYSQL_DSN
- Remove opengever.mysqlconfig / MySQL-python from buildout
- Run bin/buildout
- Start the deployment up again
"""

from opengever.base.hooks import create_models
from opengever.base.model import create_session
from opengever.core.upgrade import TRACKING_TABLE_NAME
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.setup import create_sql_tables
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DDL
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy import TEXT
from sqlalchemy import VARCHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import INTEGER
import logging
import transaction


log = logging.getLogger('mysql2postgres')
log.setLevel(logging.INFO)
stream_handler = log.root.handlers[0]
stream_handler.setLevel(logging.INFO)


# The second group of sequences don't follow the default naming scheme of
# {table}_{column}_seq, which is why we have to hardcode a mapping

SEQUENCES_BY_COLUMN = {
    ('activities', 'id'): 'activities_id_seq',
    ('agendaitems', 'id'): 'agendaitems_id_seq',
    ('notification_defaults', 'id'): 'notification_defaults_id_seq',
    ('notifications', 'id'): 'notifications_id_seq',
    ('resources', 'id'): 'resources_id_seq',
    ('watchers', 'id'): 'watchers_id_seq',

    ('committees', 'id'): 'committee_id_seq',
    ('generateddocuments', 'id'): 'generateddocument_id_seq',
    ('meetings', 'id'): 'meeting_id_seq',
    ('members', 'id'): 'member_id_seq',
    ('memberships', 'id'): 'membership_id_seq',
    ('proposals', 'id'): 'proposal_id_seq',
    ('proposalhistory', 'id'): 'proposal_history_id_seq',
    ('submitteddocuments', 'id'): 'submitteddocument_id_seq',
    ('tasks', 'id'): 'task_id_seq',
}


def create_schemas(plone, new_meta):
    log.info("Creating schemas in new PostgreSQL DB...")
    create_sql_tables()
    create_models()
    # Reflect new metadata again after creating tables
    new_meta.reflect()


def create_tracking_table(new_session, new_engine, new_meta):
    log.info("Creating tracking table in new PostgreSQL DB...")
    tracking_table = Table(
        TRACKING_TABLE_NAME, new_meta,
        Column('profileid', String(50), primary_key=True),
        Column('upgradeid', BigInteger, nullable=False),
    )
    tracking_table.create()
    # Reflect new metadata again after creating tracking table
    new_meta.reflect()


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


def get_sequence_names(new_session):
    stmt = text("SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'")
    return [r for (r, ) in new_session.execute(stmt).fetchall()]


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
                    # Can store unicode directly to columns of type TEXT
                    if all([isinstance(value, unicode),
                           isinstance(new_column.type, TEXT)]):
                        record[fieldname] = value
                        continue
                    # For VARCHAR columns, unicode values need to be encoded
                    if all([isinstance(value, unicode),
                           isinstance(new_column.type, VARCHAR)]):
                        log.info("Casting '%s' %r to %r" % (
                            new_column.name, value, value.encode('utf-8')))
                        record[fieldname] = value.encode('utf-8')
                        continue

                    log.info("Casting '%s' %r to %r" % (
                        new_column.name, value, new_type))
                    record[fieldname] = new_type(value)

        records.append(record)
    return records


def restart_sequence(new_session, seq_name, new_col):
    assert isinstance(new_col.type, INTEGER)
    max_value = new_session.query(func.max(new_col)).scalar()
    if max_value is not None:
        log.info("Setting sequence %r to value %r" % (seq_name, max_value))
        stmt = text("SELECT pg_catalog.setval(:seq, :val, true)")
        new_session.execute(stmt, {'seq': seq_name, 'val': max_value})


def update_sequences(new_session, new_meta):
    """Set sequences to max value of their corresponding column
    """
    log.info('Updating sequences...')
    sequences_to_process = get_sequence_names(new_session)
    for new_table in new_meta.tables.values():
        for new_col in new_table.columns:
            seq_name = SEQUENCES_BY_COLUMN.get((new_table.name, new_col.name))
            if seq_name:
                # Column matches a sequence, update sequence as needed
                restart_sequence(new_session, seq_name, new_col)
                sequences_to_process.remove(seq_name)

    if sequences_to_process:
        log.error("Not all sequences could be processed!")
        log.error("Unmatched sequences: %r" % sequences_to_process)
        raise Exception("Unprocessed sequences")

    log.info("All sequences updated.")


def migrate_data(plone,
                 old_session, old_engine, old_meta,
                 new_session, new_engine, new_meta):
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

    # Set sequences to max value of their corresponding column
    update_sequences(new_session, new_meta)

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
    old_session, old_engine, old_meta = get_mysql_connection(options)
    log.info('Checking new connection (PostgreSQL)')
    new_session, new_engine, new_meta = get_postgres_connection()

    create_schemas(plone, new_meta)
    create_tracking_table(new_session, new_engine, new_meta)
    migrate_data(
        plone,
        old_session, old_engine, old_meta,
        new_session, new_engine, new_meta,
    )
    transaction.commit()


if __name__ == '__main__':
    main()
