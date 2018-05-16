from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import TEXT
from sqlalchemy import Table
from sqlalchemy import VARCHAR
from sqlalchemy import create_engine
import argparse


SEQUENCES = {
    'activities_id_seq': ('activities', 'id'),
    'adresses_id_seq': ('addresses', 'id'),
    'agendaitems_id_seq': ('agendaitems', 'id'),
    'archived_address_id_seq': ('archived_addresses', 'id'),
    'archived_contact_id_seq': ('archived_contacts', 'id'),
    'archived_mail_address_id_seq': ('archived_mail_addresses', 'id'),
    'archived_phonenumber_id_seq': ('archived_phonenumbers', 'id'),
    'archived_url_id_seq': ('archived_urls', 'id'),
    'committee_id_seq': ('committees', 'id'),
    'contacts_id_seq': ('contacts', 'id'),
    'excerpts_id_seq': ('excerpts', 'id'),
    'generateddocument_id_seq': ('generateddocuments', 'id'),
    'locks_id_seq': ('locks', 'id'),
    'mail_adresses_id_seq': ('mail_addresses', 'id'),
    'meeting_id_seq': ('meetings', 'id'),
    'member_id_seq': ('members', 'id'),
    'membership_id_seq': ('memberships', 'id'),
    'notification_defaults_id_seq': ('notification_defaults', 'id'),
    'notifications_id_seq': ('notifications', 'id'),
    'org_roles_id_seq': ('org_roles', 'id'),
    'participation_roles_id_seq': ('participation_roles', 'id'),
    'participations_id_seq': ('participations', 'id'),
    'periods_id_seq': ('periods', 'id'),
    'phonenumber_id_seq': ('phonenumbers', 'id'),
    'proposal_id_seq': ('proposals', 'id'),
    'resources_id_seq': ('resources', 'id'),
    'submitteddocument_id_seq': ('submitteddocuments', 'id'),
    'task_id_seq': ('tasks', 'id'),
    'teams_id_seq': ('teams', 'id'),
    'urls_id_seq': ('urls', 'id'),
    'watchers_id_seq': ('watchers', 'id'),
}


def create_schema(engine):
    import opengever.activity.model
    import opengever.contact.models
    import opengever.globalindex.model
    import opengever.locking.model
    import opengever.meeting.model
    from opengever.base.model import Base
    Base.metadata.create_all(engine)

    import opengever.ogds.models.admin_unit
    import opengever.ogds.models.group
    import opengever.ogds.models.org_unit
    import opengever.ogds.models.team
    import opengever.ogds.models.user
    from opengever.ogds.models import BASE
    BASE.metadata.create_all(engine)

    from ftw.dictstorage.sql import DictStorageModel
    DictStorageModel.metadata.create_all(engine)


def create_tracking_table(engine):
    meta = MetaData()
    meta.reflect(bind=engine)
    if 'opengever_upgrade_version' in meta.tables:
        return
    tracking_table = Table(
        'opengever_upgrade_version',
        meta,
        Column('profileid', String(50), primary_key=True),
        Column('upgradeid', BigInteger, primary_key=True),
    )
    tracking_table.create(bind=engine)


def disable_triggers(engine, table):
    stmt = DDL('ALTER TABLE %(table)s DISABLE TRIGGER ALL',
               context={'table': table})
    engine.execute(stmt)


def enable_triggers(engine, table):
    stmt = DDL('ALTER TABLE %(table)s ENABLE TRIGGER ALL',
               context={'table': table})
    engine.execute(stmt)


def cast_row_values(rows, table):
    """Read each row into a Python dict, and cast values to the appropriate
    type, as defined by the column's `python_type`.
    """
    records = []
    for row in rows:
        record = {}
        for fieldname, value in row.items():
            new_column = table.columns[fieldname]

            record[fieldname] = value
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
                        record[fieldname] = value.encode('utf-8')
                        continue

                    record[fieldname] = new_type(value)

        records.append(record)
    return records


def migrate_data(src_engine, dst_engine):
    src_meta = MetaData()
    src_meta.reflect(bind=src_engine)
    dst_meta = MetaData()
    dst_meta.reflect(bind=dst_engine)

    for table_name in src_meta.tables:
        if table_name not in dst_meta.tables:
            print "!!!Table %s missing." % table_name
            continue
        dst_table = dst_meta.tables[table_name]
        res, = dst_engine.execute(dst_table.count()).first()
        if res > 0:
            print "!!!Destination table %s not empty, skipping..." % table_name
            continue
        src_table = src_meta.tables[table_name]
        res = src_engine.execute(src_table.select())
        records = cast_row_values(res, dst_table)
        if records:
            print "Migrating table %s..." % table_name
            disable_triggers(dst_engine, table_name)
            dst_engine.execute(dst_table.insert(), records)
            enable_triggers(dst_engine, table_name)
        else:
            print "Source table %s is empty." % table_name


def restart_sequences(engine):
    for seq, (table, col) in SEQUENCES.items():
        res = engine.execute('SELECT MAX(%s) FROM %s' % (col, table))
        max_value, = res.first()
        if max_value:
            res = engine.execute("SELECT setval('%s', %s, true);" % (seq, max_value))
            last_value, = res.first()
            if last_value != max_value:
                print "!!!Restarting sequence %s failed." % seq
            else:
                print "Restarted sequence %s at %s." % (seq, max_value)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('src_dsn', help='Source DSN')
    parser.add_argument('dst_dsn', help='Destination DSN')
    options = parser.parse_args()

    src_engine = create_engine(options.src_dsn)
    if src_engine.driver != 'cx_oracle':
        print "Source Database must be Oracle"
        sys.exit(1)

    dst_engine = create_engine(options.dst_dsn)
    if dst_engine.driver != 'psycopg2':
        print "Destination Database must be PostgreSQL"
        sys.exit(1)

    create_schema(dst_engine)
    create_tracking_table(dst_engine)
    migrate_data(src_engine, dst_engine)
    restart_sequences(dst_engine)


if __name__ == '__main__':
    main()

