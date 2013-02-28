#!/usr/bin/env python
# This assumes an id on each field.

import logging
import sys

import MySQLdb
import yaml

log = logging.getLogger('anonymize')


def get_truncates(config):
    database = config.get('database', {})
    truncates = database.get('truncate', [])
    sql = []
    for truncate in truncates:
        sql.append('TRUNCATE %s' % truncate)
    return sql


def get_deletes(config):
    database = config.get('database', {})
    tables = database.get('tables', [])
    sql = []
    for table, data in tables.iteritems():
        if 'delete' in data:
            fields = []
            for f, v in data['delete'].iteritems():
                fields.append('%s = "%s"' % (f, v))
            statement = 'DELETE FROM %s WHERE ' % table + ' AND '.join(fields)
            sql.append(statement)
    return sql

listify = lambda x: x if isinstance(x, list) else [x]

def get_updates(config):
    database = config.get('database', {})
    tables = database.get('tables', [])
    sql = []
    for table, data in tables.iteritems():
        updates = []
        for operation, details in data.iteritems():
            if operation == 'nullify':
                for field in listify(details):
                    updates.append("%s = NULL" % (field,))
            elif operation == 'random_int':
                for field in listify(details):
                    updates.append("%s = IF(%s, ROUND(RAND() * 2147483648), NULL)" % (field, field))
            elif operation == 'random_ip':
                for field in listify(details):
                    updates.append("%s = IF(%s, INET_NTOA(RAND() * 4294967295), NULL)" % (field, field))
            elif operation == 'random_email':
                for field in listify(details):
                    updates.append("%s = IF(%s, CONCAT(id, '@mozilla.com'), NULL)" % (field, field))
            elif operation == 'random_username':
                for field in listify(details):
                    updates.append("%s = IF(%s, CONCAT('_user_', id), NULL)" % (field, field))
            elif operation == 'random_date':
                    updates.append("%s = IF(%s, FROM_UNIXTIME(UNIX_TIMESTAMP(NOW()) - RAND() * 3600 * 24 * 365), NULL)" % (field, field))
            elif operation == 'random_md5':
                    updates.append("%s = IF(%s, MD5(RAND()), NULL)" % (field, field))
            elif operations == 'leave_as_is':
                continue
            elif operations == 'random_url':
                    updates.append("%s = IF(%s, CONCAT('http://example.com/action?id=', MD5(RAND())), NULL)" % (field, field))
            elif operation == 'delete':
                continue
            else:
                log.warning('Unknown operation.')
        if updates:
            sql.append('UPDATE %s SET %s' % (table, ', '.join(updates)))
    return sql


def check_configuration(config):
    connection_info = config['connection']
    connection = MySQLdb.connect(
        host=connection_info.get('host', 'localhost'),
        user=connection_info['username'],
        passwd=connection_info['password'],
        db=connection_info['database'],
    )

    maintenance_connection = MySQLdb.connect(
        host=connection_info.get('host', 'localhost'),
        user=connection_info['username'],
        passwd=connection_info['password'],
        db="information_schema",
    )

    database = config.get('database', {})
    tables = database.get('tables', [])

    for table in tables:
        configured_columns = set()
        for operations in tables[table].values():
            configured_columns.update(operations)

        maintenance = maintenance_connection.cursor()
        maintenance.execute('''
            SELECT COLUMN_NAME 
            FROM COLUMNS 
            WHERE 
                SCHEMA_NAME = '%(schema)s' AND 
                TABLE_NAME = '%(table)s'
        ''' % {
            'schema': config['database'],
            'table': table
        })

        existing_columns = maintenance.fetchall()
        missed_columns = existing_columns - configured_columns
        extra_columns = configured_columns - existing_columns

        if missed_columns:
            logger.error("All columns must be specified in configuration.")
            logger.error("Missing columns are: %s" % (missed_collumns, ))
            sys.exit(1)

        if extra_columns:
            logger.error("Your configuration names columns that don't exists in the database.")
            logger.error("These columns are: %s" % (extra_columns, ))
            sys.exit(1)


def anonymize(config):
    sql = []
    sql.extend(get_truncates(config))
    sql.extend(get_deletes(config))
    sql.extend(get_updates(config))
    print 'SET FOREIGN_KEY_CHECKS=0;'
    for stmt in sql:
        print stmt + ';'
    print 'SET FOREIGN_KEY_CHECKS=1;'


if __name__ == '__main__':
    db_file_names = sys.argv[1:]
    if not db_file_names:
        logger.error("Usage: %s config_file [config_file ...]", sys.argv[0])

    for db_file_name in db_file_names:
        logger.info("Processing %s", db_file_name)
        with open(db_file_name) as db_file:
            cfg = yaml.loads(db_file)

        check_config(fg)
        anonymize(cfg)

