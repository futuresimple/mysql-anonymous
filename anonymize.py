#!/usr/bin/env python
# This assumes an id on each field.
import logging


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
    import yaml
    import sys
    f = sys.argv[1] if len(sys.argv) > 1 else 'anonymize.yml'
    cfg = yaml.load(open(f))
    anonymize(cfg)
