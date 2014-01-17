import os
import re
import sys
import pickle
import argparse

from douban.sqlstore import store_from_config

SCHEMA_CACHE = 'schema_cache.pickle'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='sqlstore config')
    parser.add_argument('--without-drop-table', action='store_true')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--keep-auto-increment', action='store_true')
    parser.add_argument('--only-meaningful-changes',
                        action='store_true',
                        help='Do not treat as change if only AUTO_INCREMENT changes')
    args = parser.parse_args()

    if not args.config:
        print 'sqlstore config must be specified'
        return 1

    re_auto_increment = re.compile('\s+AUTO_INCREMENT=\d+')

    schema_cache = {}
    if args.only_meaningful_changes:
        try:
            schema_cache = pickle.load(open(SCHEMA_CACHE))
        except:
            pass

    store = store_from_config(args.config)
    for name, farm in store.farms.items():
        output_file = 'database-{}.sql'.format(name)
        tmp_output_file = '{}-tmp'.format(output_file)
        if args.verbose:
            print 'Dump schema in {} to {}...'.format(name, output_file)
        cursor = farm.get_cursor()
        cursor.execute('show tables')
        tables = sorted([r[0] for r in cursor.fetchall()])
        fail = False
        with open(tmp_output_file, 'w') as f:
            f.write('/*!40101 SET @saved_cs_client = @@character_set_client */;\n')
            f.write('/*!40101 SET character_set_client = utf8 */;\n\n')
            for table in tables:
                try:
                    if not args.without_drop_table:
                        f.write('DROP TABLE IF EXISTS `{}`;\n'.format(table))
                    cursor.execute('show create table `{}`'.format(table))
                    schema = cursor.fetchone()[-1]
                    if not args.keep_auto_increment:
                        schema = re_auto_increment.sub('', schema)
                    elif args.only_meaningful_changes:
                        _table = '{}.{}'.format(name, table)
                        _schema = schema_cache.get(_table)
                        if _schema and (re_auto_increment.sub('', _schema) == \
                           re_auto_increment.sub('', schema)):
                            # only AUTO_INCREMENT changes, definition does not
                            # change, use cached schema to keep AUTO_INCREMENT
                            schema = _schema
                        else:
                            schema_cache[_table] = schema
                    f.write('{};\n\n'.format(schema))
                except Exception, exc:
                    fail = True
                    msg = 'dump schema of "{}.{}" fail: {}'.format(name, table, exc)
                    print >>sys.stderr, msg
                    break
            f.write('/*!40101 SET character_set_client = @saved_cs_client */;\n')
        if not fail:
            os.rename(tmp_output_file, output_file)
        else:
            try:
                os.remove(tmp_output_file)
            except Exception, exc:
                print >>sys.stderr, 'remove tmp file "{}" fail: {}'.format(tmp_output_file, exc)

    if args.only_meaningful_changes:
        with open(SCHEMA_CACHE, 'w') as f:
            pickle.dump(schema_cache, f)
