#!/usr/bin/env python
#coding=utf8

"""Utility script for generating sqlstore configs

sample settings file:

################# starts #####################
default_params = {
    'roles': ['m', 's', 'b', 'g', 'h'],
    'rw_user': {
        'user': 'rw_user',
        'passwd': 'password'
    },
    'ro_user': {
        'user': 'ro_user',
        'passwd': 'password'
    },
    'tables': [],
}

farms = {
    'luz': {
        'port': 3306,
        'dbs': ['luz_farm'],
        'online': True,
    },
}

configs = {
    'shire-online.erb': {
        'instances': ['luz_m'],
    }
}
################## ends ######################

"""

import imp
import sys
import pprint
import json
import argparse
from StringIO import StringIO

from douban.sqlstore import SqlFarm

verbose = False

class DuplicatedTable(Exception):
    def __init__(self, table, farms):
        self.table = table
        self.farms = farms

    def __str__(self):
        return 'There are duplicated tables in these farms: %s' % \
                ','.join(self.farms)

class FarmManager(object):
    def __init__(self, config):
        if isinstance(config, basestring):
            config = imp.load_source('sqlstore_settings', config)
        self._default_params = config.default_params
        self.farms = config.farms
        self.configs = config.configs

    def get_conf(self, instance):
        """Get MySQLdb compatible config

        instance: luz_m, orc_s etc.
        """

        farm, role = instance.rsplit('_', 1)
        _conf = self._default_params.copy()
        _conf.update(self.farms.get(farm, {}))
        _available_roles = self._default_params.get('roles', [])
        fallback_role = None
        if role not in _conf['roles']:
            # fallback to next higher priority role
            # b -> s -> m
            # if there is not "b" role in the farm, then fallback to "s" for
            # example
            fallback_roles = reversed(_available_roles[:_available_roles.index(role)])
            for _role in fallback_roles:
                if _role in _conf['roles']:
                    fallback_role = _role
                    break
            if not fallback_role:
                return {}
            else:
                role = fallback_role

        if role == 'm':
            user = _conf['rw_user']['user']
            passwd = _conf['rw_user']['passwd']
        else:
            user = _conf['ro_user']['user']
            passwd = _conf['ro_user']['passwd']

        host_prefix = _conf.get('host_prefix', None)
        host = '%s_%s' % (farm, role) if not host_prefix else '%s_%s' % (host_prefix, role)
        host = _conf['hostname'] if _conf.get('hostname') else host
        conf = {
            'host': host,
            'port': _conf['port'],
            'user': user,
            'passwd': passwd,
            'db': _conf['dbs'][0],
        }
        return conf

    def get_tables(self, farm):
        tables = self.farms.get(farm, {}).get('tables')
        if tables:
            return tables

        dbcnf = self.get_sqlstore_dbcnf('%s_m' % farm)
        farm = SqlFarm(dbcnf, connect_timeout=1)
        cursor = farm.get_cursor()
        cursor.execute('show tables')
        tables = [r[0] for r in cursor.fetchall()]
        return tables

    def get_sqlstore_dbcnf(self, instance):
        conf = self.get_conf(instance)
        return '%(host)s:%(port)d:%(db)s:%(user)s:%(passwd)s' % conf if conf else ''

    def gen_config(self, name, instances, extras={}, roles=['m', 's', 'b']):
        conf = {
            'farms': {},
            'migration': {},
            'options': {},
        }
        all_tables = {}
        all_instances = set()
        for index, instance in enumerate(instances):
            if instance in all_instances:
                if verbose:
                    print >>sys.stderr, 'duplicate instance:', instance
            all_instances.add(instance)

            try:
                # FIXME: roles override the roles param
                name, roles = instance.rsplit('_', 1)
            except ValueError:
                name = instance
                instance = '%s_m' % instance
            farm_name = '%s_farm' % name
            conf['farms'][farm_name] = {}

            role_names = {
                'm': 'master',
                's': 'slave',
                'b': 'backup',
            }
            for role in roles:
                instance = '%s_%s' % (name, role)
                dbcnf = self.get_sqlstore_dbcnf(instance)
                #TODO: len(roles) == 1 means non-algorithm configs
                if len(roles) == 1:
                    role_name = 'master'
                else:
                    role_name = role_names[role]
                conf['farms'][farm_name][role_name] = dbcnf

            tables = self.get_tables(name)
            if tables is None:
                return None
            for table in tables:
                if table in all_tables:
                    farms = [farm_name, all_tables[table]]
                    raise DuplicatedTable(table, farms)
                all_tables[table] = farm_name

            if index == 0:
                tables.append('*')
            conf['farms'][farm_name]['tables'] = tables

        if verbose:
            print >>sys.stderr, 'done!'

        conf.update(extras)
        return conf

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        default='/etc/sqlstore/settings.py')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    global verbose
    verbose = args.verbose

    try:
        config = imp.load_source('sqlstore_settings', args.config)
    except Exception, exc:
        print >>sys.stderr, 'Read config "%s" "fail: %s' % (args.config, exc)
        return 1

    fm = FarmManager(config)
    skipped = 0
    for output_filename, options in fm.configs.items():
        if verbose:
            print >>sys.stderr, 'Processing config "%s"...' % output_filename,
        try:
            farms_config = fm.gen_config(output_filename,
                                         options['instances'],
                                         extras=options.get('extras', {}))
        except Exception, exc:
            print >>sys.stderr, 'Skip generating config file "%s": %s'  % (output_filename, exc)
            skipped += 1
            continue
        _format = options.get('format', 'python')
        if not _format in ('python', 'json'):
            raise Exception('Invaid output format: %s' % _format)
        config = farms_config
        if _format == 'python':
            output = StringIO()
            pp = pprint.PrettyPrinter(indent=4, stream=output)
            pp.pprint(config)
            with open(output_filename, 'w') as cf:
                cf.write(output.getvalue())
        elif _format == 'json':
            json.dump(config, open(output_filename, 'w'), indent=4)
    return skipped

if __name__ == '__main__':
    sys.exit(main())
