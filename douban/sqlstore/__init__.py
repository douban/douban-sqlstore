#!/usr/bin/env python
# encoding: utf-8

'''SqlStore library for douban'''

from contextlib import contextmanager
from operator import itemgetter
from warnings import warn, catch_warnings, formatwarning
from hashlib import md5
import collections
import os
import pwd
import random
import re
import socket
import string
import sys
import syslog
import threading
import time
import traceback

try:
    import cPickle as pickle
except ImportError:
    import pickle

import MySQLdb
from MySQLdb.constants.CR import COMMANDS_OUT_OF_SYNC, SERVER_GONE_ERROR

try:
    from raven import Client as RavenClient
    from raven.utils.stacks import get_stack_info, iter_stack_frames
except ImportError:
    RavenClient = None

import douban.utils.config
from douban.utils import hashdict
from douban.utils.imloaded import imloaded
from douban.utils.slog import log

from .dbconfig import DBConfig
from .table_finder import find_tables

imloaded('douban.sqlstore')

syslog.openlog('sqlstore')

host = socket.gethostname()
start_time = time.ctime()

try:
    CMDLINE = ' '.join(sys.argv)
except Exception:
    CMDLINE = ''

slog = lambda message: log('sqlstore', '"%s" %s' % (CMDLINE, message))

try:
    USER = pwd.getpwuid(os.geteuid()).pw_name
except Exception, exc:
    slog('Count not get effective user name: %s' % exc)
    USER = 'unknown'


class QueryDisabledException(Exception):

    def __init__(self, sql, recover_timestamp):
        self.sql = sql
        self.recover_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                          time.localtime(recover_timestamp))

    def __str__(self):
        msg = ('Query is temporarily disabled due to performance issue'
               '(will be recoverd after %s): %s')
        return msg % (self.recover_time, self.sql)


class InvalidMySQLDataException(Exception):

    def __init__(self, message, sql, args):
        self.message = message
        self.sql = sql
        self._args = args

    def __str__(self):
        return '%s: SQL:%s args:%s' % (self.message, self.sql, self._args)


class PleaseIgnoreThisMySQLException(MySQLdb.OperationalError):
    sentry_dsn = False

    def __str__(self):
        return ('This exception is only meaningful to DBA, '
                'pleae ignore: %s') % (self.args,)


class LogCursor(object):

    '''记录所有执行的SQL'''

    def __init__(self, cursor):
        self.cursor = cursor
        self.log = []

    def execute(self, *a, **kw):
        '''提供与MySQLdb.Cursor相同的执行SQL接口'''

        stack = traceback.extract_stack(limit=6)
        time_begin = time.time()
        try:
            retval = self.cursor.execute(*a, **kw)
            timecost = time.time() - time_begin
            self.log.append((a, kw, timecost, stack[:-1]))
        except Exception:
            stack = traceback.extract_stack(limit=5)
            self.log.append((a, kw, 0, stack))
            raise
        return retval

    def __iter__(self):
        return iter(self.cursor)

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)


class WarningCursor(object):

    '''警告已经废弃的store调用接口'''

    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, attr):
        warn(('store.farm and store.farmr are deprecated interface, please '
              'use store.get_cursor() instead'),
             DeprecationWarning,
             stacklevel=2)
        return getattr(self.cursor, attr)


class SqlFarm(object):

    '''单个数据库的访问接口'''

    isolation_levels = {
        'READ-UNCOMMITTED': 1,
        'READ-COMMITTED': 2,
        'REPEATABLE-READ': 3,
        'SERIALIZABLE': 4,
    }

    def __init__(self, conf=None, delete_without_where=False, store=None,
                 name='', **kwargs):
        self.dbcnf = parse_config_string(conf)
        self.dbcnf.update(kwargs)
        self.host = self.dbcnf.get('host', '')
        self.name = name or '%s_farm' % self.host.split('_')[0]
        self.delete_without_where = delete_without_where
        self.cursor = None
        self.expire_time = None
        self.set_expire_time()
        self.store = store or SqlStore(db_config={})
        self.tx_isolation = ''

    def __str__(self):
        return '<SqlFarm object id:%s farm:%s host:%s>' % (id(self),
                                                           self.name,
                                                           self.host)

    __repr__ = __str__

    def connect(self, host, user, passwd, db, **kwargs):
        '''提供与MySQLdb.Cursor相同的数据库连接接口'''

        conn_params = dict(host=host, user=user, db=db,
                           init_command='set names utf8', **kwargs)
        if passwd:
            conn_params['passwd'] = passwd

        try:
            if not getattr(MySQLdb, 'origin_connect', None):
                conn = MySQLdb.connect(**conn_params)
            else:
                conn = MySQLdb.origin_connect(**conn_params)
        except Exception, exc:
            self.store.send_exception_to_onimaru(exc, self)
            raise

        cursor = conn.cursor()
        cursor.execute('set sort_buffer_size=2000000')
        if self.dbcnf.get('disable_mysql_query_cache'):
            cursor.execute('set session query_cache_type = OFF')
        cursor.execute('select @@tx_isolation')
        r = cursor.fetchone()
        self.tx_isolation = r and r[0] or ''
        return LuzCursor(cursor, self)

    def close(self):
        '''关闭数据库连接'''

        if self.cursor:
            self.cursor.connection.close()
            self.cursor = None

    def is_expired(self):
        '''cursor是否已过期'''

        return self.expire_time < time.time()

    def set_expire_time(self):
        '''设置cursor过期时间'''

        expire_ts = self.dbcnf.get('connection_expire_seconds')
        self.expire_time = time.time() + (expire_ts or 3600)

    # TODO 修改所有调用ro参数的代码，删除已经废弃的ro参数
    def get_cursor(self, ro=False):
        '''取得执行SQL的cursor'''

        if self.cursor is None or self.is_expired():
            self.cursor = self.connect(**self.dbcnf)
            self.set_expire_time()

        return self.cursor

    def start_log(self):
        '''开始保存SQL执行记录'''

        if self.cursor is None:
            self.cursor = self.connect(**self.dbcnf)
        if not isinstance(self.cursor, LogCursor):
            self.cursor = LogCursor(self.cursor)

    def stop_log(self):
        '''停止保存SQL执行记录'''

        if isinstance(self.cursor, LogCursor):
            self.cursor = self.cursor.cursor

    def get_log(self, name, log_format='text', with_traceback=False):
        '''获取已经保存的SQL执行记录'''

        def sql_log(name, logs, with_traceback):
            if not logs:
                return ''

            logs = sorted(logs, key=itemgetter(2), reverse=True)

            _logs = []
            _logs.append('%s: %d SQL statements (%s seconds):\n' %
                         (name, len(logs), sum(x[2] for x in logs)))

            if with_traceback:
                _logs.extend(['%8.6fsec %s\n%s\n' %
                              (timecost, a,
                               ''.join(traceback.format_list((stack))))
                              for a, _, timecost, stack in logs])
            else:
                _logs.extend(['%8.6fsec %s\n' % (timecost, a)
                              for a, _, timecost, stack in logs])
            return ''.join(_logs) + '\n'

        if not isinstance(self.cursor, LogCursor):
            return log_format == 'dict' and {} or ''

        if log_format == 'dict':
            return {name: self.cursor.log}
        else:
            return sql_log(name, self.cursor.log, with_traceback)

    def is_testing(self):
        '''是否连接的测试数据库：数据库名称以test开头'''

        db_name = self.dbcnf['db']
        return db_name.startswith('test')

    def refresh(self):
        """When REPEATABLE-READ or SERIALIZABLE transaction isolation level
        is used, a new transaction should be started by issuing 'commit' or
        'rollback' to get a fresher snapshot.
        """

        if not self.cursor:
            return False

        if self.isolation_levels.get(self.tx_isolation, 100) > \
                self.isolation_levels['READ-COMMITTED']:
            try:
                cursor = self.get_cursor()
                cursor.connection.rollback()
                return True
            except MySQLdb.OperationalError, exc:
                self.store.send_exception_to_onimaru(exc, self)

                if 2000 <= exc.args[0] < 3000:
                    self.cursor = None
        return False


def parse_config_string(config_str):
    dummy = config_str.split(':')
    if len(dummy) == 4:
        host, db, user, passwd = dummy
        port = 3306
    elif len(dummy) == 5:
        host, port, db, user, passwd = dummy
    else:
        raise ValueError(config_str)
    return dict(host=host, port=int(port), db=db, user=user, passwd=passwd)

SQL_PATTERNS = {
    'select': re.compile(r'select\s.*?\sfrom\s+`?(?P<table>\w+)`?',
                         re.I | re.S),
    'insert': re.compile(r'insert\s+(ignore\s+)?(into\s+)?`?(?P<table>\w+)`?',
                         re.I),
    'update': re.compile(r'update\s+(ignore\s+)?`?(?P<table>\w+)`?\s+set',
                         re.I),
    'replace': re.compile(r'replace\s+(into\s+)?`?(?P<table>\w+)`?', re.I),
    'delete': re.compile(r'delete\s+from\s+`?(?P<table>\w+)`?', re.I),
}

if not os.environ.get('SQLSTORE_WAYLIFE'):
    execute_waylifer = None
else:
    try:
        from waylife import Waylifer, WAY_SQLSTORE_ARGS_LITERAL
    except ImportError:
        execute_waylifer = None
    else:
        execute_waylifer = Waylifer(flag=WAY_SQLSTORE_ARGS_LITERAL)


class SqlStore(object):

    def __init__(self, host='', user='', password='', db='luz_farm',
                 db_config=None, tables_map=None, created_via='UNKNOWN_APP',
                 db_config_name=None, **kwargs):
        self._kwargs = kwargs
        if not SqlStore.is_safe(created_via):
            warn('SqlStore should be created via DAE API in async mode '
                 '(created via: %s' % created_via)

        self.initialized = False
        self.config_lock = threading.Lock()

        self.db_config = db_config
        self.db_config_name = db_config_name
        self.farms = {}
        self.tables = {}
        self.tables_map = tables_map or {}
        self.disabled_queries = {}
        self.disabled_queries_with_args = {}

        # Statsd
        self.statsd = None
        self.statsd_sample_rate = 1

        # ConfigRceciver info
        self.cfgreloader = None
        self.cfgreloader_config_node = None
        self.cfgreloader_blacklist_node = None

        # Logging and migration info
        self.logging = False
        self.show_warnings = False
        self.treat_warning_as_error = False
        self.treat_warning_as_error_sampling_rate = 0
        # for transaction
        self.in_transaction = False
        self.modified_tables = set()
        self.modified_cursors = set()
        self.executed_queries = set()

        if self.db_config_name:
            self._init_db_config_from_file()
        elif db_config is not None:
            self.parse_config(db_config)
        else:
            warn(('SqlStore now has new interface, please use db_config as '
                  'paramater to create SqlStore object'), DeprecationWarning)
            farm = SqlFarm(':'.join([host, db, user, password]), name='',
                           store=self)
            self.farms[db] = farm
            self.tables['*'] = farm

    def _init_db_config_from_file(self):
        self.db_config_name = check_override(self.db_config_name)
        self.db_config = douban.utils.config.read_config(self.db_config_name,
                                                         'sqlstore')
        self.parse_config(self.db_config)

    def __getstate__(self):
        d = self.__dict__.copy()
        d['cfgreloader'] = None
        # clear properties related to transaction
        d['in_transaction'] = False
        d['modified_tables'] = set()
        d['modified_cursors'] = set()
        d['executed_queries'] = set()
        d.pop('config_lock', None)
        return d

    def __setstate__(self, d):
        # if the object passed down to other dpark member
        d['config_lock'] = threading.Lock()
        self.__dict__.update(d)
        if self.db_config_name:
            # reinitialize the config from local file system when unpickle
            self._init_db_config_from_file()
        elif self.db_config is not None:
            # TODO: should reinitialize cfgreloader
            self.parse_config(self.db_config)

    def __str__(self):
        return '<SqlStore object id:%s with %s tables>' % (id(self),
                                                           len(self.tables))
    __repr__ = __str__

    @staticmethod
    def is_safe(created_via):
        created_via_dae_api = created_via == 'DAE_API'
        dae_async_mode = os.environ.get('DAE_WORKER', None) == 'async'
        if dae_async_mode and not created_via_dae_api:
            return False
        return True

    def parse_config(self, db_config):
        ''' db_config must be a dict
        '''

        self.config_lock.acquire()

        if self.initialized and db_config == self.db_config:
            self.config_lock.release()
            return

        sentry_dsn = db_config.get('sentry_dsn')
        if sentry_dsn and RavenClient:
            self.raven_client = RavenClient(sentry_dsn)
        else:
            self.raven_client = None

        _self_farms = {}
        _self_tables = {}
        _farms = db_config.get('farms', {})
        for name, farm_config in _farms.items():
            new_dbcnf = parse_config_string(farm_config['master'])
            farm = self.get_farm(name, no_default=True)
            if not farm or farm.dbcnf != new_dbcnf:
                farm = SqlFarm(farm_config['master'],
                               store=self,
                               name=name,
                               **self._kwargs)
            _self_farms[name] = farm
            for table in farm_config['tables']:
                _self_tables[table] = farm
        if db_config and '*' not in _self_tables:
            raise MySQLdb.DatabaseError('No default farm specified')
        self.farms = _self_farms
        self.tables = _self_tables

        # initialize statsd client
        if db_config.get('statsd', {}).get('config'):
            try:
                import statsdclient
            except ImportError:
                self.statsd = None
                print >> sys.stderr, 'No statsdclient installed'
            else:
                statsd_config = db_config['statsd']
                config = statsd_config['config']
                sample_rate = statsd_config.get('sample_rate', 1)
                try:
                    self.statsd = statsdclient.statsd_from_config(config)
                    self.statsd_sample_rate = float(sample_rate)
                except Exception, ex:
                    self.statsd = None
                    print >> sys.stderr, 'initialize statsd fail:', ex

        options = db_config.get('options', {})
        self.logging = options.get('logging', False)
        if os.getenv('DOUBAN_CORELIB_SQLSTORE_LOGGING'):
            self.logging = True
        self.show_warnings = options.get('show_warnings', False)
        if os.getenv('DOUBAN_CORELIB_SQLSTORE_SHOW_WARNINGS'):
            self.show_warnings = True
        self.treat_warning_as_error = \
            options.get('treat_warning_as_error', False)
        if os.getenv('DOUBAN_CORELIB_SQLSTORE_TREAT_WARNING_AS_ERROR'):
            self.treat_warning_as_error = True
        self.treat_warning_as_error_sampling_rate = \
            options.get('treat_warning_as_error_sampling_rate', 0)

        self.cfgreloader_config_node = \
            db_config.get('cfgreloader', {}).get('config_node', None)
        self.cfgreloader_blacklist_node = \
            db_config.get('cfgreloader', {}).get('blacklist_node', None)

        if self.cfgreloader_config_node:
            try:
                if not self.cfgreloader:
                    from douban.cfgreloader import cfgreloader
                    self.cfgreloader = cfgreloader
            except Exception, exc:
                self.send_exception_to_onimaru(exc, self)
                warn('Failed creating cfgrealoder: %s' % exc)

            if self.cfgreloader:
                try:
                    self.cfgreloader.register(self.cfgreloader_config_node,
                                              self.receive_conf,
                                              identity=self)
                except Exception, exc:
                    self.send_exception_to_onimaru(exc, self)
                    msg = 'Failed registering callback %r for node %s: %s'
                    msg = msg % (self.receive_conf,
                                 self.cfgreloader_config_node,
                                 exc)
                    print >> sys.stderr, msg

        if self.cfgreloader_blacklist_node:
            try:
                if not self.cfgreloader:
                    from douban.cfgreloader import cfgreloader
                    self.cfgreloader = cfgreloader
            except Exception, exc:
                self.send_exception_to_onimaru(exc, self)
                warn('Failed creating cfgreloader: %s' % exc)

            if self.cfgreloader:
                try:
                    self.cfgreloader.register(self.cfgreloader_blacklist_node,
                                              self.receive_query_blacklist,
                                              identity=self)
                except Exception, exc:
                    self.send_exception_to_onimaru(exc, self)
                    msg = 'Failed registering callback %r for node %s: %s'
                    msg = msg % (self.receive_query_blacklist,
                                 self.cfgreloader_blacklist_node,
                                 exc)
                    print >> sys.stderr, msg

        self.db_config = db_config
        self.initialized = True
        self.config_lock.release()

    def receive_conf(self, data, version=None, mtime=None):
        ''' callback function for cfgmanager to receive lastest sqlstore config
        '''

        try:
            db_config = eval(data)
            self.parse_config(db_config)
            return True
        except Exception, exc:
            self.send_exception_to_onimaru(exc, self)
            msg = 'in douban.sqlstore.receive_conf, '
            msg += 'Failed parsing config received from cfgreloader: %s'
            msg = msg % exc
            msg += ''.join(traceback.format_stack())
            return (False, msg)

    def receive_query_blacklist(self, data, version=None, mtime=None):
        ''' callback function for cfgmanager to receive lastest blacklist
        of queries
        '''

        try:
            now = time.time()
            _disabled_queries = {}
            _disabled_queries_with_args = {}

            # Clean up existing items, remove expired ones
            for checksum, expire_time in self.disabled_queries.items():
                if expire_time > now:
                    _disabled_queries[checksum] = expire_time
            for checksum, expire_time in \
                    self.disabled_queries_with_args.items():
                if expire_time > now:
                    _disabled_queries_with_args[checksum] = expire_time

            # Update blacklists
            blacklists = pickle.loads(data)
            for checksum, expire_time in blacklists.get('partial', {}).items():
                if expire_time > now:
                    _disabled_queries[checksum] = expire_time
                else:
                    _disabled_queries.pop(checksum, None)
            for checksum, expire_time in blacklists.get('full', {}).items():
                if expire_time > now:
                    _disabled_queries_with_args[checksum] = expire_time
                else:
                    _disabled_queries_with_args.pop(checksum, None)

            self.disabled_queries = _disabled_queries
            self.disabled_queries_with_args = _disabled_queries_with_args

            return True
        except Exception, exc:
            self.send_exception_to_onimaru(exc, self)
            msg = ('in douban.sqlstore.receive_query_blacklist, '
                   'Failed parsing query blacklist received from '
                   'cfgmanager: %s')
            msg = msg % exc
            msg += ''.join(traceback.format_stack())
            return (False, msg)

    def close(self):
        for farm in self.farms.values():
            farm.close()

    def get_farm(self, farm_name, no_default=False):
        farm = self.farms.get(farm_name)

        if farm is None and not no_default:
            warn('Farm %r is not configured, use default farm' % farm_name,
                 stacklevel=3)
            return self.tables['*']
        else:
            return farm

    def get_farm_by_table(self, table):
        farm = self.tables.get(table)
        if farm is None:
            farm_name = self.tables_map.get(table)
            if farm_name:
                farm = self.get_farm(farm_name)
        if farm is None:
            return self.tables['*']
        else:
            return farm

    def _flush_get_cursor_log(self, cursor):
        if len(cursor.queries) > 1:
            syslog.syslog('get_cursor: %s' % '|'.join(cursor.queries))
        cursor.queries = []

    def _flush_accessed_tables(self, cursor):
        cursor.tables = set()

    # TODO 修改所有调用ro参数的代码，删除已经废弃的ro参数
    def get_cursor(self, ro=False, farm=None, table='*', tables=None):
        """get a cursor according to table or tables.

        Note:

          * If `tables` is given, `table` is ignored.
          * If `farm` is given, `table` and `tables` are both ignored.
        """

        not_specifying_table = False
        if farm:
            farm = self.get_farm(farm)
        elif tables:
            farms = set(self.get_farm_by_table(table) for table in tables)
            if len(farms) > 1:
                raise MySQLdb.DatabaseError('%s are not in the same farm' %
                                            tables)
            farm = farms.pop()
        else:
            farm = self.get_farm_by_table(table)
            if table == '*':
                not_specifying_table = True
        cursor = farm.get_cursor()
        self._flush_get_cursor_log(cursor)
        self._flush_accessed_tables(cursor)
        if not_specifying_table:
            _file, _lineno, _module, _line = \
                traceback.extract_stack(limit=2)[0]
            _query = '%s|%d|%s' % (_file, _lineno, _line)
            cursor.queries.append(_query)
        return cursor

    def parse_execute_sql(self, sql):
        sql = sql.lstrip()
        cmd = sql.split(' ', 1)[0].lower()
        if cmd not in SQL_PATTERNS:
            raise Exception('SQL command %s is not yet supported' % cmd)
        match = SQL_PATTERNS[cmd].match(sql)
        if not match:
            raise Exception(sql)

        tables = [t for t in find_tables(sql) if t in self.tables]
        table = match.group('table')

        if table in tables:
            tables.remove(table)

        return cmd, [table] + list(tables)

    def transaction_begin(self):
        if self.in_transaction or self.modified_cursors:
            raise Exception('another transaction has not been finished: %s' %
                            ','.join(self.modified_tables))
        self.in_transaction = True
        self.modified_tables = set()
        self.modified_cursors = set()
        self.executed_queries = set()

    def transaction_end(self):
        if self.in_transaction:
            if len(self.modified_cursors) > 1:
                message = 'WRITE_TABLES_IN_DIFFERENT_FARM %s' % \
                    ','.join(self.modified_tables)
                warn(message)
                if self.logging:
                    slog(message)
            self.in_transaction = False

    def execute(self, sql, args=None):
        cmd, tables = self.parse_execute_sql(sql)
        if self.logging and len(tables) > 1:
            message = 'MULTIPLE_TABLES_WITH_SINGLE_CURSOR %s %s' % \
                (sql, ','.join(tables))
            slog(message)

        cursor = self.get_cursor(table=tables[0])
        self._flush_get_cursor_log(cursor)
        ret = cursor.execute(sql, args, called_from_store=True)
        if cmd == 'select':
            return cursor.fetchall()
        else:
            self.modified_cursors.add(cursor)
            self.modified_tables.update(tables)
            self.executed_queries.add(sql)
            if cmd == 'insert' and cursor.lastrowid:
                ret = cursor.lastrowid
            return ret

    def commit(self):
        self.transaction_end()
        first_error = None
        try:
            for cursor in self.modified_cursors:
                try:
                    cursor.connection.commit()
                except Exception, e:
                    if not first_error:
                        first_error = e
                    try:
                        cursor.farm.cursor = None
                    except Exception:
                        pass
            if self.logging and len(self.modified_tables) > 1:
                sqls = '\n'.join(self.executed_queries)
                message = 'MULTIPLE_TABLES_IN_TRANSACTION %s %s' % \
                    (sqls, ','.join(self.modified_tables))
                slog(message)
        finally:
            self.modified_cursors.clear()
            self.modified_tables.clear()
            self.executed_queries.clear()
            if first_error:
                raise first_error

    def rollback(self):
        self.transaction_end()
        first_error = None
        try:
            for cursor in self.modified_cursors:
                try:
                    cursor.connection.rollback()
                except Exception, e:
                    if not first_error:
                        first_error = e
                    try:
                        cursor.farm.cursor = None
                    except Exception:
                        pass
            if self.logging and len(self.modified_tables) > 1:
                sqls = '\n'.join(self.executed_queries)
                message = 'MULTIPLE_TABLES_IN_TRANSACTION %s %s' % \
                    (sqls, ','.join(self.modified_tables))
                slog(message)
        finally:
            self.modified_cursors.clear()
            self.modified_tables.clear()
            self.executed_queries.clear()
            if first_error:
                raise first_error

    def rollback_all(self, force=False):
        try:
            if force:
                for farm in self.farms.values():
                    try:
                        farm.get_cursor().connection.rollback()
                    except Exception:
                        try:
                            farm.cursor = None
                        except Exception:
                            pass
            else:
                for cursor in self.modified_cursors:
                    try:
                        cursor.connection.rollback()
                    except Exception:
                        try:
                            cursor.farm.cursor = None
                        except Exception:
                            pass
        finally:
            self.modified_cursors.clear()
            self.modified_tables.clear()
            self.executed_queries.clear()

    def start_log(self):
        for farm in self.farms.values():
            farm.start_log()

    def stop_log(self):
        for farm in self.farms.values():
            farm.stop_log()

    # TODO 检查所有使用detail参数的代码，删除已经废弃的detail参数
    def get_log(self, detail=False, log_format='text', with_traceback=False):
        """Return SQL logs in two formats: text or dict
        """

        logs = [farm.get_log(name, log_format, with_traceback)
                for name, farm in self.farms.items()]

        if log_format == 'dict':
            _logs = {}
            for log in logs:
                _logs.update(log)
            return _logs
        else:
            return ' '.join(logs)

    def refresh_all(self):
        """When REPEATABLE-READ or SERIALIZABLE transaction isolation level
        is used, a new transaction should be started by issuing 'commit' or
        'rollback' to get a fresher snapshot.
        """

        count = 0
        for farm in self.farms.values():
            count += farm.refresh() and 1 or 0
        return count

    def is_testing(self):
        return any(farm.is_testing() for farm in self.farms.values())

    @contextmanager
    def manage_cursor(self, farm=None, table='*', tables=None, commit=True):
        cursor = self.get_cursor(farm=farm, table=table, tables=tables)
        try:
            yield cursor
        except Exception:
            if commit:
                cursor.connection.rollback()
            raise
        else:
            if commit:
                cursor.connection.commit()

    def send_exception_to_onimaru(self, exception=None, source=None):
        if not getattr(self, 'raven_client', None):
            return

        try:
            frames = get_stack_info(iter_stack_frames())
            frames = list(reversed(frames))[:-2]
            data = {
                'sentry.interfaces.Stacktrace': {
                    'frames': frames
                }
            }
            _extra = {
                'source': CMDLINE,
                'user': USER,
                'host': host,
                'start_time': start_time,
            }
            if source:
                if isinstance(source, LuzCursor):
                    _extra['cursor'] = source.__dict__
                    _extra['farm'] = source.farm.__dict__
                    _extra['store'] = source.farm.store.__dict__
                elif isinstance(source, SqlFarm):
                    _extra['farm'] = source.__dict__
                    _extra['store'] = source.store.__dict__
                elif isinstance(source, SqlStore):
                    _extra['store'] = source.__dict__
            if exception:
                message = '%s: %s' % (exception.__class__.__name__,
                                      str(exception))
            else:
                message = 'sqlstore'
            self.raven_client.captureMessage(message, data=data, extra=_extra)
        except Exception, exc:
            slog('SEND_TO_SENTRY_FAIL: %s' % exc)


class LuzCursor():

    def __init__(self, cursor, farm):
        self.cursor = cursor
        self.farm = farm
        self.delete_without_where = self.farm.delete_without_where
        self.queries = []
        self.latest_ten_queries = collections.deque(maxlen=10)
        self.tables = set()
        self.garbage_chars = string.whitespace + ';'

        self.client_info = 'unknown'
        sql = 'select host from information_schema.processlist where id=%s'
        try:
            self.cursor.execute(sql, self.cursor.connection.thread_id())
            rs = self.cursor.fetchone()
            if rs:
                self.client_info = rs[0]
        except MySQLdb.OperationalError, exc:
            self.farm.store.send_exception_to_onimaru(exc, self)

            if 2000 <= exc.args[0] < 3000:
                self.farm.cursor = None
            # Only DBA needs to keep an eye on server gone away error
            if exc.args[0] == SERVER_GONE_ERROR:
                raise PleaseIgnoreThisMySQLException(*exc.args)
            else:
                raise

    def __str__(self):
        name = 'LuzCursor'
        return '<%s object id:%s farm:%s host:%s from:%s>' % (name,
                                                              id(self),
                                                              self.farm.name,
                                                              self.farm.host,
                                                              self.client_info)
    __repr__ = __str__

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, args=None, **kwargs):
        query_start = time.time()
        sql = sql.strip(self.garbage_chars)
        cmd = sql.split(' ', 1)[0].lower()
        host = self.farm.dbcnf['host']
        try:
            key = 'sqlstore.{host}.{cmd}'.format(host=host, cmd=cmd)
            return self._execute(cmd, sql, args, **kwargs)
        except Exception:
            exc_class, exception, tb = sys.exc_info()
            try:
                error_code = getattr(exception, 'args', [0])[0]
            except IndexError:
                # exc may be self-defined exceptions like
                # QueryDisabledException which does not have meaningful
                # error code
                error_code = 0
            key = 'sqlstore.{host}.{cmd}.{error_code}'
            key = key.format(host=host, cmd=cmd, error_code=error_code)
            raise exc_class, exception, tb
        finally:
            if self.farm.store.statsd:
                try:
                    sample_rate = self.farm.store.statsd_sample_rate
                    self.farm.store.statsd.timing_since(key,
                                                        query_start,
                                                        sample_rate)
                except Exception:
                    pass

    def _execute(self, cmd, sql, args=None, **kwargs):
        self.latest_ten_queries.append((time.time(), sql, args))
        called_from_store = kwargs.pop('called_from_store', False)

        if cmd != 'select':
            self.farm.store.modified_cursors.add(self)

        # Check if there are full parameterized queries to be blocked
        if self.farm.store.disabled_queries_with_args:
            if args:
                _sql = sql % self.cursor.connection.literal(args)
            else:
                _sql = sql
            fingerprint = md5(_sql).hexdigest()
            expire_time = \
                self.farm.store.disabled_queries_with_args.get(fingerprint)
            if expire_time:
                if expire_time > time.time():
                    raise QueryDisabledException(_sql, expire_time)
                else:
                    self.farm.store.disabled_queries_with_args.pop(fingerprint,
                                                                   None)

        # Check if there are non-parameterized quereis to be blocked
        fingerprint = md5(sql).hexdigest()
        if self.farm.store.disabled_queries:
            expire_time = self.farm.store.disabled_queries.get(fingerprint)
            if expire_time:
                if expire_time > time.time():
                    raise QueryDisabledException(sql, expire_time)
                else:
                    self.farm.store.disabled_queries.pop(fingerprint, None)

        if args is None and '%' in sql:
            message = 'POSSIBLE_MISTAKENLY_ESCAPED_SQL %s' % sql
            slog(message)

        source = os.environ.get('SQLSTORE_SOURCE') or CMDLINE
        source = source.replace('%', '%%')
        sql = sql + ' -- SRC:' + source + ' MD5:' + fingerprint + ' USER:' + \
            USER + ' CLIENT:' + self.client_info

        try:
            if self.farm.store.logging and not called_from_store:
                pre_table_cnt = len(self.tables)
                _tables = [t for t in find_tables(sql) if t in
                           self.farm.store.tables]
                self.tables.update(_tables)
                if len(self.tables) > 1 and pre_table_cnt != len(self.tables):
                    message = 'MULTIPLE_TABLES_WITH_SINGLE_CURSOR %s %s' % \
                        (sql, ','.join(self.tables))
                    slog(message)

                if self.queries:
                    self.queries.append(sql)

            norm = sql.lower()
            if not self.delete_without_where and norm.startswith('delete ') \
                    and 'where' not in norm:
                raise Exception('delete without where is forbidden')
            if not self.delete_without_where and norm.startswith('update ') \
                    and 'where' not in norm:
                raise Exception('update without where is forbidden')

            chosen_by_god = random.random() < \
                self.farm.store.treat_warning_as_error_sampling_rate

            if not self.farm.store.show_warnings and \
                    not self.farm.store.treat_warning_as_error and \
                    not chosen_by_god:
                return self.cursor.execute(sql, () if args is None else args)

            with catch_warnings(record=True) as w:
                ret = self.cursor.execute(sql, () if args is None else args)
                if w:
                    if self.farm.store.treat_warning_as_error or chosen_by_god:
                        self.cursor.connection.rollback()
                        exc = InvalidMySQLDataException(w[0].message,
                                                        sql,
                                                        args)
                        self.farm.store.send_exception_to_onimaru(exc, self)
                        raise exc

                    print >> sys.stderr, '=== MySQL warnings start ==='
                    for _w in w:
                        print >> sys.stderr, formatwarning(_w.message,
                                                           _w.category,
                                                           _w.filename,
                                                           _w.lineno,
                                                           _w.line)

                    print >> sys.stderr, '-' * 40
                    print >> sys.stderr, 'SQL:', sql
                    print >> sys.stderr, 'args:', args

                    print >> sys.stderr, '-' * 40
                    print >> sys.stderr, ''.join(traceback.format_stack())
                    print >> sys.stderr, '=== MySQL warnings end ===\n'
                return ret
        except MySQLdb.OperationalError, exc:
            self.farm.store.send_exception_to_onimaru(exc, self)

            if 2000 <= exc.args[0] < 3000:
                self.farm.cursor = None
            # Only DBA needs to keep an eye on server gone away error
            if exc.args[0] == SERVER_GONE_ERROR:
                raise PleaseIgnoreThisMySQLException(*exc.args)
            else:
                raise
        except MySQLdb.ProgrammingError, exc:
            exc_class, exception, tb = sys.exc_info()
            self.farm.store.send_exception_to_onimaru(exc, self)

            # 从 Commands out of sync 错误中自动恢复
            if exc.args[0] == COMMANDS_OUT_OF_SYNC:
                self.farm.cursor = None
                try:
                    for query in self.latest_ten_queries:
                        message = '%r COMMANDS_OUT_OF_SYNC %r' % (self, query)
                        slog(message)
                except Exception:
                    pass

            raise exc_class, exception, tb


if execute_waylifer:
    LuzCursor.execute = execute_waylifer(LuzCursor.execute)


def replace_sqlstore_config(old, new):
    _configs = get_override_configs()
    _configs[str(old)] = str(new)
    cfg_override = ['%s=>%s' % (k, v) for (k, v) in _configs.items()]
    os.environ['SQLSTORE_CONFIG_OVERRIDE'] = ' '.join(cfg_override)
    # FIXME: http://pastebin.dapps.douban.com/show/6454/
    # if new.endswith('offline'):
    #    os.environ['DOUBAN_CORELIB_DISABLE_MC'] = '1'


def get_override_configs():
    cfg_override = os.environ.get('SQLSTORE_CONFIG_OVERRIDE', '')
    try:
        _configs = dict([i.split('=>') for i in cfg_override.split()])
    except Exception:
        _configs = {}
    return _configs


def check_override(db_config_name):
    # Allow config override via environment variable
    # SQLSTORE_CONFIG_OVERRIDE="shire-online=>shire-offline ark-online=>
    # ark-offline"
    _configs = get_override_configs()
    return _configs.get(db_config_name, db_config_name)


_stores = {}


def store_from_config(config, use_cache=True, created_via='UNKNOWN_APP',
                      **kwargs):
    if not SqlStore.is_safe(created_via):
        warn('SqlStore should be created via DAE API in async mode '
             '(created via: %s' % created_via)
    if isinstance(config, basestring):
        config = check_override(config)
    if isinstance(config, (dict, basestring)):
        cache_key = hashdict([config, kwargs])
    else:
        # unexpected config format, make it unhashable, do not cache
        cache_key = {}

    cachable = use_cache and isinstance(cache_key, collections.Hashable)
    store = _stores.get(cache_key) if cachable else None
    if store is None:
        db_config_name = None
        db_config = None
        if isinstance(config, basestring):
            db_config_name = config
        else:
            db_config = config
        store = SqlStore(db_config=db_config, db_config_name=db_config_name,
                         created_via=created_via, **kwargs)
        if cachable:
            _stores[cache_key] = store
    store.rollback_all()
    return store



# vim: set et ts=4 sw=4 :
