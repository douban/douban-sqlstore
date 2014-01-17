# encoding=utf8

import os
import pwd
import tempfile
from unittest import TestCase
from warnings import catch_warnings

import MySQLdb
from mock import patch, Mock, MagicMock
from nose.tools import eq_, ok_

tmp_config_dir = '/tmp/sqlstore-%s' % pwd.getpwuid(os.geteuid()).pw_name
import douban.utils.config
douban.utils.config.config_dir = tmp_config_dir

import douban.sqlstore as M


class ModuleTest(TestCase):
    _format = '%Y-%m-%d %H:%M:%S'
    database = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
        },
    }

    def test_DBConfig_should_exists_in_module(self):
        ok_(hasattr(M, 'DBConfig'))

    def test_store_from_config_should_accept_dict_as_parameter(self):
        M.store_from_config({})

    def test_store_from_config_should_cache_store_objects(self):
        f = tempfile.NamedTemporaryFile()
        print >>f, "{}"
        f.flush()
        store1 = M.store_from_config(f.name)
        store2 = M.store_from_config(f.name)
        ok_(store1 is store2, 'store1 and store2 are not same')

    def test_store_from_config_should_not_cache_when_use_cache_is_False(self):
        f = tempfile.NamedTemporaryFile()
        print >>f, "{}"
        f.flush()
        store1 = M.store_from_config(f.name)
        store2 = M.store_from_config(f.name, use_cache=False)
        ok_(store1 is not store2, 'store1 and store2 are same')

    def test_store_from_config_with_dict_should_cache_store_objects(self):
        store1 = M.store_from_config(self.database)
        store2 = M.store_from_config(self.database)
        ok_(store1 is store2, 'store1 and store2 are not same')

    def test_store_from_config_with_dict_should_not_cache(self):
        store1 = M.store_from_config(self.database)
        store2 = M.store_from_config(self.database, use_cache=False)
        ok_(store1 is not store2, 'store1 and store2 are same')

    def prepare_store(self, use_cache=False, created_via='test_sqlstore',
                      **kwargs):
        return M.store_from_config(self.database, use_cache=use_cache,
                                   created_via=created_via, **kwargs)

    def test_start_log_should_log_calls(self):
        store = self.prepare_store()

        store.start_log()
        try:
            c = store.get_cursor(table='test_table1')
            c.execute("select * from test_table1 limit 1")
            log = store.get_log()
            ok_(log, 'No log')
        finally:
            store.stop_log()

    def test_stop_log_multiple_times_should_not_change_cursor(self):
        store = self.prepare_store()

        store.start_log()
        c = store.get_cursor(table='test_table1')
        c.execute("select * from test_table1 limit 1")
        store.stop_log()
        store.stop_log()
        c = store.get_cursor(table='test_table1')
        ok_(isinstance(c, M.LuzCursor), 'c is not LuzCursor instance')

    def test_transaction(self):
        store = self.prepare_store()

        store.transaction_begin()
        store.execute("update test_table1 set id=id where id=1")
        store.execute("update test_table2 set id=id where id=1")

        # begin another transaction before commit/rollback should raise
        self.assertRaises(Exception, store.transaction_begin)
        store.commit()

        # empty transaction
        store.transaction_begin()
        store.transaction_end()

    def test_modified_cursor(self):
        store = self.prepare_store()
        # test_table1 is in farm1
        store.execute("update test_table1 set id=id where id=1")
        eq_(len(store.modified_cursors), 1)

        # test_table2 is in farm2
        cursor = store.get_cursor(table='test_table2')
        cursor.execute('update test_table2 set id=id where id=1')
        eq_(len(store.modified_cursors), 2)

        store.rollback_all()
        eq_(len(store.modified_cursors), 0)

    def test_sqlstore_should_not_allow_unsafe_use(self):
        os.environ['DAE_WORKER'] = 'async'
        with catch_warnings(record=True) as w:
            self.prepare_store()
            del os.environ['DAE_WORKER']
            found_unsafe_warning = False
            msg = 'SqlStore should be created via DAE API in async mode'
            for _w in w:
                if msg in str(_w.message):
                    found_unsafe_warning = True
            ok_(found_unsafe_warning, 'Sqlstore is not safe')

    def test_sqlstore_should_allow_safe_use(self):
        os.environ['DAE_WORKER'] = 'async'
        with catch_warnings(record=True) as w:
            self.prepare_store(created_via='DAE_API')
            del os.environ['DAE_WORKER']
            found_unsafe_warning = False
            msg = 'SqlStore should be created via DAE API in async mode'
            for _w in w:
                if msg in str(_w.message):
                    found_unsafe_warning = True
            ok_(not found_unsafe_warning, 'Sqlstore safe checking overkills')


class LogTest(TestCase):

    def test_log_without_scribe(self):
        temp = douban.utils.slog.scribeclient
        douban.utils.slog.scribeclient = None
        from douban.sqlstore import slog
        with patch('sys.stderr') as mock_stderr:
            slog('testmessage')
            ok_(mock_stderr.write.called, 'Log is not printed to stderr')
        douban.utils.slog.scribeclient = temp

    def test_log_with_scribe(self):
        mock = Mock()
        temp = douban.utils.slog.scribeclient
        douban.utils.slog.scribeclient = mock
        from douban.sqlstore import slog
        slog('test-message')
        ok_(mock.send.called, 'Does not use scribeclient')
        douban.utils.slog.scribeclient = temp


class TestSqlStoreConfigLoad(TestCase):

    sqlstore_tmp_config = os.path.join(tmp_config_dir, "sqlstore")
    online_path = os.path.join(sqlstore_tmp_config, "test-online")
    offline_path = os.path.join(sqlstore_tmp_config, "test-offline")

    online_content = """
{
    'farms':{
        'farm1': {
            "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
            "tables": ["*"],
        }
    }
}
"""

    offline_content = """
{
    'farms':{
        'farm1': {
            "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
            "tables": ["*"],
        }
    }
}
"""

    def setUp(self):
        if not os.path.exists(self.sqlstore_tmp_config):
            os.makedirs(self.sqlstore_tmp_config, 0755)
        with open(self.online_path, 'w') as f:
            f.write(self.online_content)
        with open(self.offline_path, 'w') as f:
            f.write(self.offline_content)

    def tearDown(self):
        os.remove(self.online_path)
        os.remove(self.offline_path)

    def test_normal_load_configfile(self):
        store = M.store_from_config('test-online', use_cache=False)
        eq_(store.db_config_name, 'test-online')
        eq_(store.get_farm('farm1').dbcnf['db'], 'test_sqlstore1')

    def test_replace(self):
        M.replace_sqlstore_config('test-online', 'test-offline')
        store = M.store_from_config('test-online', use_cache=False)
        eq_(store.db_config_name, 'test-offline')
        eq_(store.get_farm('farm1').dbcnf['db'], 'test_sqlstore2')

    def test_pickle(self):
        import pickle
        store = M.store_from_config('test-online', use_cache=False)
        buf = pickle.dumps(store)
        _store = pickle.loads(buf)
        eq_(store.db_config, _store.db_config)
        eq_(store.db_config_name, _store.db_config_name)
        eq_(len(store.farms), len(_store.farms))
        M.replace_sqlstore_config('test-online', 'test-offline')
        eq_(store.db_config_name, 'test-online')
        _store_2 = pickle.loads(buf)
        eq_(_store_2.db_config_name, 'test-offline')
        eq_(store.get_farm('farm1').dbcnf['db'], 'test_sqlstore1')


class SentryTest(TestCase):
    database = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
        },
        'sentry_dsn': 'http://abc:def@abc.com/111',
    }

    database_no_sentry = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
        },
    }

    def test_report_to_sentry(self):
        store = M.store_from_config(self.database)
        with patch('raven.Client.captureMessage') as raven_mock:
            try:
                store.execute('select a from non_existing_table limit 1')
            except MySQLdb.ProgrammingError:
                pass
            ok_(raven_mock.called, 'Does not report to sentry')

    def test_no_sentry(self):
        store = M.store_from_config(self.database_no_sentry)
        with patch('raven.Client.captureMessage') as raven_mock:
            try:
                store.execute('select a from non_existing_table limit 1')
            except MySQLdb.ProgrammingError:
                pass
            ok_(not raven_mock.called, 'Should not report to sentry')

    def test_no_interference_between_stores(self):
        store1 = M.store_from_config(self.database)
        store2 = M.store_from_config(self.database_no_sentry)
        store3 = M.store_from_config(self.database, use_cache=False)

        with patch('raven.Client.captureMessage') as raven_mock1:
            try:
                store1.execute('select a from non_existing_table limit 1')
            except MySQLdb.ProgrammingError:
                pass
            ok_(raven_mock1.called, 'Does not report to sentry')

        with patch('raven.Client.captureMessage') as raven_mock2:
            try:
                store2.execute('select a from non_existing_table limit 1')
            except MySQLdb.ProgrammingError:
                pass
            ok_(not raven_mock2.called, 'Should not report to sentry')

        with patch('raven.Client.captureMessage') as raven_mock3:
            try:
                store3.execute('select a from non_existing_table limit 1')
            except MySQLdb.ProgrammingError:
                pass
            ok_(raven_mock3.called, 'Does not report to sentry')


class ConfigPushTest(TestCase):
    database = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2", "*"],
            },
        },
    }

    database_new_config = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore3:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
            "farm3": {
                "master": "127.0.0.1:3306:test_sqlstore3:sqlstore:sqlstore",
                "tables": ["test_table3"],
            },
        },
    }

    def test_push_config(self):
        farm1_dbconf = {
            'host': '127.0.0.1',
            'port': 3306,
            'db': 'test_sqlstore1',
            'user': 'sqlstore',
            'passwd': 'sqlstore',
        }
        farm1_new_dbconf = {
            'host': '127.0.0.1',
            'port': 3306,
            'db': 'test_sqlstore3',
            'user': 'sqlstore',
            'passwd': 'sqlstore',
        }
        farm2_dbconf = {
            'host': '127.0.0.1',
            'port': 3306,
            'db': 'test_sqlstore2',
            'user': 'sqlstore',
            'passwd': 'sqlstore',
        }
        farm3_dbconf = {
            'host': '127.0.0.1',
            'port': 3306,
            'db': 'test_sqlstore3',
            'user': 'sqlstore',
            'passwd': 'sqlstore',
        }

        store = M.store_from_config(self.database)
        farm1 = store.get_farm('farm1')
        eq_(farm1.dbcnf, farm1_dbconf)
        farm2 = store.get_farm('farm2')
        eq_(farm2.dbcnf, farm2_dbconf)

        # push new config
        store.receive_conf(str(self.database_new_config))

        # farm1 should change
        _farm1 = store.get_farm('farm1')
        eq_(_farm1.dbcnf, farm1_new_dbconf)
        ok_(_farm1 is not farm1)

        # farm2 should be re-used
        _farm2 = store.get_farm('farm2')
        eq_(_farm2.dbcnf, farm2_dbconf)
        ok_(_farm2 is farm2)

        # farm3 should be created
        farm3 = store.get_farm('farm3')
        eq_(farm3.dbcnf, farm3_dbconf)


class StatsdTest(TestCase):
    database = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
        },
        'statsd': {
            'config': 'test',
            'sample_rate': 1
        }
    }

    database_no_statsd = {
        'farms': {
            "farm1": {
                "master": "127.0.0.1:3306:test_sqlstore1:sqlstore:sqlstore",
                "tables": ["test_table1", "*"],
            },
            "farm2": {
                "master": "127.0.0.1:3306:test_sqlstore2:sqlstore:sqlstore",
                "tables": ["test_table2"],
            },
        },
    }

    @patch('statsdclient.statsd_from_config')
    def test_exception_stats(self, mock_statsd_from_config):
        statsd = MagicMock()
        mock_statsd_from_config.return_value = statsd
        store = M.store_from_config(self.database, use_cache=False)
        try:
            store.execute('select a from non_existing_table limit 1')
        except MySQLdb.ProgrammingError:
            pass
        ok_(statsd.timing_since.called,
            'statsd.timing_since is not called')

    @patch('statsdclient.statsd_from_config')
    def test_success_stats(self, mock_statsd_from_config):
        statsd = MagicMock()
        mock_statsd_from_config.return_value = statsd
        store = M.store_from_config(self.database, use_cache=False)
        store.execute('select id from test_table1 limit 1')
        ok_(statsd.timing_since.called,
            'statsd.timing_since is not called')

    @patch('statsdclient.statsd_from_config')
    def test_exception_no_stats(self, mock_statsd_from_config):
        statsd = MagicMock()
        mock_statsd_from_config.return_value = statsd
        store = M.store_from_config(self.database_no_statsd, use_cache=False)
        try:
            store.execute('select a from non_existing_table limit 1')
        except MySQLdb.ProgrammingError:
            pass
        ok_(not statsd.timing_since.called,
            'statsd.timing_since is called')

    @patch('statsdclient.statsd_from_config')
    def test_success_no_stats(self, mock_statsd_from_config):
        statsd = MagicMock()
        mock_statsd_from_config.return_value = statsd
        store = M.store_from_config(self.database_no_statsd, use_cache=False)
        store.execute('select id from test_table1 limit 1')
        ok_(not statsd.timing_since.called,
            'statsd.timing_since is called')


if __name__ == '__main__':
    import unittest
    unittest.main()
