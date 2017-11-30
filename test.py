from __future__ import print_function, unicode_literals
import unittest
import datetime
import json
import app
from chalicelib.check_utils import *
from chalicelib.utils import *
from chalicelib.fs_connection import FSConnection


class TestUnitTests(unittest.TestCase):
    environ_info = {
        'fourfront': None,
        'es': None,
        'bucket': None,
        'ff_env': None
    }
    connection = FSConnection('test', environ_info)

    def test_connection_fields(self):
        self.assertTrue(self.connection.fs_environment == 'test')
        self.assertTrue(self.connection.s3_connection.status_code == 404)

    def test_run_check_basics(self):
        check_res = run_check(self.connection, 'wrangler_checks/item_counts_by_type', {})
        # run_check returns a dict with results
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == 'item_counts_by_type')

    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, []],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', []],
            ['syscks/indexing_progress', {}, []],
            ['wrangler_checks/iteasdts_by_type', {}, []],
            ['system_checks/test_function_unused', {}, []]
        ]
        for bad_check_info in bad_check_group:
            check_res = run_check(self.connection, bad_check_info[0], bad_check_info[1])
            self.assertTrue(isinstance(check_res, str))
            self.assertTrue('ERROR' in check_res)

    def test_checkresult_basics(self):
        test_check = init_check_res(self.connection, 'test_check', description='Unittest check')
        self.assertTrue(test_check.s3_connection.status_code == 404)
        self.assertTrue(test_check.get_latest_check() is None)
        self.assertTrue(test_check.get_closest_check(1) is None)
        self.assertTrue(test_check.title == 'Test Check')
        formatted_res = test_check.format_result(datetime.datetime.utcnow())
        self.assertTrue(formatted_res.get('status') == 'PEND')
        self.assertTrue(formatted_res.get('title') == 'Test Check')
        self.assertTrue(formatted_res.get('description') == 'Unittest check')
        check_res = test_check.store_result()
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == formatted_res.get('name'))
        self.assertTrue(check_res.get('description') == "Malformed status; look at Foursight check definition.")
        self.assertTrue(check_res.get('brief_output') == formatted_res.get('brief_output') == None)


class TestIntegrated(unittest.TestCase):
    # should add tests for functions that require app.current_request
    # will need to figure out how to mock these
    environ = 'mastertest' # hopefully this is up
    conn, _ = app.init_connection(environ)
    if conn is None:
        environ = 'hotseat' # back up if self.environ is down
        conn, _ = app.init_connection(environ)

    def test_init_connection(self):
        self.assertFalse(self.conn is None)
        # test the ff connection
        assert(self.conn.fs_environment == 'mastertest')
        assert(self.conn.ff)
        assert(self.conn.es)
        assert(self.conn.ff_env == 'fourfront-mastertest')

    def test_init_environments(self):
        app.init_environments() # default to 'all' environments
        assert(self.environ in app.ENVIRONMENTS)
        for env, env_data in app.ENVIRONMENTS.items():
            assert('fourfront' in env_data)
            assert('es' in env_data)
            assert('bucket' in env_data)
            assert('ff_env' in env_data)

    def test_get_check(self):
        # do this for every check
        for check_str in get_check_strings():
            get_check = check_str.split('/')[1]
            chalice_resp = app.get_check(self.environ, get_check)
            self.assertTrue(chalice_resp.status_code == 200)
            body = chalice_resp.body
            self.assertTrue(body.get('status') == 'success')
            self.assertTrue(body.get('checks_found') == get_check)
            self.assertTrue(body.get('checks', {}).get('name') == get_check)
            self.assertTrue(body.get('checks', {}).get('status') in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'])
            self.assertTrue('timestamp' in body.get('checks', {}))

    def test_get_environment(self):
        env_resp = app.get_environment(self.environ)
        self.assertTrue(env_resp.status_code == 200)
        body = env_resp.body
        self.assertTrue(body.get('environment') == self.environ)
        self.assertTrue(body.get('status') == 'success')
        details = body.get('details')
        self.assertTrue(details.get('bucket').startswith('foursight-'))
        self.assertTrue(details.get('bucket').endswith(self.environ))
        this_env = app.ENVIRONMENTS.get(self.environ)
        self.assertTrue(this_env == details)


if __name__ == '__main__':
    unittest.main()
