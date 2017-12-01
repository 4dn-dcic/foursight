from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import app
from chalicelib.check_utils import *
from chalicelib.utils import *
from chalicelib.fs_connection import FSConnection


class TestFSConnection(unittest.TestCase):
    environ_info = {
        'fourfront': 'test1',
        'es': 'test2',
        'bucket': None,
        'ff_env': 'test3'
    }
    connection = FSConnection('test', environ_info)

    def test_connection_fields(self):
        self.assertTrue(self.connection.fs_environment == 'test')
        self.assertTrue(self.connection.s3_connection.status_code == 404)
        self.assertTrue(self.connection.ff == 'test1')
        self.assertTrue(self.connection.es == 'test2')
        self.assertTrue(self.connection.ff_env == 'test3')

    def test_run_check_with_bad_connection(self):
        check_res = run_check(self.connection, 'wrangler_checks/item_counts_by_type', {})
        # run_check returns a dict with results
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == 'item_counts_by_type')

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


class TestAppRoutes(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app.init_connection(environ)
    if conn is None:
        environ = 'hotseat' # back up if self.environ is down
        conn, _ = app.init_connection(environ)

    def test_init_connection(self):
        self.assertFalse(self.conn is None)
        # test the ff connection
        self.assertTrue(self.conn.fs_environment == 'mastertest')
        self.assertTrue(self.conn.ff)
        self.assertTrue(self.conn.es)
        self.assertTrue(self.conn.ff_env == 'fourfront-mastertest')

    def test_init_environments(self):
        app.init_environments() # default to 'all' environments
        self.assertTrue(self.environ in app.ENVIRONMENTS)
        for env, env_data in app.ENVIRONMENTS.items():
            self.assertTrue('fourfront' in env_data)
            self.assertTrue('es' in env_data)
            self.assertTrue('bucket' in env_data)
            self.assertTrue('ff_env' in env_data)

    def test_init_response(self):
        # a good reponse
        connection, response = app.init_response(self.environ)
        self.assertTrue(connection is not None)
        self.assertTrue(response.body == 'Foursight response')
        # a bad Response
        connection, response = app.init_response('not_an_environment')
        self.assertTrue(connection is None)
        self.assertTrue(response.body != 'Foursight response')
        self.assertTrue(response.status_code == 400)

    def test_view_foursight(self):
        res = app.view_foursight(self.environ)
        self.assertTrue(res.headers == {u'Content-Type': u'text/html'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.to_dict().keys()) == set(['body', 'headers', 'statusCode']))
        self.assertTrue('<!DOCTYPE html>' in res.body)
        self.assertTrue('Foursight' in res.body)
        # this is pretty weak
        res2 = app.view_rerun('environ', 'indexing_progress')
        self.assertTrue('<!DOCTYPE html>' in res2.body)
        self.assertTrue('Foursight' in res2.body)
        self.assertFalse(res2 == res)

    def test_run_foursight_checks(self):
        res = app.run_foursight_checks(self.environ, 'all')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'all')
        self.assertTrue(isinstance(res.body['checks'], list) and len(res.body['checks']) > 0)

    def test_get_foursight_checks(self):
        res = app.get_foursight_checks(self.environ, 'all')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'all')
        self.assertTrue(isinstance(res.body['checks'], list) and len(res.body['checks']) > 0)

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
        # bad environment
        resp2 = app.get_environment('not_an_environment')
        self.assertTrue(resp2.status_code == 400)
        self.assertTrue(resp2.body['status'] == 'error')
        self.assertTrue('Invalid environment provided' in resp2.body['description'])

    def test_get_check(self):
        test_check = 'indexing_progress'
        res = app.get_check(self.environ, test_check)
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'checks_found']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(isinstance(res.body['checks'], dict) and res.body['checks']['name'] == test_check)
        self.assertTrue(res.body['checks_found'] == test_check)
        # bad response
        res = app.get_check(self.environ, 'not_a_real_check')
        self.assertTrue(res.status_code == 400)
        self.assertTrue(res.body['status'] == 'error')
        self.assertTrue(res.body['description'] == 'Could not get results for: not_a_real_check. Maybe no such check result exists?')
        # still need to figure out how to to test put_check and put_environment


class TestCheckUtils(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app.init_connection(environ)
    if conn is None:
        environ = 'hotseat' # back up if self.environ is down
        conn, _ = app.init_connection(environ)

    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, []],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', []],
            ['syscks/indexing_progress', {}, []],
            ['wrangler_checks/iteasdts_by_type', {}, []],
            ['system_checks/test_function_unused', {}, []]
        ]
        for bad_check_info in bad_check_group:
            check_res = run_check(self.conn, bad_check_info[0], bad_check_info[1])
            self.assertFalse(isinstance(check_res, dict))
            self.assertTrue('ERROR' in check_res)

    def test_get_check_strings(self):
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


if __name__ == '__main__':
    unittest.main()
