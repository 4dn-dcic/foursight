from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import app
from chalicelib import check_utils, utils, check_groups, wrangler_utils, checkresult
from chalicelib.fs_connection import FSConnection
from dateutil import tz


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
        check_res = check_utils.run_check(self.connection, 'wrangler_checks/item_counts_by_type', {})
        # run_check returns a dict with results
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == 'item_counts_by_type')

    def test_checkresult_basics(self):
        test_check = utils.init_check_res(self.connection, 'test_check', description='Unittest check', ff_link='not_a_real_http_link')
        self.assertTrue(test_check.s3_connection.status_code == 404)
        self.assertTrue(test_check.get_latest_check() is None)
        self.assertTrue(test_check.get_closest_check(1) is None)
        self.assertTrue(test_check.title == 'Test Check')
        formatted_res = test_check.format_result(datetime.datetime.utcnow())
        self.assertTrue(formatted_res.get('status') == 'IGNORE')
        self.assertTrue(formatted_res.get('title') == 'Test Check')
        self.assertTrue(formatted_res.get('description') == 'Unittest check')
        # set a bad status on purpose
        test_check.status = "BAD_STATUS"
        check_res = test_check.store_result()
        self.assertTrue(check_res.get('name') == formatted_res.get('name'))
        self.assertTrue(check_res.get('description') == "Malformed status; look at Foursight check definition.")
        self.assertTrue(check_res.get('brief_output') == formatted_res.get('brief_output') == None)
        self.assertTrue(check_res.get('ff_link') == 'not_a_real_http_link')


class TestAppRoutes(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app.init_connection(environ)

    def home_route(self):
        res = app.index()
        self.assertTrue(json.loads(res) == {'foursight': 'insight into fourfront'})

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
        res2 = app.view_rerun(self.environ, 'indexing_progress')
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

    def test_put_environment(self):
        # this one is interesting... will be tested by putting a clone of
        # mastertest into itself. actual fxn run is run_put_environment
        get_res = app.get_environment(self.environ)
        env_data = get_res.body.get('details')
        # make sure the environ we have is legit
        self.assertTrue(env_data and 'fourfront' in env_data and 'es' in env_data and 'ff_env' in env_data)
        env_res = app.run_put_environment(self.environ, env_data)
        self.assertTrue(env_res.status_code == 200)
        self.assertTrue(env_res.body.get('status') == 'success')
        self.assertTrue(env_res.body.get('environment') == self.environ)
        self.assertTrue(env_res.body.get('description') == 'Succesfully made: ' + self.environ)
        checks_run = env_res.body.get('initial_checks')
        self.assertTrue(isinstance(checks_run, list) and len(checks_run) > 0)
        # failure case
        bad_res = app.run_put_environment(self.environ, {'key1': 'res1'})
        self.assertTrue(bad_res.status_code == 400)
        self.assertTrue(bad_res.body.get('status') == 'error')
        self.assertTrue(bad_res.body.get('body') == {'key1': 'res1'})
        self.assertTrue(bad_res.body.get('description') == 'Environment creation failed')
        # make sure they match after run_put_environment
        get_res2 = app.get_environment(self.environ)
        self.assertTrue(get_res.body == get_res2.body)

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

    def test_put_check(self):
        # actually tests run_put_check, which holds all functionality
        # besides app.current_request
        check_name = 'test_put_check'
        ts_uuid = datetime.datetime.utcnow().isoformat()
        put_data = {
            'description': 'Just a test for run_put_check',
            'brief_output': ['res1'],
            'full_output': {'key1': 'res1', 'key2': 'res2'},
            'uuid': ts_uuid
        }
        res = app.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check'] == check_name)
        put_res = res.body['updated_content']
        self.assertTrue(put_res is not None)
        self.assertTrue(put_res.get('uuid') == ts_uuid)
        # now put another one with the same uuid
        put_data['brief_output'] = ['res2']
        put_data['full_output'] = {'key2': 'res3'}
        res = app.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_res = res.body['updated_content']
        self.assertTrue(put_res['brief_output'] == ['res1', 'res2'])
        self.assertTrue(put_res['full_output'] == {'key1': 'res1', 'key2': 'res3'})
        # now do it with strings. brief_output should be unchanged if we don't overwrite it
        del put_data['brief_output']
        put_data['full_output'] = 'abc '
        res = app.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_data['full_output'] = '123'
        res = app.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_res = res.body['updated_content']
        self.assertTrue(put_res['brief_output'] == ['res1', 'res2'])
        self.assertTrue(put_res['full_output'] == 'abc 123')
        # lastly, cover bad output
        put_data = 'NOT_A_DICT'
        res = app.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 400)
        self.assertTrue(res.body['status'] == 'error')
        self.assertTrue(res.body['description'] == 'PUT request is malformed: NOT_A_DICT')


class TestCheckResult(unittest.TestCase):
    # use a fake check name and store on mastertest
    check_name = 'test_only_check'
    environ = 'mastertest' # hopefully this is up
    connection, _ = app.init_connection(environ)

    def test_check_result_methods(self):
        check = checkresult.CheckResult(self.connection.s3_connection, self.check_name)
        # default status
        self.assertTrue(check.status == 'IGNORE')
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.full_output = ['first_item']
        res = check.store_result()
        # fetch this check. latest and closest result with 0 diff should be the same
        late_res = check.get_latest_check()
        self.assertTrue(late_res == res)
        close_res = check.get_closest_check(0, 0)
        self.assertTrue(close_res == res)
        all_res = check.get_all_checks()
        self.assertTrue(len(all_res) > 0)
        # this should be true since all results will be identical
        self.assertTrue(all_res[-1].get('description') == res.get('description'))
        # ensure that previous check results can be fetch using the uuid functionality
        res_uuid = res['uuid']
        check_copy = checkresult.CheckResult(self.connection.s3_connection, self.check_name, uuid=res_uuid)
        self.assertTrue(res == check_copy.store_result())


class TestCheckUtils(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app.init_connection(environ)

    def test_get_check_strings(self):
        # do this for every check
        all_check_strs = check_utils.get_check_strings()
        for check_str in all_check_strs:
            get_check = check_str.split('/')[1]
            chalice_resp = app.get_check(self.environ, get_check)
            body = chalice_resp.body
            if body.get('status') == 'success':
                self.assertTrue(chalice_resp.status_code == 200)
                self.assertTrue(body.get('checks_found') == get_check)
                self.assertTrue(body.get('checks', {}).get('name') == get_check)
                self.assertTrue(body.get('checks', {}).get('status') in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'])
                self.assertTrue('uuid' in body.get('checks', {}))
            elif body.get('status') == 'error':
                error_msg = "Could not get results for: " + get_check + ". Maybe no such check result exists?"
                self.assertTrue(body.get('description') == error_msg)
        # test a specific check
        one_check_str = check_utils.get_check_strings('indexing_progress')
        self.assertTrue(one_check_str == 'system_checks/indexing_progress')
        self.assertTrue(one_check_str in all_check_strs)
        # test a specific check that doesn't exist
        bad_check_str = check_utils.get_check_strings('not_a_real_check')
        self.assertTrue(bad_check_str is None)

    def test_fetch_check_group(self):
        all_checks = check_utils.fetch_check_group('all')
        self.assertTrue(isinstance(all_checks, list) and len(all_checks) > 0)
        daily_checks = check_utils.fetch_check_group('daily_checks')
        # get list of check strings from lists of check info
        daily_check_strs = [chk_str[0] for chk_str in daily_checks]
        all_check_strs = [chk_str[0] for chk_str in all_checks]
        self.assertTrue(set(daily_check_strs) <= set(all_check_strs))
        # make sure there are not duplicate check check names
        self.assertTrue(len(all_check_strs) == len(set(all_check_strs)))
        # non-existant check group
        bad_checks = check_utils.fetch_check_group('not_a_check_group')
        self.assertTrue(bad_checks is None)

    def test_run_check_group(self):
        all_checks_res = check_utils.run_check_group(self.conn, 'all')
        self.assertTrue(isinstance(all_checks_res, list) and len(all_checks_res) > 0)
        for check_res in all_checks_res:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
            self.assertTrue('uuid' in check_res)
        # non-existant check group
        bad_checks_res = check_utils.run_check_group(self.conn, 'not_a_check_group')
        assert(bad_checks_res == [])
        # use a bad check groups
        test_checks_res = check_utils.run_check_group(self.conn, 'malformed_test_checks')
        assert("ERROR with [{}, []] in group: malformed_test_checks" in test_checks_res)
        assert("ERROR with ['system_checks/indexing_progress', []] in group: malformed_test_checks" in test_checks_res)
        assert("ERROR with ['system_checks/indexing_progress', {}] in group: malformed_test_checks" in test_checks_res)

    def run_check_group_repeats(self):
        repeat_res = check_utils.run_check_group(self.conn, 'wrangler_test_checks')
        unified_uuid = None
        for check_res in repeat_res:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
            self.assertTrue('uuid' in check_res)
            if unified_uuid:
                self.assertTrue(check_res['uuid'] == unified_uuid)
            else:
                # gotta set it on the first iteration
                unified_uuid = check_res['uuid']

    def test_get_check_group_latest(self):
        all_res = check_utils.get_check_group_latest(self.conn, 'all')
        for check_res in all_res:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
        # non-existant check group
        bad_res = check_utils.get_check_group_latest(self.conn, 'not_a_check_group')
        assert(bad_res == [])
        # bad check group. will skip all malformed checks
        test_res = check_utils.get_check_group_latest(self.conn, 'malformed_test_checks')
        assert(len(test_res) == 0)

    def test_run_check(self):
        test_info = ['system_checks/indexing_records', {}, []]
        check_res = check_utils.run_check(self.conn, test_info[0], test_info[1])
        self.assertTrue(isinstance(check_res, dict))
        self.assertTrue('name' in check_res)
        self.assertTrue('status' in check_res)

    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, []],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', []],
            ['syscks/indexing_progress', {}, []],
            ['wrangler_checks/iteasdts_by_type', {}, []],
            ['system_checks/test_function_unused', {}, []]
        ]
        for bad_check_info in bad_check_group:
            check_res = check_utils.run_check(self.conn, bad_check_info[0], bad_check_info[1])
            self.assertFalse(isinstance(check_res, dict))
            self.assertTrue('ERROR' in check_res)

    def test_check_groups(self):
        # this may not be the best approach
        for key, val in check_groups.__dict__.items():
            if '_checks' in key and isinstance(val, list):
                for check_info in val:
                    if key != 'malformed_test_checks':
                        self.assertTrue(len(check_info) == 3)
                        self.assertTrue(isinstance(check_info[1], dict))
                        self.assertTrue(isinstance(check_info[2], list))
                    else:
                        self.assertTrue(len(check_info) != 3)


class TestUtils(unittest.TestCase):
    @utils.check_function(abc=123)
    def test_function_dummy(*args, **kwargs):
        return kwargs

    def test_check_function_deco_default_kwargs(self):
        # test to see if the check_function decorator correctly overrides
        # kwargs of decorated function if none are provided
        kwargs_default = self.test_function_dummy()
        self.assertTrue(kwargs_default == {'abc': 123})
        kwargs_add = self.test_function_dummy(bcd=234)
        self.assertTrue(kwargs_add == {'abc': 123, 'bcd': 234})
        kwargs_override = self.test_function_dummy(abc=234)
        self.assertTrue(kwargs_override == {'abc': 234})

    def test_build_dummy_result(self):
        dummy_check = 'dumb_test'
        dummy_res = utils.build_dummy_result(dummy_check)
        self.assertTrue(dummy_res['status'] == 'IGNORE')
        self.assertTrue(dummy_res['name']) == dummy_check
        self.assertTrue('uuid' in dummy_res)


class TestWranglerUtils(unittest.TestCase):
    timestr_1 = '2017-04-09T17:34:53.423589+00:00' # UTC
    timestr_2 = '2017-04-09T17:34:53.423589+05:00' # 5 hours ahead of UTC
    timestr_3 = '2017-04-09T17:34:53.423589'
    timestr_4 = '2017-04-09T17:34:53'
    timestr_bad = '2017-04-0589+00:00'

    def test_parse_datetime_with_tz_to_utc(self):
        dt_tz_a = None
        dt_tz_b = None
        for t_str in [self.timestr_1, self.timestr_2, self.timestr_3, self.timestr_4]:
            dt = wrangler_utils.parse_datetime_with_tz_to_utc(t_str)
            self.assertTrue(dt is not None)
            self.assertTrue(dt.tzinfo is not None and dt.tzinfo == tz.tzutc())
            if t_str == self.timestr_1:
                dt_tz_a = dt
            elif t_str == self.timestr_2:
                dt_tz_b = dt
        self.assertTrue(dt_tz_a > dt_tz_b)
        dt_bad = wrangler_utils.parse_datetime_with_tz_to_utc(self.timestr_bad)
        self.assertTrue(dt_bad is None)

    def test_get_FDN_Connection(self):
        # run this for all environments to ensure access keys are in place
        app.init_environments()
        for env in app.ENVIRONMENTS:
            conn, _ = app.init_connection(env)
            fdn_conn = wrangler_utils.get_FDN_Connection(conn)
            self.assertTrue(fdn_conn is not None)


if __name__ == '__main__':
    unittest.main()
