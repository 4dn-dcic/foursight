from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import os
import time
import app
from chalicelib import (
    app_utils,
    check_utils,
    utils,
    run_result,
    fs_connection,
    s3_connection
)
from dcicutils import s3_utils, ff_utils
from dateutil import tz


class FSTest(unittest.TestCase):
    def setUp(self):
        self.t_start = time.time()
        print(''.join(['\n\nRunning: ', self._testMethodName]))

    def tearDown(self):
        print('Took %s seconds.' % str(time.time()-self.t_start))

class TestFSConnection(FSTest):
    environ_info = {
        'fourfront': 'test1',
        'es': 'test2',
        'bucket': None,
        'ff_env': 'test3'
    }
    connection = fs_connection.FSConnection('test', environ_info, test=True)

    def test_connection_fields(self):
        self.assertTrue(self.connection.fs_env == 'test')
        self.assertTrue(self.connection.s3_connection.status_code == 404)
        self.assertTrue(self.connection.ff_server == 'test1')
        self.assertTrue(self.connection.ff_es == 'test2')
        self.assertTrue(self.connection.ff_env == 'test3')
        self.assertTrue(self.connection.ff_s3 is None)
        self.assertTrue(self.connection.ff_keys is None)

    def test_run_check_with_bad_connection(self):
        check_res = check_utils.run_check_or_action(self.connection, 'wrangler_checks/item_counts_by_type', {})
        # run_check_or_action returns a dict with results
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == 'item_counts_by_type')

    def test_check_result_basics(self):
        test_check = utils.init_check_res(self.connection, 'test_check')
        test_check.description = 'Unittest check'
        test_check.ff_link = 'not_a_real_http_link'
        self.assertTrue(test_check.s3_connection.status_code == 404)
        self.assertTrue(test_check.get_latest_result() is None)
        self.assertTrue(test_check.get_primary_result() is None)
        with self.assertRaises(Exception) as exc:
            test_check.get_closest_result(1)
        self.assertTrue('Could not find any results' in str(exc.exception))
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

    def test_bad_ff_connection_in_fs_connection(self):
        # do not set test=True, should raise because it's not a real FF
        with self.assertRaises(Exception) as exc:
            bad_connection = fs_connection.FSConnection('test', self.environ_info)
        self.assertTrue('Could not initiate connection to Fourfront' in str(exc.exception))


class TestS3Connection(FSTest):
    environ = 'mastertest'
    conn = app_utils.init_connection(environ)

    def test_s3_conn_fields(self):
        s3_conn = self.conn.s3_connection
        self.assertTrue(s3_conn.bucket)
        self.assertTrue(s3_conn.location)
        self.assertTrue(s3_conn.status_code != 404)

    def test_test_s3_conn_methods(self):
        # clean up after yourself
        test_s3_conn = s3_connection.S3Connection('foursight-test-s3')
        test_key = 'test/' + ff_utils.generate_rand_accession()
        test_value = {'abc': 123}
        self.assertTrue(test_s3_conn.status_code != 404)
        put_res = test_s3_conn.put_object(test_key, json.dumps(test_value))
        self.assertTrue(put_res is not None)
        get_res = test_s3_conn.get_object(test_key)
        self.assertTrue(json.loads(get_res) == test_value)
        prefix_keys = test_s3_conn.list_all_keys_w_prefix('test/')
        self.assertTrue(len(prefix_keys) > 0)
        self.assertTrue(test_key in prefix_keys)
        all_keys = test_s3_conn.list_all_keys()
        self.assertTrue(len(all_keys) == len(prefix_keys))
        test_s3_conn.delete_keys(all_keys)
        # now there should be 0
        all_keys = test_s3_conn.list_all_keys()
        self.assertTrue(len(all_keys) == 0)


class TestAppRoutes(FSTest):
    environ = 'mastertest' # hopefully this is up
    conn = app_utils.init_connection(environ)

    def test_home_route(self):
        res = app.index()
        self.assertTrue(json.loads(res) == {'foursight': 'insight into fourfront'})

    def test_view_foursight(self):
        res = app_utils.view_foursight(self.environ) # not is_admin
        self.assertTrue(res.headers == {u'Content-Type': u'text/html'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.to_dict().keys()) == set(['body', 'headers', 'statusCode']))
        self.assertTrue('<!DOCTYPE html>' in res.body)
        self.assertTrue('Foursight' in res.body)
        self.assertTrue('Not logged in as admin.' in res.body)
        # run with a check
        res2 = app_utils.view_run_check(self.environ, 'indexing_progress', {})
        self.assertTrue(res2.status_code == 302)
        self.assertTrue('/api/view/' + self.environ + '/indexing_progress/' in res2.body)
        # running with an check brings you to that check
        # run with an action
        res3 = app_utils.view_run_action(self.environ, 'add_random_test_nums', {})
        self.assertTrue(res3.status_code == 302)
        self.assertTrue('/api/view/' + self.environ in res3.body)
        # lastly, check with is_admin
        res = app_utils.view_foursight(self.environ, True) # is_admin
        self.assertTrue(res.status_code == 200)
        self.assertTrue('Currently logged in as admin.' in res.body)

    def test_view_foursight_check(self):
        test_check_name = 'item_counts_by_type'
        test_check = utils.init_check_res(self.conn, test_check_name)
        uuid = test_check.get_primary_result()['uuid']
        res = app_utils.view_foursight_check(self.environ, test_check_name, uuid)
        self.assertTrue(res.status_code == 200)
        self.assertTrue('<!DOCTYPE html>' in res.body)
        self.assertTrue('Foursight' in res.body)

    def test_view_foursight_history(self):
        test_check = 'test_random_nums'
        res = app_utils.view_foursight_history(self.environ, test_check) # not admin
        self.assertTrue(res.headers == {u'Content-Type': u'text/html'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue('<!DOCTYPE html>' in res.body)
        self.assertTrue('Foursight' in res.body)
        self.assertTrue('Not logged in as admin.' in res.body)
        self.assertTrue('History for Test Random Nums (mastertest)' in res.body)
        self.assertTrue('<td>' in res.body)
        # run with bad environ
        res = app_utils.view_foursight_history('not_an_environment', test_check)
        self.assertTrue('<td>' not in res.body)
        # run with bad check
        res = app_utils.view_foursight_history(self.environ, 'not_a_check')
        self.assertTrue('<td>' not in res.body)
        # run with is_admin
        res = app_utils.view_foursight_history(self.environ, test_check, is_admin=True) # not admin
        self.assertTrue(res.status_code == 200)
        self.assertTrue('Currently logged in as admin.' in res.body)
        # run with some limits/starts
        res = app_utils.view_foursight_history(self.environ, test_check, start=4, limit=2)
        self.assertTrue(res.status_code == 200)
        self.assertTrue('Previous 2' in res.body)
        self.assertTrue('Next 2' in res.body)

    def test_get_foursight_history(self):
        test_check = 'test_random_nums'
        history = app_utils.get_foursight_history(self.conn, test_check, 0, 3)
        self.assertTrue(isinstance(history, list))
        self.assertTrue(len(history[0]) == 3)
        self.assertTrue(isinstance(history[0][0], utils.basestring))
        self.assertTrue(isinstance(history[0][1], dict))
        self.assertTrue('uuid' in history[0][1])
        self.assertTrue('primary' in history[0][1])
        first_uuid_1 = history[0][1]['uuid']
        second_uuid_1 = history[1][1]['uuid']
        self.assertTrue(len(history) == 3)
        # different start and limit
        history = app_utils.get_foursight_history(self.conn, test_check, 1, 4)
        first_uuid_2 = history[0][1]['uuid']
        self.assertTrue(first_uuid_1 != first_uuid_2)
        self.assertTrue(second_uuid_1 == first_uuid_2)
        self.assertTrue(len(history) == 4)
        # bad check
        bad_history = app_utils.get_foursight_history(self.conn, 'not_a_real_check', 0, 3)
        self.assertTrue(bad_history == [])

    def test_load_foursight_result(self):
        test_check = 'test_random_nums'
        check = utils.init_check_res(self.conn, test_check)
        check_res = check.get_latest_result()
        test_uuid = check_res['uuid']
        resp = app_utils.load_foursight_result(self.environ, test_check, test_uuid)
        self.assertTrue(resp.status_code == 200)
        self.assertTrue(resp.body.get('status') == 'success')
        self.assertTrue(resp.body.get('data') == check_res)
        # bad check
        resp = app_utils.load_foursight_result(self.environ, 'not_a_valid_check', test_uuid)
        self.assertTrue(resp.status_code == 400)
        self.assertTrue(resp.body.get('status') == 'error')
        self.assertTrue(resp.body.get('description') == 'Not a valid check or action.')

    def test_run_foursight_checks(self):
        res = app_utils.run_foursight_checks(self.environ, 'valid_test_checks')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'valid_test_checks')

    def test_get_foursight_checks(self):
        res = app_utils.get_foursight_checks(self.environ, 'valid_test_checks')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'valid_test_checks')
        self.assertTrue(isinstance(res.body['checks'], list) and len(res.body['checks']) > 0)

    def test_get_environment(self):
        environments = app_utils.init_environments()
        env_resp = app_utils.get_environment(self.environ)
        self.assertTrue(env_resp.status_code == 200)
        body = env_resp.body
        self.assertTrue(body.get('environment') == self.environ)
        self.assertTrue(body.get('status') == 'success')
        details = body.get('details')
        self.assertTrue(details.get('bucket').startswith('foursight-'))
        self.assertTrue(details.get('bucket').endswith(self.environ))
        this_env = environments.get(self.environ)
        self.assertTrue(this_env == details)
        # bad environment
        resp2 = app_utils.get_environment('not_an_environment')
        self.assertTrue(resp2.status_code == 400)
        self.assertTrue(resp2.body['status'] == 'error')
        self.assertTrue('Invalid environment provided' in resp2.body['description'])

    def test_put_environment(self):
        # this one is interesting... will be tested by putting a clone of
        # mastertest into itself. actual fxn run is run_put_environment
        get_res = app_utils.get_environment(self.environ)
        env_data = get_res.body.get('details')
        # make sure the environ we have is legit
        self.assertTrue(env_data and 'fourfront' in env_data and 'es' in env_data and 'ff_env' in env_data)
        env_res = app_utils.run_put_environment(self.environ, env_data)
        self.assertTrue(env_res.status_code == 200)
        self.assertTrue(env_res.body.get('status') == 'success')
        self.assertTrue(env_res.body.get('environment') == self.environ)
        self.assertTrue(env_res.body.get('description') == 'Succesfully made: ' + self.environ)
        # failure case
        bad_res = app_utils.run_put_environment(self.environ, {'key1': 'res1'})
        self.assertTrue(bad_res.status_code == 400)
        self.assertTrue(bad_res.body.get('status') == 'error')
        self.assertTrue(bad_res.body.get('body') == {'key1': 'res1'})
        self.assertTrue(bad_res.body.get('description') == 'Environment creation failed')
        # make sure they match after run_put_environment
        get_res2 = app_utils.get_environment(self.environ)
        self.assertTrue(get_res.body == get_res2.body)

    def test_get_check(self):
        test_check = 'indexing_progress'
        res = app_utils.get_check(self.environ, test_check)
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'checks_found']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(isinstance(res.body['checks'], dict) and res.body['checks']['name'] == test_check)
        self.assertTrue(res.body['checks_found'] == test_check)
        # bad response
        res = app_utils.get_check(self.environ, 'not_a_real_check')
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
            'admin_output': 'xyz',
            'uuid': ts_uuid
        }
        res = app_utils.run_put_check(self.environ, check_name, put_data)
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
        put_data['admin_output'] = '890'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_res = res.body['updated_content']
        self.assertTrue(put_res['brief_output'] == ['res1', 'res2'])
        self.assertTrue(put_res['full_output'] == {'key1': 'res1', 'key2': 'res3'})
        self.assertTrue(put_res['admin_output'] == 'xyz890')
        # now do it with strings. brief_output should be unchanged if we don't overwrite it
        del put_data['brief_output']
        put_data['full_output'] = 'abc '
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_data['full_output'] = '123'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 200)
        put_res = res.body['updated_content']
        self.assertTrue(put_res['brief_output'] == ['res1', 'res2'])
        self.assertTrue(put_res['full_output'] == 'abc 123')
        # lastly, cover bad output
        put_data = 'NOT_A_DICT'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        self.assertTrue(res.status_code == 400)
        self.assertTrue(res.body['status'] == 'error')
        self.assertTrue(res.body['description'] == 'PUT request is malformed: NOT_A_DICT')


class TestAppUtils(FSTest):
    """
    Meant for non-route utilities in chalicelib/app_utils.py
    """
    environ = 'mastertest' # hopefully this is up
    conn = app_utils.init_connection(environ)

    def test_init_connection(self):
        # test the fs connection
        self.assertTrue(self.conn.fs_env == 'mastertest')
        self.assertTrue(self.conn.s3_connection)
        # test the ff connection
        self.assertTrue(self.conn.ff_server)
        self.assertTrue(self.conn.ff_es)
        self.assertTrue(self.conn.ff_env == 'fourfront-mastertest')
        self.assertTrue(self.conn.ff_s3 is not None)
        self.assertTrue(isinstance(self.conn.ff_keys, dict))
        self.assertTrue({'key', 'secret', 'server'} <= set(self.conn.ff_keys.keys()))

    def test_init_bad_connection(self):
        with self.assertRaises(Exception) as exc:
            conn2 = app_utils.init_connection('not_an_environment')
        self.assertTrue('invalid environment provided' in str(exc.exception))

    def test_init_environments(self):
        environments = app_utils.init_environments() # default to 'all' environments
        self.assertTrue(self.environ in environments)
        for env, env_data in environments.items():
            self.assertTrue('fourfront' in env_data)
            self.assertTrue('es' in env_data)
            self.assertTrue('bucket' in env_data)
            self.assertTrue('ff_env' in env_data)
        environments = app_utils.init_environments('mastertest')
        self.assertTrue('mastertest' in environments)
        # bad environment
        bad_envs = app_utils.init_environments('not_an_environment')
        self.assertTrue(bad_envs == {})

    def test_init_response(self):
        # a good reponse
        connection, response = app_utils.init_response(self.environ)
        self.assertTrue(connection is not None)
        self.assertTrue(response.body == 'Foursight response')
        # a bad Response
        connection, response = app_utils.init_response('not_an_environment')
        self.assertTrue(connection is None)
        self.assertTrue(response.body != 'Foursight response')
        self.assertTrue(response.status_code == 400)

    def test_check_authorization(self):
        # first test with dev auth secret
        # should be admin authorization (return True)
        req_dict = {'headers': {'authorization': os.environ.get('DEV_SECRET')}}
        auth = app_utils.check_authorization(req_dict)
        self.assertTrue(auth)
        # try with a non-valid jwt
        # this should fully test app_utils.get_jwt
        req_dict = {'headers': {'cookie': 'jwtToken=not_a_jwt;other=blah;'}}
        auth = app_utils.check_authorization(req_dict)
        self.assertFalse(auth)
        jwtToken = app_utils.get_jwt(req_dict)
        self.assertTrue(jwtToken == 'not_a_jwt')
        # try with an empty dict
        auth = app_utils.check_authorization({})
        self.assertFalse(auth)

    def test_forbidden_response(self):
        res = app_utils.forbidden_response()
        self.assertTrue(res.status_code == 403)
        self.assertTrue(res.body == 'Forbidden. Login on the /api/view/<environ> page.')

    def test_process_response(self):
        response = chalice.Response(
            status_code = 200,
            body = "A reasonable body."
        )
        self.assertTrue(response == app_utils.process_response(response))
        # test for a response that's too long
        response.body = 'A' * 6000000
        too_long_resp = app_utils.process_response(response)
        self.assertTrue(too_long_resp.status_code == 413)
        self.assertTrue(too_long_resp.body == 'Body size exceeded 6 MB maximum. Try visiting /api/view/data.')

    def test_trim_output(self):
        short_output = {'some_field': 'some_value'}
        trimmed_short = app_utils.trim_output(short_output)
        self.assertTrue(trimmed_short == json.dumps(short_output, indent=4))
        long_output = {'some_field': 'some_value ' * 100000}
        trimmed_long = app_utils.trim_output(long_output)
        self.assertTrue(trimmed_long != json.dumps(long_output, indent=4))
        self.assertTrue(trimmed_long.endswith('\n\n... Output truncated ...'))

    def test_query_params_to_literals(self):
        test_params = {
            'primary': 'True',
            'bad_bool': 'false',
            'int': '12',
            'float': '12.1',
            'str': 'abc',
            'none_str': 'None',
            'empty_str': '',
            'special': '&limit=all'
        }
        literal_params = app_utils.query_params_to_literals(test_params)
        self.assertTrue(literal_params['primary'] == True)
        self.assertTrue(literal_params['bad_bool'] == 'false')
        self.assertTrue(literal_params['int'] == 12)
        self.assertTrue(literal_params['float'] == 12.1)
        self.assertTrue(literal_params['str'] == 'abc')
        self.assertTrue(literal_params['none_str'] is None)
        self.assertTrue('empty_str' not in literal_params)
        self.assertTrue(literal_params['special'] == '&limit=all')


class TestCheckRunner(FSTest):
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)
    # set up a queue for test checks
    utils.QUEUE_NAME = 'foursight-test-check_queue'
    queue = utils.get_sqs_queue()

    def test_run_check_runner(self):
        """
        Hard to test all the internal fxns here...
        Run with a check_group with dependencies
        """
        # the check we will test with
        check = run_result.CheckResult(self.connection.s3_connection, 'add_random_test_nums')
        prior_res = check.get_latest_result()
        # first, bad input
        bad_res = app_utils.run_check_runner({'sqs_url': None})
        self.assertTrue(bad_res is None)
        retries = 0
        test_success = False
        while retries < 3 and not test_success:
            # need to manually add things to the queue
            check_vals = check_utils.fetch_check_group('valid_test_checks')
            utils.send_sqs_messages(self.queue, self.environ, check_vals)
            app_utils.run_check_runner({'sqs_url': self.queue.url})
            finished_count = 0 # since queue attrs are approximate
            # wait for queue to empty
            while finished_count < 3:
                time.sleep(1)
                sqs_attrs = utils.get_sqs_attributes(self.queue.url)
                vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
                invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
                if vis_messages == 0 and invis_messages == 0:
                    finished_count += 1
            time.sleep(1)
            # look at output
            post_res = check.get_latest_result()
            if prior_res['uuid'] < post_res['uuid']:
                test_success = True
                self.assertTrue('_run_info' in post_res['kwargs'])
                self.assertTrue({'run_id', 'dep_id', 'receipt', 'sqs_url'} <= set(post_res['kwargs']['_run_info'].keys()))
            else:
                retries += 1
        self.assertTrue(test_success)

    def test_queue_check_group(self):
        # first, assure we have the right queue and runner names
        self.assertTrue(utils.QUEUE_NAME == 'foursight-test-check_queue')
        self.assertTrue(utils.RUNNER_NAME == 'foursight-dev-check_runner')
        # get a reference point for check results
        prior_res = check_utils.get_check_group_results(self.connection, 'all_checks', use_latest=True)
        run_input = app_utils.queue_check_group(self.environ, 'all_checks')
        self.assertTrue(utils.QUEUE_NAME in run_input.get('sqs_url'))
        finished_count = 0 # since queue attrs are approximate
        # wait for queue to empty
        while finished_count < 3:
            time.sleep(1)
            sqs_attrs = utils.get_sqs_attributes(run_input.get('sqs_url'))
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            if vis_messages == 0 and invis_messages == 0:
                finished_count += 1
        # queue should be empty. check results
        time.sleep(1)
        post_res = check_utils.get_check_group_results(self.connection, 'all_checks', use_latest=True)
        # compare the runtimes to ensure checks have run
        res_compare = {}
        for check_res in post_res:
            res_compare[check_res['name']] = {'post': check_res['uuid']}
        for check_res in prior_res:
            res_compare[check_res['name']]['prior'] = check_res['uuid']
        for check_name in res_compare:
            self.assertTrue('post' in res_compare[check_name] and 'prior' in res_compare[check_name])
            self.assertTrue(res_compare[check_name]['prior'] != res_compare[check_name]['post'])

    def test_get_sqs_attributes(self):
        # bad sqs url
        bad_sqs_attrs = utils.get_sqs_attributes('not_a_queue')
        self.assertTrue(bad_sqs_attrs.get('ApproximateNumberOfMessages') == bad_sqs_attrs.get('ApproximateNumberOfMessagesNotVisible') == 'ERROR')

    def test_record_and_collect_run_info(self):
        test_run_uuid = 'test_run_uuid'
        test_dep_id = 'xxxxx'
        resp = run_result.record_run_info(test_run_uuid, test_dep_id, 'PASS')
        self.assertTrue(resp is not None)
        found_ids = utils.collect_run_info(test_run_uuid)
        self.assertTrue(set([''.join([test_run_uuid, '/', test_dep_id])]) == found_ids)


class TestCheckResult(FSTest):
    # use a fake check name and store on mastertest
    check_name = 'test_only_check'
    # another fake check, with only ERROR results
    error_check_name = 'test_only_error_check'
    environ = 'mastertest' # hopefully this is up
    connection = app_utils.init_connection(environ)

    def test_check_result_methods(self):
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        # default status
        self.assertTrue(check.status == 'IGNORE')
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        # first store without uuid and primary kwargs; should be generated
        res = check.store_result()
        self.assertTrue('uuid' in res['kwargs'])
        self.assertTrue(res['kwargs']['primary'] == False)
        # set the kwargs and store again
        prime_uuid = datetime.datetime.utcnow().isoformat()
        check.kwargs = {'primary': True, 'uuid': prime_uuid}
        res = check.store_result()
        # fetch this check. latest and closest result with 0 diff should be the same
        late_res = check.get_latest_result()
        self.assertTrue(late_res == res)
        primary_res = check.get_primary_result()
        self.assertTrue(primary_res == res)
        # check get_closest_res without and with override_date
        close_res = check.get_closest_result(0, 0)
        self.assertTrue(close_res == res)
        override_res = check.get_closest_result(override_date=datetime.datetime.utcnow())
        self.assertTrue(override_res == res)
        all_res = check.get_all_results()
        self.assertTrue(len(all_res) > 0)
        # this should be true since all results will be identical
        self.assertTrue(all_res[-1].get('description') == res.get('description'))
        # ensure that previous check results can be fetch using the uuid functionality
        res_uuid = res['uuid']
        check_copy = run_result.CheckResult(self.connection.s3_connection, self.check_name, init_uuid=res_uuid)
        # should not have 'uuid' or 'kwargs' attrs with init_uuid
        self.assertTrue(getattr(check_copy, 'uuid', None) is None)
        self.assertTrue(getattr(check_copy, 'kwargs', {}) == {})
        check_copy.kwargs = {'primary': True, 'uuid': prime_uuid}
        self.assertTrue(res == check_copy.store_result())

    def test_get_closest_result(self):
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check.status = 'ERROR'
        res = check.store_result()
        err_uuid = res['uuid']
        closest_res_no_error = check.get_closest_result(diff_mins=0)
        self.assertTrue(err_uuid > closest_res_no_error['uuid'])
        check.status = 'PASS'
        res2 = check.store_result()
        pass_uuid = res2['uuid']
        closest_res_no_error = check.get_closest_result(diff_mins=0)
        self.assertTrue(pass_uuid == closest_res_no_error['uuid'])
        # bad cases: no results and all results are ERROR
        bad_check = run_result.CheckResult(self.connection.s3_connection, 'not_a_real_check')
        with self.assertRaises(Exception) as exc:
            bad_check.get_closest_result(diff_hours=0, diff_mins=0)
        self.assertTrue('Could not find any results' in str(exc.exception))
        error_check = run_result.CheckResult(self.connection.s3_connection, self.error_check_name)
        error_check.status = 'ERROR'
        error_check.store_result()
        with self.assertRaises(Exception) as exc:
            error_check.get_closest_result(diff_hours=0, diff_mins=0)
        self.assertTrue('Could not find closest non-ERROR result' in str(exc.exception))




class TestActionResult(FSTest):
    act_name = 'test_only_action'
    environ = 'mastertest' # hopefully this is up
    connection = app_utils.init_connection(environ)

    def test_action_result_methods(self):
        action = run_result.ActionResult(self.connection.s3_connection, self.act_name)
        res = action.store_result()
        self.assertTrue(res.get('status') == 'PEND')
        self.assertTrue(res.get('output') is None)
        self.assertTrue('uuid' in res.get('kwargs'))
        action.kwargs = {'do_not_store': True}
        unstored_res = action.store_result() # will not update latest result
        self.assertTrue('do_not_store' in unstored_res['kwargs'])
        res2 = action.get_latest_result()
        self.assertTrue(res == res2)
        # bad status
        action.kwargs = {'abc': 123}
        action.status = 'NOT_VALID'
        res = action.store_result()
        self.assertTrue(res.get('status') == 'FAIL')
        self.assertTrue(res.get('description') == 'Malformed status; look at Foursight action definition.')
        self.assertTrue(res['kwargs']['abc'] == 123)
        self.assertTrue('uuid' in res.get('kwargs'))


class TestCheckUtils(FSTest):
    environ = 'mastertest' # hopefully this is up
    connection = app_utils.init_connection(environ)

    def test_get_check_strings(self):
        # do this for every check
        all_check_strs = check_utils.get_check_strings()
        for check_str in all_check_strs:
            get_check = check_str.split('/')[1]
            chalice_resp = app_utils.run_get_check(self.environ, get_check)
            body = chalice_resp.body
            if body.get('status') == 'success':
                self.assertTrue(chalice_resp.status_code == 200)
                self.assertTrue(body.get('data', {}).get('name') == get_check)
                self.assertTrue(body.get('data', {}).get('status') in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'])
            elif body.get('status') == 'error':
                error_msg = "Not a valid check or action."
                self.assertTrue(body.get('description') == error_msg)
        # test a specific check
        one_check_str = check_utils.get_check_strings('indexing_progress')
        self.assertTrue(one_check_str == 'system_checks/indexing_progress')
        self.assertTrue(one_check_str in all_check_strs)
        # test a specific check that doesn't exist
        bad_check_str = check_utils.get_check_strings('not_a_real_check')
        self.assertTrue(bad_check_str is None)

    def test_validate_check_setup(self):
        self.assertTrue(check_utils.validate_check_setup(check_utils.CHECK_SETUP) == check_utils.CHECK_SETUP)
        # make sure modules were added
        for check in check_utils.CHECK_SETUP.values():
            self.assertTrue('module' in check)
        # do a while bunch of validation failure cases
        bad_setup = {'not_a_check': {}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('does not have a proper check function defined' in str(exc.exception))
        bad_setup = {'indexing_progress': []}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must be a dictionary' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': {}, 'group': {}, 'blah': {}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have the required keys' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': {}, 'group': {}, 'schedule': []}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a string value for field' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': []}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a dictionary value for field' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a list of "display" environments' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': []}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a dictionary value' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'not_an_env': []}}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('is not an existing environment' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': []}}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a dictionary value' in str(exc.exception))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {}}}}}
        with self.assertRaises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        self.assertTrue('must have a value for field "id"' in str(exc.exception))
        # this one will work -- display provided
        okay_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {}, 'display': ['data']}}
        okay_validated = check_utils.validate_check_setup(okay_setup)
        self.assertTrue(okay_validated['indexing_progress'].get('module') == 'system_checks')
        # this one adds kwargs and id to setup
        okay_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {'id': ''}}}}}
        okay_validated = check_utils.validate_check_setup(okay_setup)
        self.assertTrue({'id', 'kwargs', 'dependencies'} <= set(okay_validated['indexing_progress']['schedule']['fake_sched']['all'].keys()))


    def test_get_action_strings(self):
        all_action_strings = check_utils.get_action_strings()
        for act_str in all_action_strings:
            self.assertTrue(len(act_str.split('/')) == 2)
        # test a specific action
        one_act_str = check_utils.get_action_strings('patch_file_size')
        self.assertTrue(one_act_str == 'wrangler_checks/patch_file_size')
        self.assertTrue(one_act_str in all_action_strings)
        # test an action that doesn't exist
        bad_act_str = check_utils.get_check_strings('not_a_real_action')
        self.assertTrue(bad_act_str is None)

    def test_get_schedule_names(self):
        schedules = check_utils.get_schedule_names()
        self.assertTrue(isinstance(schedules, list))
        self.assertTrue(len(schedules) > 0)

    def test_get_check_title_from_setup(self):
        title = check_utils.get_check_title_from_setup('indexing_progress')
        self.assertTrue(title == check_utils.CHECK_SETUP['indexing_progress']['title'])

    def test_get_check_schedule(self):
        schedule = check_utils.get_check_schedule('morning_checks')
        self.assertTrue(len(schedule.keys()) > 0)
        for env in schedule:
            self.assertTrue(isinstance(schedule[env], list))
            for check_info in schedule[env]:
                assert len(check_info) == 4

    def test_get_check_results(self):
        # dict to compare uuids
        uuid_compares = {}
        # will get primary results by default
        all_res_primary = check_utils.get_check_results(self.connection)
        for check_res in all_res_primary:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
            self.assertTrue('uuid' in check_res)
            uuid_compares[check_res['name']] = check_res['uuid']
        # compare to latest results (which should be the same or newer)
        all_res_latest = check_utils.get_check_results(self.connection, use_latest=True)
        for check_res in all_res_latest:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
            self.assertTrue('uuid' in check_res)
            if check_res['name'] in uuid_compares:
                self.assertTrue(check_res['uuid'] >= uuid_compares[check_res['name']])
        # get a specific check
        one_res = check_utils.get_check_results(self.connection, checks=['indexing_progress'])
        self.assertTrue(len(one_res) == 1)
        self.assertTrue(one_res[0]['name'] == 'indexing_progress')
        # bad check name
        test_res = check_utils.get_check_results(self.connection, checks=['not_a_real_check'])
        self.assertTrue(len(test_res) == 0)

    def test_get_grouped_check_results(self):
        grouped_results = check_utils.get_grouped_check_results(self.connection)
        for group in grouped_results:
            self.assertTrue('_name' in group)
            self.assertTrue(isinstance(group['_statuses'], dict))
            self.assertTrue(len(group.keys()) > 2)

    def test_run_check_or_action(self):
        test_uuid = datetime.datetime.utcnow().isoformat()
        check = utils.init_check_res(self.connection, 'test_random_nums')
        # with a check (primary is True)
        test_info = ['test_checks/test_random_nums', {'primary': True, 'uuid': test_uuid}, [], 'xxx']
        check_res = check_utils.run_check_or_action(self.connection, test_info[0], test_info[1])
        self.assertTrue(isinstance(check_res, dict))
        self.assertTrue('name' in check_res)
        self.assertTrue('status' in check_res)
        # make sure runtime is in kwargs and pop it
        self.assertTrue('runtime_seconds' in check_res.get('kwargs'))
        check_res.get('kwargs').pop('runtime_seconds')
        self.assertTrue(check_res.get('kwargs') == {'primary': True, 'uuid': test_uuid})
        primary_uuid = check_res.get('uuid')
        time.sleep(5)
        primary_res = check.get_primary_result()
        self.assertTrue(primary_res.get('uuid') == primary_uuid)
        latest_res = check.get_latest_result()
        self.assertTrue(latest_res.get('uuid') == primary_uuid)
        # with a check and no primary=True flag
        check_res = check_utils.run_check_or_action(self.connection, test_info[0], {})
        latest_uuid = check_res.get('uuid')
        self.assertTrue('runtime_seconds' in check_res.get('kwargs'))
        check_res.get('kwargs').pop('runtime_seconds')
        self.assertTrue(check_res.get('kwargs') == {'primary': False, 'uuid': latest_uuid})
        time.sleep(5)
        # latest res will be more recent than primary res now
        latest_res = check.get_latest_result()
        self.assertTrue(latest_res.get('uuid') == latest_uuid)
        primary_res = check.get_primary_result()
        self.assertTrue(primary_uuid < latest_uuid)

        # with an action
        action = utils.init_action_res(self.connection, 'add_random_test_nums')
        test_info_2 = ['test_checks/add_random_test_nums', {'primary': True, 'uuid': test_uuid, 'called_by': latest_uuid}, [] ,'xxx']
        action_res = check_utils.run_check_or_action(self.connection, test_info_2[0], test_info_2[1])
        self.assertTrue(isinstance(action_res, dict))
        self.assertTrue('name' in action_res)
        self.assertTrue('status' in action_res)
        self.assertTrue('output' in action_res)
        # pop runtime_seconds kwarg
        self.assertTrue('runtime_seconds' in action_res['kwargs'])
        action_res['kwargs'].pop('runtime_seconds')
        self.assertTrue(action_res.get('kwargs') == {'primary': True, 'offset': 0, 'uuid': test_uuid, 'called_by': latest_uuid})
        latest_uuid = action_res.get('uuid')
        time.sleep(3)
        latest_res = action.get_latest_result()
        self.assertTrue(latest_res.get('uuid') == latest_uuid)
        output = latest_res.get('output')
        # output will differ for latest and primary res, since the checks differ
        self.assertTrue(output['latest'] != output['primary'])

    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', [], 'xx1'],
            ['syscks/indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/iteasdts_by_type', {}, [], 'xx1'],
            ['test_checks/test_function_unused', {}, [], 'xx1']
        ]
        for bad_check_info in bad_check_group:
            check_res = check_utils.run_check_or_action(self.connection, bad_check_info[0], bad_check_info[1])
            self.assertFalse(isinstance(check_res, dict))
            self.assertTrue('ERROR' in check_res)

    def test_run_check_exception(self):
        check_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_check_error', {})
        self.assertTrue(check_res['status'] == 'ERROR')
        # this output is a list
        self.assertTrue('by zero' in ''.join(check_res['full_output']))
        self.assertTrue(check_res['description'] == 'Check failed to run. See full output.')

    def test_run_action_no_called_by(self):
        action_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_action_error', {})
        self.assertTrue(action_res['status'] == 'FAIL')
        # this output is a list
        self.assertTrue('Action is missing called_by in its kwargs' in ''.join(action_res['output']))
        self.assertTrue(action_res['description'] == 'Action failed to run. See output.')

    def test_run_action_exception(self):
        action_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_action_error', {'called_by': None})
        self.assertTrue(action_res['status'] == 'FAIL')
        # this output is a list
        self.assertTrue('by zero' in ''.join(action_res['output']))
        self.assertTrue(action_res['description'] == 'Action failed to run. See output.')


class TestUtils(FSTest):
    environ = 'mastertest' # hopefully this is up
    conn = app_utils.init_connection(environ)
    timestr_1 = '2017-04-09T17:34:53.423589+00:00' # UTC
    timestr_2 = '2017-04-09T17:34:53.423589+05:00' # 5 hours ahead of UTC
    timestr_3 = '2017-04-09T17:34:53.423589-05:00' # 5 hours behind of UTC
    timestr_4 = '2017-04-09T17:34:53.423589'
    timestr_5 = '2017-04-09T17:34:53'
    timestr_bad_1 = '2017-04-0589+00:00'
    timestr_bad_2 = '2017-xxxxxT17:34:53.423589+00:00'
    timestr_bad_3 = '2017-xxxxxT17:34:53.423589'

    @utils.check_function(abc=123, do_not_store=True, uuid=datetime.datetime.utcnow().isoformat())
    def test_function_dummy(*args, **kwargs):
        connection = app_utils.init_connection('mastertest')
        check = utils.init_check_res(connection, 'not_a_check')
        return check

    def test_stage(self):
        self.assertTrue(utils.STAGE == 'dev')

    def test_check_timeout(self):
        self.assertTrue(isinstance(utils.CHECK_TIMEOUT, int))


    def test_check_times_out(self):
        # set to one second, which is slower than test check
        utils.CHECK_TIMEOUT = 1
        with self.assertRaises(SystemExit) as exc:
            check_utils.run_check_or_action(self.conn, 'test_checks/test_random_nums', {})
        self.assertTrue('-RUN-> TIMEOUT' in str(exc.exception))
        utils.CHECK_TIMEOUT = 280

    def test_list_environments(self):
        env_list = utils.list_environments()
        # assume we have at least one environments
        self.assertTrue(isinstance(env_list, list))
        self.assertTrue(self.environ in env_list)

    def test_check_function_deco_default_kwargs(self):
        # test to see if the check_function decorator correctly overrides
        # kwargs of decorated function if none are provided
        kwargs_default = self.test_function_dummy().get('kwargs')
        # pop runtime_seconds from here
        self.assertTrue('runtime_seconds' in kwargs_default)
        runtime = kwargs_default.pop('runtime_seconds')
        self.assertTrue(isinstance(runtime, float))
        self.assertTrue('_run_info' not in kwargs_default)
        uuid = kwargs_default.get('uuid')
        self.assertTrue(kwargs_default == {'abc': 123, 'do_not_store': True, 'uuid': uuid, 'primary': False})
        kwargs_add = self.test_function_dummy(bcd=234).get('kwargs')
        self.assertTrue('runtime_seconds' in kwargs_add)
        kwargs_add.pop('runtime_seconds')
        self.assertTrue(kwargs_add == {'abc': 123, 'bcd': 234, 'do_not_store': True, 'uuid': uuid, 'primary': False})
        kwargs_override = self.test_function_dummy(abc=234, primary=True).get('kwargs')
        self.assertTrue('runtime_seconds' in kwargs_override)
        kwargs_override.pop('runtime_seconds')
        self.assertTrue(kwargs_override == {'abc': 234, 'do_not_store': True, 'uuid': uuid, 'primary': True})

    def test_handle_kwargs(self):
        default_kwargs = {'abc': 123, 'bcd': 234}
        kwargs = utils.handle_kwargs({'abc': 345}, default_kwargs)
        self.assertTrue(kwargs.get('abc') == 345)
        self.assertTrue(kwargs.get('bcd') == 234)
        self.assertTrue(kwargs.get('uuid').startswith('20'))
        self.assertTrue(kwargs.get('primary') == False)

    def test_init_check_res(self):
        check = utils.init_check_res(self.conn, 'test_check')
        self.assertTrue(check.name == 'test_check')
        self.assertTrue(check.s3_connection is not None)

    def test_init_action_res(self):
        action = utils.init_action_res(self.conn, 'test_action')
        self.assertTrue(action.name == 'test_action')
        self.assertTrue(action.s3_connection is not None)

    def test_BadCheckOrAction(self):
        test_exc = utils.BadCheckOrAction()
        self.assertTrue(str(test_exc) == 'Check or action function seems to be malformed.')
        test_exc = utils.BadCheckOrAction('Abcd')
        self.assertTrue(str(test_exc) == 'Abcd')

    def test_validate_run_result(self):
        check = utils.init_check_res(self.conn, 'test_check')
        action = utils.init_action_res(self.conn, 'test_action')
        # bad calls
        with self.assertRaises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(action, is_check=True)
        self.assertTrue(str(exc.exception) == 'Check function must return a CheckResult object. Initialize one with init_check_res.')
        with self.assertRaises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(check, is_check=False)
        self.assertTrue(str(exc.exception) == 'Action functions must return a ActionResult object. Initialize one with init_action_res.')
        check.store_result = 'Not a fxn'
        with self.assertRaises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(check, is_check=True)
        self.assertTrue(str(exc.exception) == 'Do not overwrite the store_result method of the check or action result.')

    def parse_datetime_to_utc(self):
        [dt_tz_a, dt_tz_b, dt_tz_c] = ['None'] * 3
        for t_str in [self.timestr_1, self.timestr_2, self.timestr_3, self.timestr_4]:
            dt = utils.parse_datetime_to_utc(t_str)
            self.assertTrue(dt is not None)
            self.assertTrue(dt.tzinfo is not None and dt.tzinfo == tz.tzutc())
            if t_str == self.timestr_1:
                dt_tz_a = dt
            elif t_str == self.timestr_2:
                dt_tz_b = dt
            elif t_str == self.timestr_3:
                dt_tz_c = dt
        self.assertTrue(dt_tz_c > dt_tz_a > dt_tz_b)
        for bad_tstr in [self.timestr_bad_1, self.timestr_bad_2, self.timestr_bad_3]:
            dt_bad = utils.parse_datetime_to_utc(bad_tstr)
            self.assertTrue(dt_bad is None)
        # use a manual format
        dt_5_man = utils.parse_datetime_to_utc(self.timestr_5, manual_format="%Y-%m-%dT%H:%M:%S")
        dt_5_auto = utils.parse_datetime_to_utc(self.timestr_5)
        self.assertTrue(dt_5_auto == dt_5_man)

    def test_get_s3_utils(self):
        """
        Sanity test for s3 utils for all envs
        """
        environments = app_utils.init_environments()
        for env in environments:
            conn = app_utils.init_connection(env)
            s3_obj = s3_utils.s3Utils(env=conn.ff_env)
            self.assertTrue(s3_obj.sys_bucket is not None)
            self.assertTrue(s3_obj.outfile_bucket is not None)
            self.assertTrue(s3_obj.raw_file_bucket is not None)
            ff_keys = s3_obj.get_access_keys()
            self.assertTrue({'server', 'key', 'secret'} <= set(ff_keys.keys()))
            hg_keys = s3_obj.get_higlass_key()
            self.assertTrue({'server', 'key', 'secret'} <= set(hg_keys.keys()))


if __name__ == '__main__':
    unittest.main(warnings='ignore')
