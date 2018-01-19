from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import os
import time
import app
from chalicelib import app_utils, check_utils, utils, check_groups, wrangler_utils, run_result, fs_connection
from dateutil import tz


class TestFSConnection(unittest.TestCase):
    environ_info = {
        'fourfront': 'test1',
        'es': 'test2',
        'bucket': None,
        'ff_env': 'test3'
    }
    connection = fs_connection.FSConnection('test', environ_info)

    def test_connection_fields(self):
        self.assertTrue(self.connection.fs_environment == 'test')
        self.assertTrue(self.connection.s3_connection.status_code == 404)
        self.assertTrue(self.connection.ff == 'test1')
        self.assertTrue(self.connection.es == 'test2')
        self.assertTrue(self.connection.ff_env == 'test3')

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
        self.assertTrue(test_check.get_closest_result(1) is None)
        self.assertTrue(test_check.title == 'Test Check')
        formatted_res = test_check.format_result(datetime.datetime.utcnow())
        self.assertTrue(formatted_res.get('status') == 'IGNORE')
        self.assertTrue(formatted_res.get('title') == 'Test Check')
        self.assertTrue(formatted_res.get('description') == 'Unittest check')
        self.assertTrue(formatted_res.get('runnable') == False)
        # set a bad status on purpose
        test_check.status = "BAD_STATUS"
        check_res = test_check.store_result()
        self.assertTrue(check_res.get('name') == formatted_res.get('name'))
        self.assertTrue(check_res.get('description') == "Malformed status; look at Foursight check definition.")
        self.assertTrue(check_res.get('brief_output') == formatted_res.get('brief_output') == None)
        self.assertTrue(check_res.get('ff_link') == 'not_a_real_http_link')


class TestAppRoutes(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app_utils.init_connection(environ)

    def test_stage(self):
        self.assertTrue(app_utils.STAGE == 'dev')

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
        self.assertTrue('admin_output' not in res.body)
        # this is pretty weak
        res2 = app_utils.view_rerun(self.environ, 'indexing_progress')
        self.assertTrue(res.status_code == 200)
        self.assertTrue('<!DOCTYPE html>' in res.body)
        self.assertTrue('Foursight' in res.body)
        self.assertTrue(res.body != res2.body)
        # lastly, check with is_admin
        res = app_utils.view_foursight(self.environ, True) # is_admin
        self.assertTrue(res.status_code == 200)
        self.assertTrue('admin_output' not in res.body)

    def test_run_foursight_checks(self):
        res = app_utils.run_foursight_checks(self.environ, 'all')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'all')

    def test_get_foursight_checks(self):
        res = app_utils.get_foursight_checks(self.environ, 'all')
        self.assertTrue(res.status_code == 200)
        self.assertTrue(set(res.body.keys()) == set(['status', 'environment', 'checks', 'check_group']))
        self.assertTrue(res.body['environment'] == self.environ)
        self.assertTrue(res.body['status'] == 'success')
        self.assertTrue(res.body['check_group'] == 'all')
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


class TestAppUtils(unittest.TestCase):
    """
    Meant for non-route utilities in chalicelib/app_utils.py
    """
    environ = 'mastertest' # hopefully this is up
    conn, _ = app_utils.init_connection(environ)

    def test_init_connection(self):
        self.assertFalse(self.conn is None)
        # test the ff connection
        self.assertTrue(self.conn.fs_environment == 'mastertest')
        self.assertTrue(self.conn.ff)
        self.assertTrue(self.conn.es)
        self.assertTrue(self.conn.ff_env == 'fourfront-mastertest')

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

    def test_list_environments(self):
        env_list = app_utils.list_environments()
        # assume we have at least one environments
        self.assertTrue(isinstance(env_list, list))
        self.assertTrue(self.environ in env_list)

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


class TestCheckRunner(unittest.TestCase):
    environ = 'mastertest'
    connection, _ = app_utils.init_connection(environ)

    def test_run_check_runner(self):
        """
        Hard to test all the internal fxns here...
        Run with wrangler_test_checks check_group that gives a unique output
        """
        # the check we will test with
        check = run_result.CheckResult(self.connection.s3_connection, 'items_created_in_the_past_day')
        prior_res = check.get_latest_result()
        # first, bad input
        bad_res = app_utils.run_check_runner({'sqs_url': None})
        self.assertTrue(bad_res is None)
        # need to manually add things to the queue
        queue = app_utils.get_sqs_queue()
        check_vals = check_utils.fetch_check_group('wrangler_test_checks')
        app_utils.send_sqs_messages(queue, self.environ, check_vals)
        app_utils.run_check_runner({'sqs_url': queue.url})
        # this **should** work
        sqs_attrs = app_utils.get_sqs_attributes(queue.url)
        vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
        invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
        self.assertTrue(vis_messages > 0 or invis_messages > 0)
        # wait for queue to empty
        while vis_messages > 0 or invis_messages > 0:
            sqs_attrs = app_utils.get_sqs_attributes(queue.url)
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            time.sleep(2)
        # look at output
        post_res = check.get_latest_result()
        prior_uuid = datetime.datetime.strptime(prior_res['uuid'], "%Y-%m-%dT%H:%M:%S.%f")
        post_uuid = datetime.datetime.strptime(post_res['uuid'], "%Y-%m-%dT%H:%M:%S.%f")
        self.assertTrue(post_uuid > prior_uuid)

    def test_queue_check_group(self):
        # first, assure we have the right queue and runner names
        self.assertTrue(app_utils.QUEUE_NAME == 'foursight-dev-check_queue')
        self.assertTrue(app_utils.RUNNER_NAME == 'foursight-dev-check_runner')
        # get a reference point for check results
        prior_res = check_utils.get_check_group_latest(self.connection, 'all')
        run_input = app_utils.queue_check_group(self.environ, 'all')
        self.assertTrue(app_utils.QUEUE_NAME in run_input.get('sqs_url'))
        # this **should** work
        sqs_attrs = app_utils.get_sqs_attributes(run_input.get('sqs_url'))
        vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
        invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
        self.assertTrue(vis_messages > 0 or invis_messages > 0)
        # wait for queue to empty
        while vis_messages > 0 or invis_messages > 0:
            sqs_attrs = app_utils.get_sqs_attributes(run_input.get('sqs_url'))
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            time.sleep(2)
        # queue should be empty. check results
        post_res = check_utils.get_check_group_latest(self.connection, 'all')
        # compare the runtimes to ensure checks have run
        res_compare = {}
        for check_res in post_res:
            res_compare[check_res['name']] = {'post': check_res['uuid']}
        for check_res in prior_res:
            res_compare[check_res['name']]['prior'] = check_res['uuid']
        for check_name in res_compare:
            self.assertTrue('post' in res_compare[check_name] and 'prior' in res_compare[check_name])
            prior_uuid = datetime.datetime.strptime(res_compare[check_name]['prior'], "%Y-%m-%dT%H:%M:%S.%f")
            post_uuid = datetime.datetime.strptime(res_compare[check_name]['post'], "%Y-%m-%dT%H:%M:%S.%f")
            self.assertTrue(post_uuid > prior_uuid)

    def test_queue_action_group(self):
        # get a reference point for action results
        action = utils.init_action_res(self.connection, 'add_random_test_nums')
        prior_res = action.get_latest_result()
        run_input = app_utils.queue_check_group(self.environ, 'add_random_test_nums', use_action_group=True)
        self.assertTrue(app_utils.QUEUE_NAME in run_input.get('sqs_url'))
        # this **should** work
        sqs_attrs = app_utils.get_sqs_attributes(run_input.get('sqs_url'))
        vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
        invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
        self.assertTrue(vis_messages > 0 or invis_messages > 0)
        # wait for queue to empty
        while vis_messages > 0 or invis_messages > 0:
            sqs_attrs = app_utils.get_sqs_attributes(run_input.get('sqs_url'))
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            time.sleep(2)
        # queue should be empty. check results
        post_res = action.get_latest_result()
        # compare the runtimes to ensure actions have run
        prior_uuid = datetime.datetime.strptime(prior_res['uuid'], "%Y-%m-%dT%H:%M:%S.%f")
        post_uuid = datetime.datetime.strptime(post_res['uuid'], "%Y-%m-%dT%H:%M:%S.%f")
        self.assertTrue(post_uuid > prior_uuid)

    def test_get_sqs_attributes(self):
        # bad sqs url
        bad_sqs_attrs = app_utils.get_sqs_attributes('not_a_queue')
        self.assertTrue(bad_sqs_attrs.get('ApproximateNumberOfMessages') == bad_sqs_attrs.get('ApproximateNumberOfMessagesNotVisible') == 'ERROR')

    def test_record_and_collect_run_info(self):
        test_run_uuid = 'test_run_uuid'
        test_dep_id = 'xxxxx'
        resp = app_utils.record_run_info(test_run_uuid, test_dep_id, 'PASS')
        self.assertTrue(resp is not None)
        found_ids = app_utils.collect_run_info(test_run_uuid)
        self.assertTrue(set([''.join([test_run_uuid, '/', test_dep_id])]) == found_ids)


class TestCheckResult(unittest.TestCase):
    # use a fake check name and store on mastertest
    check_name = 'test_only_check'
    environ = 'mastertest' # hopefully this is up
    connection, _ = app_utils.init_connection(environ)

    def test_check_result_methods(self):
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        # default status
        self.assertTrue(check.status == 'IGNORE')
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.full_output = ['first_item']
        res = check.store_result()
        # fetch this check. latest and closest result with 0 diff should be the same
        late_res = check.get_latest_result()
        self.assertTrue(late_res == res)
        close_res = check.get_closest_result(0, 0)
        self.assertTrue(close_res == res)
        all_res = check.get_all_results()
        self.assertTrue(len(all_res) > 0)
        # this should be true since all results will be identical
        self.assertTrue(all_res[-1].get('description') == res.get('description'))
        # ensure that previous check results can be fetch using the uuid functionality
        res_uuid = res['uuid']
        check_copy = run_result.CheckResult(self.connection.s3_connection, self.check_name, uuid=res_uuid)
        self.assertTrue(res == check_copy.store_result())


class TestCheckUtils(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app_utils.init_connection(environ)

    def test_get_check_strings(self):
        # do this for every check
        all_check_strs = check_utils.get_check_strings()
        for check_str in all_check_strs:
            get_check = check_str.split('/')[1]
            chalice_resp = app_utils.get_check(self.environ, get_check)
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

    def test_fetch_action_group(self):
        patch_actions = check_utils.fetch_action_group('patch_file_size')
        self.assertTrue(isinstance(patch_actions, list) and len(patch_actions) > 0)
        test_actions = check_utils.fetch_action_group('add_random_test_nums')
        self.assertTrue(isinstance(test_actions, list) and len(test_actions) > 0)
        # non-existant action group
        bad_actions = check_utils.fetch_action_group('not_an_action_group')
        self.assertTrue(bad_actions is None)

    def test_run_check_group(self):
        """
        This test will need to be removed/changed as more checks are made
        """
        all_checks_res = check_utils.run_check_group(self.conn, 'all')
        self.assertTrue(isinstance(all_checks_res, list) and len(all_checks_res) > 0)
        for check_res in all_checks_res:
            self.assertTrue(isinstance(check_res, dict))
            self.assertTrue('name' in check_res)
            self.assertTrue('status' in check_res)
            self.assertTrue('uuid' in check_res)
            # assert the check actually ran
            self.assertTrue(check_res.get('description') != 'Check failed to run. See full output.')
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

    def test_run_check_or_action(self):
        # with a check
        test_info = ['test_checks/test_random_nums', {}, [], 'xxx']
        check_res = check_utils.run_check_or_action(self.conn, test_info[0], test_info[1])
        self.assertTrue(isinstance(check_res, dict))
        self.assertTrue('name' in check_res)
        self.assertTrue('status' in check_res)
        # with an action
        test_info_2 = ['test_checks/add_random_test_nums', {}, [] ,'xxx']
        action_res = check_utils.run_check_or_action(self.conn, test_info_2[0], test_info_2[1])
        self.assertTrue(isinstance(action_res, dict))
        self.assertTrue('name' in action_res)
        self.assertTrue('status' in action_res)
        self.assertTrue('output' in action_res)

    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', [], 'xx1'],
            ['syscks/indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/iteasdts_by_type', {}, [], 'xx1'],
            ['test_checks/test_function_unused', {}, [], 'xx1']
        ]
        for bad_check_info in bad_check_group:
            check_res = check_utils.run_check_or_action(self.conn, bad_check_info[0], bad_check_info[1])
            self.assertFalse(isinstance(check_res, dict))
            self.assertTrue('ERROR' in check_res)

    def test_run_check_exception(self):
        check_res = check_utils.run_check_or_action(self.conn, 'test_checks/test_check_error', {})
        self.assertTrue(check_res['status'] == 'ERROR')
        # this output is a list
        self.assertTrue('by zero' in ''.join(check_res['full_output']))
        self.assertTrue(check_res['description'] == 'Check failed to run. See full output.')

    def test_run_action_exception(self):
        action_res = check_utils.run_check_or_action(self.conn, 'test_checks/test_action_error', {})
        self.assertTrue(action_res['status'] == 'FAIL')
        # this output is a list
        self.assertTrue('by zero' in ''.join(action_res['output']))
        self.assertTrue(action_res['description'] == 'Action failed to run. See output.')

class TestCheckGroup(unittest.TestCase):
    def test_check_groups(self):
        # make sure check groups are dicts
        self.assertTrue(isinstance(check_groups.CHECK_GROUPS, dict))
        self.assertTrue(isinstance(check_groups.TEST_CHECK_GROUPS, dict))
        # ensure check groups look good
        dependency_ids = []
        used_check_mods = []
        for key, val in check_groups.CHECK_GROUPS.items():
            self.assertTrue('_checks' in key)
            self.assertTrue(isinstance(val, list))
            within_group_dep_ids = []
            used_dep_ids = []
            for check_info in val:
                self.assertTrue(len(check_info) == 4)
                self.assertTrue(isinstance(check_info[0], app_utils.basestring))
                self.assertTrue(len(check_info[0].split('/')) == 2)
                used_check_mods.append(check_info[0].split('/')[0].strip())
                self.assertTrue(isinstance(check_info[1], dict))
                self.assertTrue(isinstance(check_info[2], list))
                used_dep_ids.extend(check_info[2])
                self.assertTrue(isinstance(check_info[3], app_utils.basestring))
                within_group_dep_ids.append(check_info[3])
            dependency_ids.extend(within_group_dep_ids)
            # ensure all ids within a group are unique
            within_group_unique = list(set(within_group_dep_ids))
            self.assertTrue(len(within_group_unique) == len(within_group_dep_ids))
            # ensure all dep ids used in this group belong to the group
            self.assertTrue(set(used_dep_ids).issubset(set(within_group_dep_ids)))
        # ensure all dependency ids are unique
        dependency_ids_unique = list(set(dependency_ids))
        self.assertTrue(len(dependency_ids_unique) == len(dependency_ids))
        # ensure all the used check modules are added to CHECK_MODULES
        for mod in used_check_mods:
            self.assertTrue(mod in check_groups.CHECK_MODULES)

        # this is a bit janky
        for key, val in check_groups.TEST_CHECK_GROUPS.items():
            self.assertTrue('_test_checks' in key)
            self.assertTrue(isinstance(val, list))
            for check_info in val:
                if 'malformed' in key:
                    self.assertTrue(len(check_info) != 3)
                else:
                    self.assertTrue(len(check_info) == 4)
                    self.assertTrue(isinstance(check_info[0], app_utils.basestring))
                    self.assertTrue(isinstance(check_info[1], dict))
                    self.assertTrue(isinstance(check_info[2], list))
                    self.assertTrue(isinstance(check_info[3], app_utils.basestring))


class TestActionGroups(unittest.TestCase):
    def test_action_groups_content(self):
        # verify all names of action groups are functions with the
        # @action_function deco AND all checks/actions in the group are valid.
        for action_group in check_groups.ACTION_GROUPS:
            actions = []
            self.assertTrue(isinstance(check_groups.ACTION_GROUPS[action_group], list))
            for entry in check_groups.ACTION_GROUPS[action_group]:
                entry_string = entry[0]
                self.assertTrue(isinstance(entry_string, app_utils.basestring))
                self.assertTrue(len(entry_string.split('/')) == 2)
                [mod, name] = entry_string.split('/')
                is_check = False
                is_action = False
                check_mod = check_utils.__dict__.get(mod)
                self.assertTrue(check_mod is not None)
                method = check_mod.__dict__.get(name)
                self.assertTrue(method is not None)
                if utils.check_method_deco(method, utils.CHECK_DECO):
                    is_check = True
                elif utils.check_method_deco(method, utils.ACTION_DECO):
                    is_action = True
                    actions.append(name)
                self.assertTrue(is_check or is_action)
            # ensure action_group name matches an action in the group
            self.assertTrue(action_group in actions)


class TestUtils(unittest.TestCase):
    environ = 'mastertest' # hopefully this is up
    conn, _ = app_utils.init_connection(environ)

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

    def test_init_check_res(self):
        check = utils.init_check_res(self.conn, 'test_check', runnable=True)
        self.assertTrue(check.name == 'test_check')
        self.assertTrue(check.s3_connection is not None)
        self.assertTrue(check.runnable == True)

    def test_init_action_res(self):
        action = utils.init_action_res(self.conn, 'test_action')
        self.assertTrue(action.name == 'test_action')
        self.assertTrue(action.s3_connection is not None)


class TestWranglerUtils(unittest.TestCase):
    timestr_1 = '2017-04-09T17:34:53.423589+00:00' # UTC
    timestr_2 = '2017-04-09T17:34:53.423589+05:00' # 5 hours ahead of UTC
    timestr_3 = '2017-04-09T17:34:53.423589-05:00' # 5 hours behind of UTC
    timestr_4 = '2017-04-09T17:34:53.423589'
    timestr_5 = '2017-04-09T17:34:53'
    timestr_bad_1 = '2017-04-0589+00:00'
    timestr_bad_2 = '2017-xxxxxT17:34:53.423589+00:00'
    timestr_bad_3 = '2017-xxxxxT17:34:53.423589'

    def test_parse_datetime_with_tz_to_utc(self):
        [dt_tz_a, dt_tz_b, dt_tz_c] = ['None'] * 3
        for t_str in [self.timestr_1, self.timestr_2, self.timestr_3, self.timestr_4, self.timestr_5]:
            dt = wrangler_utils.parse_datetime_with_tz_to_utc(t_str)
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
            dt_bad = wrangler_utils.parse_datetime_with_tz_to_utc(bad_tstr)
            self.assertTrue(dt_bad is None)

    def test_get_s3_utils_obj(self):
        environments = app_utils.init_environments()
        for env in environments:
            conn, _ = app_utils.init_connection(env)
            s3_obj = wrangler_utils.get_s3_utils_obj(conn)
            self.assertTrue(s3_obj.sys_bucket is not None)
            self.assertTrue(s3_obj.outfile_bucket is not None)
            self.assertTrue(s3_obj.raw_file_bucket is not None)

    def test_get_FDN_connection(self):
        # run this for all environments to ensure access keys are in place
        environments = app_utils.init_environments()
        for env in environments:
            conn, _ = app_utils.init_connection(env)
            fdn_conn = wrangler_utils.get_FDN_connection(conn)
            self.assertTrue(fdn_conn is not None)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
