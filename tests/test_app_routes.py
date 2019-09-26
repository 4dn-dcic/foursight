from conftest import *

class TestAppRoutes():
    environ = 'mastertest' # hopefully this is up
    conn = app_utils.init_connection(environ)

    def test_view_foursight(self):
        res = app_utils.view_foursight(self.environ) # not is_admin
        assert (res.headers == {u'Content-Type': u'text/html'})
        assert (res.status_code == 200)
        assert (set(res.to_dict().keys()) == set(['body', 'headers', 'statusCode', 'multiValueHeaders']))
        assert ('<!DOCTYPE html>' in res.body)
        assert ('Foursight' in res.body)
        assert  ('Not logged in as admin' in res.body)
        # run a check, which redirects to future check result
        res2 = app_utils.view_run_check(self.environ, 'indexing_progress', {})
        assert (res2.status_code == 302)
        assert ('/view/' + self.environ + '/indexing_progress/' in res2.body)
        # get check uuid from res location
        chk_uuid = res2.headers['Location'].split('/')[-1]
        # running action w/ an check brings you to the action bound to a check
        act_kwargs = {'check_name': 'indexing_progress', 'called_by': chk_uuid}
        res3 = app_utils.view_run_action(self.environ, 'add_random_test_nums', act_kwargs)
        assert (res3.status_code == 302)
        assert (res3.body == res2.body)
        # running action w/o check info gives 200 with action info
        res4 = app_utils.view_run_action(self.environ, 'add_random_test_nums', {})
        assert (res4.status_code == 200)
        assert ('Action is queued.' in res4.body['details'])
        res = app_utils.view_foursight(self.environ, True) # is_admin
        assert (res.status_code == 200)
        assert ('Currently logged in as admin' in res.body)

    def test_view_foursight_check(self):
        test_check_name = 'item_counts_by_type'
        test_check = utils.init_check_res(self.conn, test_check_name)
        uuid = test_check.get_primary_result()['uuid']
        res = app_utils.view_foursight_check(self.environ, test_check_name, uuid)
        assert (res.status_code == 200)
        assert ('<!DOCTYPE html>' in res.body)
        assert ('Foursight' in res.body)

    def test_view_foursight_history(self):
        test_check = 'test_random_nums'
        res = app_utils.view_foursight_history(self.environ, 'indexing_progress') # not admin
        assert (res.headers == {u'Content-Type': u'text/html'})
        assert (res.status_code == 200)
        assert ('<!DOCTYPE html>' in res.body)
        assert ('Foursight' in res.body)
        assert ('Not logged in as admin' in res.body)
        assert ('History for Indexing progress (mastertest)' in res.body)
        assert ('<td>' in res.body)
        # run with bad environ
        res = app_utils.view_foursight_history('not_an_environment', test_check)
        assert ('<td>' not in res.body)
        # run with bad check
        res = app_utils.view_foursight_history(self.environ, 'not_a_check')
        assert ('<td>' not in res.body)
        # run with is_admin
        res = app_utils.view_foursight_history(self.environ, test_check, is_admin=True) # not admin
        assert (res.status_code == 200)
        assert ('Currently logged in as admin' in res.body)
        # run with some limits/starts
        res = app_utils.view_foursight_history(self.environ, test_check, start=4, limit=2)
        assert (res.status_code == 200)
        assert ('Previous 2' in res.body)
        assert ('Next 2' in res.body)

    def test_get_foursight_history(self):
        test_check = 'test_random_nums'
        history = app_utils.get_foursight_history(self.conn, test_check, 0, 3)
        assert (isinstance(history, list))
        assert (len(history[0]) == 4)
        assert (isinstance(history[0][0], utils.basestring))
        assert (isinstance(history[0][1], utils.basestring) or history[0][1] is None)
        assert (isinstance(history[0][2], dict))
        assert ('uuid' in history[0][2])
        assert ('primary' in history[0][2])
        assert (history[0][3] is True)
        first_uuid_1 = history[0][2]['uuid']
        second_uuid_1 = history[1][2]['uuid']
        assert (len(history) == 3)
        # different start and limit
        history = app_utils.get_foursight_history(self.conn, test_check, 1, 4)
        first_uuid_2 = history[0][2]['uuid']
        assert (first_uuid_1 != first_uuid_2)
        assert (second_uuid_1 == first_uuid_2)
        assert (len(history) == 4)
        # bad check
        bad_history = app_utils.get_foursight_history(self.conn, 'not_a_real_check', 0, 3)
        assert (bad_history == [])

    def test_run_get_environment(self):
        environments = app_utils.init_environments()
        env_resp = app_utils.run_get_environment(self.environ)
        assert (env_resp.status_code == 200)
        body = env_resp.body
        assert (body.get('environment') == self.environ)
        assert (body.get('status') == 'success')
        details = body.get('details')
        assert (details.get('bucket').startswith('foursight-'))
        assert (details.get('bucket').endswith(self.environ))
        this_env = environments.get(self.environ)
        assert (this_env == details)
        # bad environment
        resp2 = app_utils.run_get_environment('not_an_environment')
        assert (resp2.status_code == 400)
        assert (resp2.body['status'] == 'error')
        assert ('Invalid environment provided' in resp2.body['description'])

    def test_put_environment(self):
        # this one is interesting... will be tested by putting a clone of
        # mastertest into itself. actual fxn run is run_put_environment
        get_res = app_utils.run_get_environment(self.environ)
        env_data = get_res.body.get('details')
        # make sure the environ we have is legit
        assert (env_data and 'fourfront' in env_data and 'es' in env_data and 'ff_env' in env_data)
        env_res = app_utils.run_put_environment(self.environ, env_data)
        assert (env_res.status_code == 200)
        assert (env_res.body.get('status') == 'success')
        assert (env_res.body.get('environment') == self.environ)
        assert (env_res.body.get('description') == 'Succesfully made: ' + self.environ)
        # failure case
        bad_res = app_utils.run_put_environment(self.environ, {'key1': 'res1'})
        assert (bad_res.status_code == 400)
        assert (bad_res.body.get('status') == 'error')
        assert (bad_res.body.get('body') == {'key1': 'res1'})
        assert (bad_res.body.get('description') == 'Environment creation failed')
        # make sure they match after run_put_environment
        get_res2 = app_utils.run_get_environment(self.environ)
        assert (get_res.body == get_res2.body)

    def test_run_get_check(self):
        test_check = 'indexing_progress'
        res = app_utils.run_get_check(self.environ, test_check)
        assert (res.status_code == 200)
        assert (set(res.body.keys()) == set(['status', 'data']))
        assert (res.body['status'] == 'success')
        assert (isinstance(res.body['data'], dict) and res.body['data']['name'] == test_check)
        # bad response
        res = app_utils.run_get_check(self.environ, 'not_a_real_check')
        assert (res.status_code == 400)
        assert (res.body['status'] == 'error')
        assert (res.body['description'] == 'Not a valid check or action.')

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
        assert (res.status_code == 200)
        assert (res.body['environment'] == self.environ)
        assert (res.body['status'] == 'success')
        assert (res.body['check'] == check_name)
        put_res = res.body['updated_content']
        assert (put_res is not None)
        assert (put_res.get('uuid') == ts_uuid)
        # now put another one with the same uuid
        put_data['brief_output'] = ['res2']
        put_data['full_output'] = {'key2': 'res3'}
        put_data['admin_output'] = '890'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        assert (res.status_code == 200)
        put_res = res.body['updated_content']
        assert (put_res['brief_output'] == ['res1', 'res2'])
        assert (put_res['full_output'] == {'key1': 'res1', 'key2': 'res3'})
        assert (put_res['admin_output'] == 'xyz890')
        # now do it with strings. brief_output should be unchanged if we don't overwrite it
        del put_data['brief_output']
        put_data['full_output'] = 'abc '
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        assert (res.status_code == 200)
        put_data['full_output'] = '123'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        assert (res.status_code == 200)
        put_res = res.body['updated_content']
        assert (put_res['brief_output'] == ['res1', 'res2'])
        assert (put_res['full_output'] == 'abc 123')
        # lastly, cover bad output
        put_data = 'NOT_A_DICT'
        res = app_utils.run_put_check(self.environ, check_name, put_data)
        assert (res.status_code == 400)
        assert (res.body['status'] == 'error')
        assert (res.body['description'] == 'PUT request is malformed: NOT_A_DICT')
