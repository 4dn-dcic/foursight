from conftest import *
import json
import boto3
from botocore.exceptions import ClientError

class TestAppUtils():
    """
    Meant for non-route utilities in chalicelib/app_utils.py
    """
    environ = 'mastertest' # hopefully this is up
    conn = app_utils.init_connection(environ)

    def test_init_connection(self):
        # test the fs connection
        assert (self.conn.fs_env == 'mastertest')
        assert (self.conn.connections)
        # test the ff connection
        assert (self.conn.ff_server)
        assert (self.conn.ff_es)
        assert (self.conn.ff_env == 'fourfront-mastertest')
        assert (self.conn.ff_s3 is not None)
        assert (isinstance(self.conn.ff_keys, dict))
        assert ({'key', 'secret', 'server'} <= set(self.conn.ff_keys.keys()))

    def test_get_favicon(self):
        """ Tests that given 'mastertest' we get the right url for favicon """
        expected = self.conn.ff_server + 'static/img/favicon-fs.ico'
        actual = app_utils.get_favicon(self.conn.ff_server)
        assert expected == actual

    def test_init_bad_connection(self):
        with pytest.raises(Exception) as exc:
            app_utils.init_connection('not_an_environment')
        assert ('is not valid!' in str(exc.value))

    def test_bad_view_result(self):
        """ Tests giving a bad response to process_view_result """
        res = 'a string, not a dict response'
        error = app_utils.process_view_result(self.conn, res, False)
        assert error['status'] == 'ERROR'

    def test_init_environments(self):
        environments = app_utils.init_environments() # default to 'all' environments
        assert (self.environ in environments)
        for env, env_data in environments.items():
            assert ('fourfront' in env_data)
            assert ('es' in env_data)
            assert ('bucket' in env_data)
            assert ('ff_env' in env_data)
        environments = app_utils.init_environments('mastertest')
        assert ('mastertest' in environments)
        # bad environment
        bad_envs = app_utils.init_environments('not_an_environment')
        assert (bad_envs == {})

    def test_init_response(self):
        # a good reponse
        connection, response = app_utils.init_response(self.environ)
        assert (connection is not None)
        assert (response.body == 'Foursight response')
        # a bad Response
        connection, response = app_utils.init_response('not_an_environment')
        assert (connection is None)
        assert (response.body != 'Foursight response')
        assert (response.status_code == 400)

    def test_check_authorization(self):
        # first test with dev auth secret
        # should be admin authorization (return True)
        req_dict = {'headers': {'authorization': os.environ.get('DEV_SECRET')}}
        auth = app_utils.check_authorization(req_dict)
        assert auth
        # try with a non-valid jwt
        # this should fully test app_utils.get_jwt
        req_dict = {'headers': {'cookie': 'jwtToken=not_a_jwt;other=blah;'}}
        auth = app_utils.check_authorization(req_dict)
        assert not auth
        jwtToken = app_utils.get_jwt(req_dict)
        assert (jwtToken == 'not_a_jwt')
        # try with an empty dict
        auth = app_utils.check_authorization({})
        assert not auth

    def test_check_jwt_authorization(self):
        """ Tests same functionality as above except with a valid jwt """
        from unittest import mock
        payload1 = {
            "email": "carl_vitzthum@hms.harvard.edu",
            "email_verified": True,
            "sub": "1234567890",
            "name": "Dummy",
            "iat": 1516239022
        }  # mock a 'correct' jwt decode
        with mock.patch('chalicelib.app_utils.get_jwt', return_value='token'):
            with mock.patch('jwt.decode', return_value=payload1):
                auth = app_utils.check_authorization({}, env='mastertest')
            assert auth
        with mock.patch('chalicelib.app_utils.get_jwt', return_value='token'):
            with mock.patch('jwt.decode', return_value=payload1):
                # test authenticating on more than one env
                auth = app_utils.check_authorization({}, env='mastertest,cgap-dev')
            assert auth
        # build a 'request header' that just consists of the context we would expect
        # to see if authenticating from localhost
        ctx = {
            'context': {
                'identity' : {
                    'sourceIp': '127.0.0.1'
                }
            }
        }
        auth = app_utils.check_authorization(ctx, env='all')
        assert auth
        with mock.patch('chalicelib.app_utils.get_jwt', return_value='token'):
            with mock.patch('jwt.decode', return_value=payload1):
                auth = app_utils.check_authorization({}, env='data,staging') # test more than one
            assert auth
            # Unverified email should fail
            payload2 = {
                "email": "william_ronchetti@hms.harvard.edu",
                "email_verified": False,
                "sub": "1234567890",
                "name": "Dummy",
                "iat": 1516239022
            }
            with mock.patch('jwt.decode', return_value=payload2):
                auth = app_utils.check_authorization({}, env='mastertest')
            assert not auth
            # Email not found
            payload3 = {
                "email": "blah@blah",
                "email_verified": True,
                "sub": "1234567890",
                "name": "Dummy",
                "iat": 1516239022
            }
            with mock.patch('jwt.decode', return_value=payload3):
                auth = app_utils.check_authorization({}, env='mastertest')
            assert not auth


    def test_forbidden_response(self):
        res = app_utils.forbidden_response()
        assert (res.status_code == 403)
        assert (res.body == 'Forbidden. Login on the /view/<environment> page.')

    def test_get_domain_and_context(self):
        domain, context = app_utils.get_domain_and_context(
            {'headers': {'host': 'xyz'}, 'context': {'path': '/api/123'}}
        )
        assert (domain == 'xyz')
        assert (context == '/api/')
        # with no context provided
        domain, context = app_utils.get_domain_and_context(
            {'headers': {'host': 'xyz'}}
        )
        assert (context == '/')

    def test_process_response(self):
        response = chalice.Response(
            status_code = 200,
            body = "A reasonable body."
        )
        assert (response == app_utils.process_response(response))
        # test for a response that's too long
        response.body = 'A' * 6000000
        too_long_resp = app_utils.process_response(response)
        assert (too_long_resp.status_code == 413)
        assert (too_long_resp.body == 'Body size exceeded 6 MB maximum.')

    def test_trim_output(self):
        short_output = {'some_field': 'some_value'}
        trimmed_short = app_utils.trim_output(short_output)
        assert (trimmed_short == {'some_field': 'some_value'})
        long_output = {'some_field': 'some_value ' * 100000}
        trimmed_long = app_utils.trim_output(long_output)
        assert trimmed_long == app_utils.TRIM_ERR_OUTPUT

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
        assert (literal_params['primary'] == True)
        assert (literal_params['bad_bool'] == 'false')
        assert (literal_params['int'] == 12)
        assert (literal_params['float'] == 12.1)
        assert (literal_params['str'] == 'abc')
        assert (literal_params['none_str'] is None)
        assert ('empty_str' not in literal_params)
        assert (literal_params['special'] == '&limit=all')

    # XXX: mock_s3 does not work (mocks resource but not client), so use 'real' S3 in a safe way
    # hopefully this will be fixed eventually -Will 4/27/2020
    @pytest.mark.integrated
    def test_delete_foursight_env(self):
        """ Tests the delete foursight API on real S3 using a test bucket and fake env names """
        client = boto3.client('s3', region_name='us-east-1')
        resource = boto3.resource('s3', region_name='us-east-1')
        test_bucket = 'foursight-unit-test-envs'

        # lets give it some bucket layout that mimics ours but is not exactly the same
        resource.create_bucket(Bucket=test_bucket)
        client.put_object(Bucket=test_bucket, Key='yellow', Body=json.dumps({'test': 'env'}))
        client.put_object(Bucket=test_bucket, Key='pink', Body=json.dumps({'test': 'env'}))
        client.put_object(Bucket=test_bucket, Key='red', Body=json.dumps({'test': 'env'}))

        # lets delete the 'red' bucket, all other buckets should be there
        app_utils.run_delete_environment('red', bucket=test_bucket)

        # we should now only have 2 with keys 'data' and 'staging'
        try:
            yellow_body = json.loads(resource.Object(test_bucket, 'yellow').get()['Body'].read().decode("utf-8"))
            pink_body = json.loads(resource.Object(test_bucket, 'pink').get()['Body'].read().decode("utf-8"))
            assert yellow_body['test'] == 'env'
            assert pink_body['test'] == 'env'
        except Exception as e:
            raise AssertionError('Was not able to get expected foursight-unit-test-envs configs, '
                                 'got exception: %s' % str(e))

        # ensure the one we wanted to delete is actually gone
        with pytest.raises(ClientError):
            json.loads(resource.Object(test_bucket, 'red').get()['Body'].read().decode("utf-8"))

        # delete the remaining
        app_utils.run_delete_environment('pink', bucket=test_bucket)
        app_utils.run_delete_environment('yellow', bucket=test_bucket)

        # verify those deletes were successful
        with pytest.raises(ClientError):
            json.loads(resource.Object(test_bucket, 'pink').get()['Body'].read().decode("utf-8"))
            json.loads(resource.Object(test_bucket, 'yellow').get()['Body'].read().decode("utf-8"))


