from conftest import *

class TestUtils():
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

    def test_get_stage_info(self):
        # after using app.set_stage('test')
        info = utils.get_stage_info()
        assert ({'stage', 'runner_name', 'queue_name'} <= set(info.keys()))
        assert (info['stage'] == 'dev')
        assert ('dev' in info['runner_name'])
        assert ('test' in info['queue_name'])

    def test_check_timeout(self):
        assert (isinstance(utils.CHECK_TIMEOUT, int))

    def test_check_times_out(self):
        # set to one second, which is slower than test check
        utils.CHECK_TIMEOUT = 1
        with pytest.raises(SystemExit) as exc:
            check_utils.run_check_or_action(self.conn, 'test_checks/test_random_nums', {})
        assert ('-RUN-> TIMEOUT' in str(exc.value))
        utils.CHECK_TIMEOUT = 280

    def test_list_environments(self):
        env_list = utils.list_environments()
        # assume we have at least one environments
        assert (isinstance(env_list, list))
        assert (self.environ in env_list)

    def test_check_function_deco_default_kwargs(self):
        # test to see if the check_function decorator correctly overrides
        # kwargs of decorated function if none are provided
        kwargs_default = self.test_function_dummy().get('kwargs')
        # pop runtime_seconds from here
        assert ('runtime_seconds' in kwargs_default)
        runtime = kwargs_default.pop('runtime_seconds')
        assert (isinstance(runtime, float))
        assert ('_run_info' not in kwargs_default)
        uuid = kwargs_default.get('uuid')
        assert (kwargs_default == {'abc': 123, 'do_not_store': True, 'uuid': uuid, 'primary': False, 'queue_action': 'Not queued'})
        kwargs_add = self.test_function_dummy(bcd=234).get('kwargs')
        assert ('runtime_seconds' in kwargs_add)
        kwargs_add.pop('runtime_seconds')
        assert (kwargs_add == {'abc': 123, 'bcd': 234, 'do_not_store': True, 'uuid': uuid, 'primary': False, 'queue_action': 'Not queued'})
        kwargs_override = self.test_function_dummy(abc=234, primary=True).get('kwargs')
        assert ('runtime_seconds' in kwargs_override)
        kwargs_override.pop('runtime_seconds')
        assert (kwargs_override == {'abc': 234, 'do_not_store': True, 'uuid': uuid, 'primary': True, 'queue_action': 'Not queued'})

    def test_handle_kwargs(self):
        default_kwargs = {'abc': 123, 'bcd': 234}
        kwargs = utils.handle_kwargs({'abc': 345}, default_kwargs)
        assert (kwargs.get('abc') == 345)
        assert (kwargs.get('bcd') == 234)
        assert (kwargs.get('uuid').startswith('20'))
        assert (kwargs.get('primary') == False)

    def test_init_check_res(self):
        check = utils.init_check_res(self.conn, 'test_check')
        assert (check.name == 'test_check')
        assert (check.connections['s3'] is not None)

    def test_init_action_res(self):
        action = utils.init_action_res(self.conn, 'test_action')
        assert (action.name == 'test_action')
        assert (action.connections['s3'] is not None)

    def test_BadCheckOrAction(self):
        test_exc = utils.BadCheckOrAction()
        assert (str(test_exc) == 'Check or action function seems to be malformed.')
        test_exc = utils.BadCheckOrAction('Abcd')
        assert (str(test_exc) == 'Abcd')

    def test_validate_run_result(self):
        check = utils.init_check_res(self.conn, 'test_check')
        action = utils.init_action_res(self.conn, 'test_action')
        # bad calls
        with pytest.raises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(action, is_check=True)
        assert (str(exc.value) == 'Check function must return a CheckResult object. Initialize one with CheckResult.')
        with pytest.raises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(check, is_check=False)
        assert (str(exc.value) == 'Action functions must return a ActionResult object. Initialize one with ActionResult.')
        check.store_result = 'Not a fxn'
        with pytest.raises(utils.BadCheckOrAction) as exc:
            utils.validate_run_result(check, is_check=True)
        assert (str(exc.value) == 'Do not overwrite the store_result method of the check or action result.')

    def parse_datetime_to_utc(self):
        [dt_tz_a, dt_tz_b, dt_tz_c] = ['None'] * 3
        for t_str in [self.timestr_1, self.timestr_2, self.timestr_3, self.timestr_4]:
            dt = utils.parse_datetime_to_utc(t_str)
            assert (dt is not None)
            assert (dt.tzinfo is not None and dt.tzinfo == tz.tzutc())
            if t_str == self.timestr_1:
                dt_tz_a = dt
            elif t_str == self.timestr_2:
                dt_tz_b = dt
            elif t_str == self.timestr_3:
                dt_tz_c = dt
        assert (dt_tz_c > dt_tz_a > dt_tz_b)
        for bad_tstr in [self.timestr_bad_1, self.timestr_bad_2, self.timestr_bad_3]:
            dt_bad = utils.parse_datetime_to_utc(bad_tstr)
            assert (dt_bad is None)
        # use a manual format
        dt_5_man = utils.parse_datetime_to_utc(self.timestr_5, manual_format="%Y-%m-%dT%H:%M:%S")
        dt_5_auto = utils.parse_datetime_to_utc(self.timestr_5)
        assert (dt_5_auto == dt_5_man)

    def test_camel_case_to_snake(self):
        camel_test = 'SomeCamelCaseString'
        snake_res = utils.convert_camel_to_snake(camel_test)
        assert snake_res == 'some_camel_case_string'

    def test_get_s3_utils(self):
        """
        Sanity test for s3 utils for all envs
        """
        environments = [env for env in app_utils.init_environments() if 'cgap' not in env]
        for env in environments:
            conn = app_utils.init_connection(env)
            s3_obj = s3_utils.s3Utils(env=conn.ff_env)
            assert (s3_obj.sys_bucket is not None)
            assert (s3_obj.outfile_bucket is not None)
            assert (s3_obj.raw_file_bucket is not None)
            ff_keys = s3_obj.get_access_keys()
            assert ({'server', 'key', 'secret'} <= set(ff_keys.keys()))
            hg_keys = s3_obj.get_higlass_key()
            assert ({'server', 'key', 'secret'} <= set(hg_keys.keys()))
