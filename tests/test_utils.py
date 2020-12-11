from conftest import *


class TestUtils():
    environ = DEV_ENV # hopefully this is up
    app_utils_obj = app_utils.AppUtils()
    conn = app_utils_obj.init_connection(environ)

    @check_function(abc=123, do_not_store=True, uuid=datetime.datetime.utcnow().isoformat())
    def test_function_dummy(self, *args, **kwargs):
        connection = self.app_utils_obj.init_connection(self.environ)
        check = run_result.CheckResult(connection, 'not_a_check')
        check.summary = 'A string summary'
        check.description = 'A string description'
        check.ff_link = 'A string link'
        check.action = 'A string action'
        check.kwargs = {}
        return check

    def test_check_timeout(self):
        assert (isinstance(decorators.Decorators(FOURSIGHT_PREFIX).CHECK_TIMEOUT, int))

    @pytest.mark.skip  # Works but does not behave correctly with pytest
    def test_check_times_out(self):
        # set to one second, which is slower than test check
        os.environ['CHECK_TIMEOUT'] = 1
        with pytest.raises(SystemExit) as exc:
            check_utils.CheckHandler.run_check_or_action(self.conn, 'test_checks/test_random_nums', {})
        assert ('-RUN-> TIMEOUT' in str(exc.value))
        os.environ['CHECK_TIMEOUT'] = 870

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
        kwargs = decorators.Decorators.handle_kwargs({'abc': 345}, default_kwargs)
        assert (kwargs.get('abc') == 345)
        assert (kwargs.get('bcd') == 234)
        assert (kwargs.get('uuid').startswith('20'))
        assert (kwargs.get('primary') == False)

    def test_get_s3_utils(self):
        """
        Sanity test for s3 utils for all envs
        """
        environments = [env for env in self.app_utils_obj.init_environments() if 'cgap' not in env]
        for env in environments:
            conn = self.app_utils_obj.init_connection(env)
            s3_obj = s3_utils.s3Utils(env=conn.ff_env)
            assert (s3_obj.sys_bucket is not None)
            assert (s3_obj.outfile_bucket is not None)
            assert (s3_obj.raw_file_bucket is not None)
            ff_keys = s3_obj.get_access_keys()
            assert ({'server', 'key', 'secret'} <= set(ff_keys.keys()))
            hg_keys = s3_obj.get_higlass_key()
            assert ({'server', 'key', 'secret'} <= set(hg_keys.keys()))
