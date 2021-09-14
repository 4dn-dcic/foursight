from conftest import *


class TestCheckResult():
    # use a fake check name and store on DEV_ENV
    check_name = 'test_only_check'
    # another fake check, with only ERROR results
    error_check_name = 'test_only_error_check'
    environ = DEV_ENV  # hopefully this is up
    app_utils_obj = app_utils.AppUtils()
    connection = app_utils_obj.init_connection(environ)

    @staticmethod
    def check_res_without_id_alias(res1, res2):
        """ Ignore the id_alias field when comparing check responses"""
        if 'id_alias' in res1:
            del res1['id_alias']
        if 'id_alias' in res2:
            del res2['id_alias']
        assert res1 == res2

    @pytest.mark.parametrize('use_es', [False])
    def test_check_result_methods(self, use_es):
        """ Enabling the ES layer causes test flakiness due to the index refresh interval.
            Presumably the test would pass for ES if you inserted time.sleep(2) after every call
            to store_result.
        """
        check = run_result.CheckResult(self.connection, self.check_name)
        if not use_es:
            check.es = False  # trigger s3 fallback
        # default status
        assert (check.status == 'IGNORE')
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        # first store without uuid and primary kwargs; should be generated
        res = check.store_result()
        assert ('uuid' in res['kwargs'])
        assert (res['kwargs']['primary'] == False)
        assert (res['type'] == 'check')
        # set the kwargs and store again
        prime_uuid = datetime.datetime.utcnow().isoformat()
        check.kwargs = {'primary': True, 'uuid': prime_uuid}
        res = check.store_result()
        # fetch this check. latest and closest result with 0 diff should be the same
        late_res = check.get_latest_result()
        self.check_res_without_id_alias(late_res, res)
        primary_res = check.get_primary_result()
        self.check_res_without_id_alias(primary_res, res)
        # check get_closest_res without and with override_date
        close_res = check.get_closest_result(0, 0)
        self.check_res_without_id_alias(close_res, res)
        override_res = check.get_closest_result(override_date=datetime.datetime.utcnow())
        self.check_res_without_id_alias(override_res, res)
        if check.es:
            check.connections['es'].refresh_index()
        all_res = check.get_all_results()
        assert (len(all_res) > 0)
        # ensure that previous check results can be fetch using the uuid functionality
        res_uuid = res['uuid']
        check_copy = run_result.CheckResult(self.connection, self.check_name, init_uuid=res_uuid)
        if not use_es:
            check_copy.es = False  # trigger s3 fallback
        # should not have 'uuid' or 'kwargs' attrs with init_uuid
        assert (getattr(check_copy, 'uuid', None) is None)
        assert (getattr(check_copy, 'kwargs', {}) == {})
        check_copy.kwargs = {'primary': True, 'uuid': prime_uuid}
        self.check_res_without_id_alias(res, check_copy.store_result())

    @pytest.mark.parametrize('use_es', [False])
    def test_get_closest_result(self, use_es):
        """ Enabling the ES layer causes test flakiness due to the index refresh interval.
            Presumably the test would pass for ES if you inserted time.sleep(2) after every call
            to store_result.
        """
        check = run_result.CheckResult(self.connection, self.check_name)
        check.status = 'ERROR'
        if not use_es:
            check.es = False  # trigger s3 fallback
        res = check.store_result()
        err_uuid = res['uuid']
        closest_res_no_error = check.get_closest_result(diff_mins=0)
        assert (err_uuid > closest_res_no_error['uuid'])
        check.status = 'PASS'
        res2 = check.store_result()
        pass_uuid = res2['uuid']
        closest_res_no_error = check.get_closest_result(diff_mins=0)
        assert (pass_uuid == closest_res_no_error['uuid'])
        # bad cases: no results and all results are ERROR
        bad_check = run_result.CheckResult(self.connection, 'not_a_real_check')
        if not use_es:
            bad_check.es = False  # trigger s3 fallback
        with pytest.raises(Exception) as exc:
            bad_check.get_closest_result(diff_hours=0, diff_mins=0)
        assert ('Could not find any results' in str(exc.value))
        error_check = run_result.CheckResult(self.connection, self.error_check_name)
        error_check.status = 'ERROR'
        if not use_es:
            error_check.es = False   # trigger s3 fallback
        error_check.store_result()
        with pytest.raises(Exception) as exc:
            error_check.get_closest_result(diff_hours=0, diff_mins=0)
        assert ('Could not find closest non-ERROR result' in str(exc.value))

    @pytest.mark.flaky
    @pytest.mark.parametrize('use_es', [True, False])
    def test_get_result_history(self, use_es):
        """
        This relies on the check having been run enough times. If not, return
        """
        check = run_result.CheckResult(self.connection, self.check_name)
        if not use_es:
            check.es = False # trigger s3 fallback
        # ensure at least one entry present
        check.status = 'IGNORE'
        check.summary = 'TEST HISTORY'
        check.kwargs['test'] = 'yea'
        res = check.store_result()
        ignore_uuid = res['uuid']
        if check.connections['es'] is not None:
            check.connections['es'].refresh_index()
        hist_10 = check.get_result_history(0, 10)
        assert isinstance(hist_10, list)
        for chk in hist_10:
            assert isinstance(chk[2], dict) and 'uuid' in chk[2]
            # this kwarg is removed
            assert '_run_info' not in chk[2]
        # eliminate timing errors, another check could've been stored
        found_chks = [chk for chk in hist_10 if chk[2]['uuid'] == ignore_uuid]
        assert len(found_chks) == 1
        assert found_chks[0][0] == 'IGNORE'
        assert found_chks[0][1] == 'TEST HISTORY'
        assert found_chks[0][2].get('test') == 'yea'
        if len(hist_10) != 10:
            return
        hist_offset = check.get_result_history(1, 2)
        hist_offset2 = check.get_result_history(2, 2)
        assert len(hist_offset) == len(hist_offset2) == 2
        if hist_offset[1][2]['uuid'] == hist_offset2[0][2]['uuid']:
            assert hist_offset[0][2]['uuid'] != hist_offset2[0][2]['uuid']
        # test after_date param
        after_date = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        hist_after = check.get_result_history(0, 10, after_date=after_date)
        for chk in hist_after:
            chk_date = datetime.datetime.strptime(chk[2]['uuid'], '%Y-%m-%dT%H:%M:%S.%f')
            assert chk_date >= after_date

    def test_filename_to_datetime(self):
        check = run_result.CheckResult(self.connection, self.check_name)
        check_result = check.store_result()
        time_key = ''.join([check.name, '/', check_result['uuid'], check.extension])
        filename_date = check.filename_to_datetime(time_key)
        compare_date = datetime.datetime.strptime(check_result['uuid'], '%Y-%m-%dT%H:%M:%S.%f')
        assert filename_date == compare_date
