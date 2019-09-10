from conftest import *

class TestCheckResult():
    # use a fake check name and store on mastertest
    check_name = 'test_only_check'
    # another fake check, with only ERROR results
    error_check_name = 'test_only_error_check'
    environ = 'mastertest' # hopefully this is up
    connection = app_utils.init_connection(environ)

    def test_check_result_methods(self):
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
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
        assert (late_res == res)
        primary_res = check.get_primary_result()
        assert (primary_res == res)
        # check get_closest_res without and with override_date
        close_res = check.get_closest_result(0, 0)
        assert (close_res == res)
        override_res = check.get_closest_result(override_date=datetime.datetime.utcnow())
        assert (override_res == res)
        all_res = check.get_all_results()
        assert (len(all_res) > 0)
        # this should be true since all results will be identical
        assert (all_res[-1].get('description') == res.get('description'))
        # ensure that previous check results can be fetch using the uuid functionality
        res_uuid = res['uuid']
        check_copy = run_result.CheckResult(self.connection.s3_connection, self.check_name, init_uuid=res_uuid)
        # should not have 'uuid' or 'kwargs' attrs with init_uuid
        assert (getattr(check_copy, 'uuid', None) is None)
        assert (getattr(check_copy, 'kwargs', {}) == {})
        check_copy.kwargs = {'primary': True, 'uuid': prime_uuid}
        assert (res == check_copy.store_result())

    def test_get_closest_result(self):
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check.status = 'ERROR'
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
        bad_check = run_result.CheckResult(self.connection.s3_connection, 'not_a_real_check')
        with pytest.raises(Exception) as exc:
            bad_check.get_closest_result(diff_hours=0, diff_mins=0)
        assert ('Could not find any results' in str(exc.value))
        error_check = run_result.CheckResult(self.connection.s3_connection, self.error_check_name)
        error_check.status = 'ERROR'
        error_check.store_result()
        with pytest.raises(Exception) as exc:
            error_check.get_closest_result(diff_hours=0, diff_mins=0)
        assert ('Could not find closest non-ERROR result' in str(exc.value))

    def test_get_result_history(self):
        """
        This relies on the check having been run enough times. If not, return
        """
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        # ensure at least one entry present
        check.status = 'IGNORE'
        check.summary = 'TEST HISTORY'
        check.kwargs['test'] = 'yea'
        res = check.store_result()
        time.sleep(2)
        ignore_uuid = res['uuid']
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
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check_result = check.store_result()
        time_key = ''.join([check.name, '/', check_result['uuid'], check.extension])
        filename_date = check.filename_to_datetime(time_key)
        compare_date = datetime.datetime.strptime(check_result['uuid'], '%Y-%m-%dT%H:%M:%S.%f')
        assert filename_date == compare_date