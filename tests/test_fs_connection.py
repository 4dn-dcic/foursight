from conftest import *

class TestFSConnection():
    environ_info = {
        'fourfront': 'test1',
        'es': 'test2',
        'bucket': None,
        'ff_env': 'test3'
    }
    connection = fs_connection.FSConnection('test', environ_info, test=True, host=HOST)

    def test_connection_fields(self):
        assert (self.connection.fs_env == 'test')
        assert (self.connection.connections['s3'].status_code == 404)
        assert (self.connection.ff_server == 'test1')
        assert (self.connection.ff_es == 'test2')
        assert (self.connection.ff_env == 'test3')
        assert (self.connection.ff_s3 is None)
        assert (self.connection.ff_keys is None)

    def test_run_check_with_bad_connection(self):
        check_handler = check_utils.CheckHandler(FOURSIGHT_PREFIX, 'chalicelib', os.path.dirname(chalicelib_path))
        check_res = check_handler.run_check_or_action(self.connection, 'wrangler_checks/item_counts_by_type', {})
        # run_check_or_action returns a dict with results
        print("check_res=" + str(check_res))
        assert (check_res.get('status') == 'ERROR')
        assert (check_res.get('name') == 'item_counts_by_type')

    def test_check_result_basics(self):
        test_check = decorators.Decorators(FOURSIGHT_PREFIX).CheckResult(self.connection, 'test_check')
        test_check.summary = 'Unittest check'
        test_check.ff_link = 'not_a_real_http_link'
        assert (test_check.connections['s3'].status_code == 404)
        assert (test_check.get_latest_result() is None)
        assert (test_check.get_primary_result() is None)
        with pytest.raises(Exception) as exec_info:
            test_check.get_closest_result(1)
        assert ('Could not find any results' in str(exec_info.value))
        formatted_res = test_check.format_result(datetime.datetime.utcnow())
        assert (formatted_res.get('status') == 'IGNORE')
        assert (formatted_res.get('summary') == 'Unittest check')
        assert (formatted_res.get('description') == 'Unittest check')
        assert (formatted_res.get('type') == 'check')
        # set a bad status on purpose
        test_check.status = "BAD_STATUS"
        check_res = test_check.store_result()
        assert (check_res.get('name') == formatted_res.get('name'))
        assert (check_res.get('description') == "Malformed status; look at Foursight check definition.")
        assert (check_res.get('brief_output') == formatted_res.get('brief_output') == None)
        assert (check_res.get('ff_link') == 'not_a_real_http_link')

    def test_bad_ff_connection_in_fs_connection(self):
        # do not set test=True, should raise because it's not a real FF
        with pytest.raises(Exception) as exec_info:
            bad_connection = fs_connection.FSConnection('test', self.environ_info, host=HOST)
        assert ('Could not initiate connection to Fourfront' in str(exec_info.value))
