from conftest import *

class TestRunResult():
    check_name = 'test_only_check'
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)

    @pytest.mark.flaky
    def test_delete_results_nonprimary(self):
        """
        Makes 5 non-primary checks and 1 primary check, deletes those 5 checks,
        verifies only those 5 were deleted, then deletes the primary check
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post some new checks
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check is just for testing purposes.'
            check.status = 'PASS'
            check.store_result()

        # post a primary check (should persist)
        primary_check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        primary_check.description = 'This is a primary check - it should persist'
        primary_check.kwargs = {'primary': True}
        res = primary_check.store_result()
        time.sleep(3)
        num_deleted = run.delete_results(prior_date=datetime.datetime.utcnow())
        assert num_deleted == 5 # primary result should not have been deleted
        queried_primary = run.get_primary_result()
        assert res['kwargs']['uuid'] == queried_primary['kwargs']['uuid']
        primary_deleted = run.delete_results(primary=False)
        assert primary_deleted >= 1 # now primary result should be gone

    @pytest.mark.flaky
    def test_delete_results_primary(self):
        """
        Tests deleting a primary check
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post a primary check
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.kwargs = {'primary': True}
        res = check.store_result()
        time.sleep(3)
        queried_primary = run.get_primary_result()
        assert res['kwargs']['uuid'] == queried_primary['kwargs']['uuid']
        num_deleted = run.delete_results(primary=False)
        assert num_deleted == 1

    @pytest.mark.flaky
    def test_delete_results_custom_filter(self):
        """
        Post some checks with a term in the description that we filter out
        based on a custom_filter
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        def term_in_descr(key):
            obj = run.get_s3_object(key)
            if obj.get('description') is not None:
                return 'bad_term' in obj.get('description')
            return False

        # post some checks to be filtered
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check contains bad_term which should be filtered.'
            check.status = 'PASS'
            check.store_result()
        for _ in range(3):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This is a normal check.'
            check.status = 'PASS'
            check.store_result()
        time.sleep(3)
        num_deleted = run.delete_results(custom_filter=term_in_descr)
        assert num_deleted == 5
        num_deleted = run.delete_results()
        assert num_deleted == 3

        # test that bad filter throws Exception
        def bad_filter(key):
            raise Exception
        with pytest.raises(Exception):
            run.delete_results(custom_filter=bad_filter)
