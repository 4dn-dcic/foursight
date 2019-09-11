from conftest import *

class TestRunResult():
    check_name = 'test_only_check'
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)

    @pytest.mark.flaky
    def test_delete_results_nonprimary(self):
        """
        Makes 5 non-primary checks, deletes those 5 checks
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post some new checks
        nChecks = run.get_n_results()
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check is just for testing purposes.'
            check.status = 'PASS'
            check.store_result()
        time.sleep(3)
        run.delete_results(prior_date=datetime.datetime.utcnow())
        time.sleep(3)
        after_clean = run.get_n_results()
        assert after_clean == nChecks

    @pytest.mark.flaky
    def test_delete_results_primary(self):
        """
        Tests deleting primary checks - both deletions should succeed since
        they are primary, others may be deleted as well if they have not been
        cleaned up.
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post a primary check
        nChecks = run.get_n_results()
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.kwargs = {'primary': True}
        check.store_result()
        time.sleep(3)
        run.delete_results(prior_date=datetime.datetime.utcnow(), primary=False)
        time.sleep(3)
        after_primary_delete = run.get_n_results()
        assert after_primary_delete <= nChecks # other test compatibility
        # post primary check again, this time don't give a prior date
        # just clean all primary results
        check.store_result()
        time.sleep(3)
        run.delete_results(primary=False)
        time.sleep(3)
        after_second_primary_delete = run.get_n_results()
        assert after_second_primary_delete <= nChecks # other test compatibility

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
        nChecks = run.get_n_results()
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check contains bad_term which should be filtered.'
            check.status = 'PASS'
            check.store_result()
        time.sleep(3)
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This is a normal check.'
            check.status = 'PASS'
            check.store_result()
        run.delete_results(custom_filter=term_in_descr)
        time.sleep(3)
        after_filter = run.get_n_results()
        assert after_filter == (nChecks + 5) # we added 10, 5 should have been deleted
        run.delete_results()
        time.sleep(3)
        after_filter = run.get_n_results()
        assert after_filter <= nChecks # now all 10 should have been deleted

        # test that bad filter throws Exception
        def bad_filter(key):
            raise Exception
        with pytest.raises(Exception):
            run.delete_results(custom_filter=bad_filter)
