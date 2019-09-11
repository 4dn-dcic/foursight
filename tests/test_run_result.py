from conftest import *

class TestRunResult():
    check_name = 'test_only_check'
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)

  #@pytest.mark.flaky
    def test_delete_results_nonprimary(self):
        """
        Makes 5 checks, deletes those 5 checks
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post some new checks
        nChecks = run.get_n_results()
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check is just for testing purposes.'
            check.status = 'PASS'
            check.store_result()
        time.sleep(5)
        import pdb; pdb.set_trace()
        run.delete_results(prior_date=datetime.datetime.now()) # this will not remove primary results
        time.sleep(5)
        after_clean = run.get_n_results()
        assert after_clean == nChecks
        
    @pytest.mark.flaky
    def test_delete_results_primary(self):
        """
        Tests deleting primary checks - both deletion should succed since
        they are primary
        """
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post a primary check
        nChecks = run.get_n_results()
        check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.kwargs = {'primary': True}
        check.store_result()
        after_post = run.get_n_results()
        while (after_post < nChecks + 1):
            time.sleep(1) # make sure s3 has receieved the check
            after_post = run.get_n_results()
        run.delete_results(prior_date=datetime.datetime.now(), primary=False)
        after_primary_delete = run.get_n_results()
        while (after_primary_delete == after_post):
            time.sleep(1)
            after_primary_delete = run.get_n_results()
        assert after_primary_delete == nChecks
        # post primary check again, this time don't give a prior date
        # just clean all primary results
        check.store_result()
        after_post = run.get_n_results()
        while (after_post < nChecks + 1):
            time.sleep(1)
            after_post = run.get_n_results()
        run.delete_results(primary=False)
        after_second_primary_delete = run.get_n_results()
        while (after_second_primary_delete == after_post):
            time.sleep(1)
            after_second_primary_delete = run.get_n_results()
        assert after_second_primary_delete == nChecks
