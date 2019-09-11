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
        now = datetime.datetime.now() + datetime.timedelta(days=1) # XXX: necessary because of time drift?
        run.delete_results(prior_date=now)
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
        now = datetime.datetime.now() + datetime.timedelta(days=1) # XXX: necessary because of time drift?
        run.delete_results(prior_date=now, primary=False)
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
