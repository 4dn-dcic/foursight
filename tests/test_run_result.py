from conftest import *

class TestRunResult():
    check_name = 'test_only_check'
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)

    def test_clear_s3(self):
        run = run_result.RunResult(self.connection.s3_connection, self.check_name)
        # post some new checks
        nChecks = run.get_n_results()
        for _ in range(5):
            check = run_result.CheckResult(self.connection.s3_connection, self.check_name)
            check.description = 'This check is just for testing purposes.'
            check.status = 'PASS'
            check.store_result()
        after_post = run.get_n_results()
        while (after_post < nChecks):
            time.sleep(1)  # make sure s3 has recieved all
            after_post = run.get_n_results()
        import pdb; pdb.set_trace()
        run.clean_s3_files(prior_date=datetime.datetime.now()) # this will not remove primary results
        after_clean = run.get_n_results()
        assert after_clean < after_post
