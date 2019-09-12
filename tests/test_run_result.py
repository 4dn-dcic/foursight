from conftest import *

class TestRunResult():
    check_name = 'test_only_check'
    environ = 'mastertest'
    connection = app_utils.init_connection(environ)
    run = run_result.RunResult(connection.connections, check_name)

    @pytest.mark.flaky
    def test_delete_results_nonprimary(self):
        """
        Makes 5 non-primary checks and 1 primary check, deletes those 5 checks,
        verifies only those 5 were deleted, then deletes the primary check
        """
        # post some new checks
        for _ in range(5):
            check = run_result.CheckResult(self.connection.connections, self.check_name)
            check.description = 'This check is just for testing purposes.'
            check.status = 'PASS'
            check.store_result()

        # post a primary check (should persist)
        primary_check = run_result.CheckResult(self.connection.connections, self.check_name)
        primary_check.description = 'This is a primary check - it should persist'
        primary_check.kwargs = {'primary': True}
        res = primary_check.store_result()
        num_deleted = self.run.delete_results(prior_date=datetime.datetime.utcnow())
        assert num_deleted == 5 # primary result should not have been deleted
        queried_primary = self.run.get_result_by_uuid(res['kwargs']['uuid'])
        assert res['kwargs']['uuid'] == queried_primary['kwargs']['uuid']
        primary_deleted = self.run.delete_results(primary=False)
        assert primary_deleted >= 1 # now primary result should be gone
        assert not self.run.get_result_by_uuid(res['kwargs']['uuid'])

    @pytest.mark.flaky
    def test_delete_results_primary(self):
        """
        Tests deleting a primary check
        """
        check = run_result.CheckResult(self.connection.connections, self.check_name)
        check.description = 'This check is just for testing purposes.'
        check.status = 'PASS'
        check.kwargs = {'primary': True}
        res = check.store_result()
        queried_primary = self.run.get_result_by_uuid(res['kwargs']['uuid'])
        assert res['kwargs']['uuid'] == queried_primary['kwargs']['uuid']
        num_deleted = self.run.delete_results(primary=False)
        assert num_deleted == 1
        assert not self.run.get_result_by_uuid(res['kwargs']['uuid'])

    @pytest.mark.flaky
    def test_delete_results_custom_filter(self):
        """
        Post some checks with a term in the description that we filter out
        based on a custom_filter
        """
        def term_in_descr(key):
            obj = self.run.get_s3_object(key)
            if obj.get('description') is not None:
                return 'bad_term' in obj.get('description')
            return False

        # post some checks to be filtered
        for _ in range(5):
            check = run_result.CheckResult(self.connection.connections, self.check_name)
            check.description = 'This check contains bad_term which should be filtered.'
            check.status = 'PASS'
            check.store_result()
        for _ in range(3):
            check = run_result.CheckResult(self.connection.connections, self.check_name)
            check.description = 'This is a normal check.'
            check.status = 'PASS'
            check.store_result()
        num_deleted = self.run.delete_results(custom_filter=term_in_descr)
        assert num_deleted == 5
        num_deleted = self.run.delete_results()
        assert num_deleted == 3

    @pytest.mark.flaky
    def test_delete_results_bad_filter(self):
        """
        Posts a check then attempts to delete it with an invalid custom_filter
        Should raise an exception. Check is then deleted.
        """
        def bad_filter(key):
            raise Exception

        check = run_result.CheckResult(self.connection.connections, self.check_name)
        check.description = 'This is a normal check.'
        check.status = 'PASS'
        check.store_result()
        with pytest.raises(Exception):
            run.delete_results(custom_filter=bad_filter)
        num_deleted = self.run.delete_results()
        assert num_deleted == 1

    @pytest.mark.flaky
    def test_delete_results_primary_custom_filter(self):
        """
        Posts two primary checks, deletes more recent one based on custom filter
        and checks get_primary_result gives the second one still since it will
        have been copied
        """
        one_uuid = datetime.datetime.utcnow().isoformat()
        two_uuid = datetime.datetime.utcnow().isoformat()

        # this function will look to delete a specific primary check based on
        # two_uuid (ie: it should only delete that uuid)
        def filter_specific_uuid(key):
            obj = self.run.get_s3_object(key)
            return obj['kwargs']['uuid'] == two_uuid

        # setup, post checks
        p_check_one = run_result.CheckResult(self.connection.connections, self.check_name)
        p_check_one.description = "This is the first primary check"
        p_check_one.status = 'PASS'
        p_check_one.kwargs = {'primary': True, 'uuid': one_uuid}
        p_check_one.store_result()
        p_check_two = run_result.CheckResult(self.connection.connections, self.check_name)
        p_check_two.description = "This is the second primary check"
        p_check_two.status = 'PASS'
        p_check_two.kwargs = {'primary': True, 'uuid': two_uuid}
        p_check_two.store_result()
        queried_primary = self.run.get_primary_result()
        assert queried_primary['kwargs']['uuid'] == two_uuid
        num_deleted = self.run.delete_results(primary=False, custom_filter=filter_specific_uuid)
        assert num_deleted == 1
        queried_primary = self.run.get_primary_result()
        assert queried_primary['kwargs']['uuid'] == two_uuid

    @pytest.mark.flaky
    def test_delete_results_error_filter(self):
        """
        Posts two checks - one successful, one fail, deletes the failed check
        using a custom filter, deletes the successful check after
        """
        def filter_error(key):
            obj = self.run.get_s3_object(key)
            return obj['status'] == 'ERROR'

        check_one = run_result.CheckResult(self.connection.connections, self.check_name)
        check_one.description = "This is the first check, it failed"
        check_one.status = 'ERROR'
        check_one.store_result()
        check_two = run_result.CheckResult(self.connection.connections, self.check_name)
        check_two.description = "This is the second check, it passed"
        check_two.status = 'PASS'
        resp = check_two.store_result()
        num_deleted = self.run.delete_results(custom_filter=filter_error)
        assert num_deleted == 1
        assert self.run.get_result_by_uuid(resp['uuid'])
        num_deleted = self.run.delete_results()
        assert num_deleted == 1
