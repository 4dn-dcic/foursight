from conftest import *

def delay_rerun(*args):
    time.sleep(90)
    return True

#pytestmark = [pytest.mark.flaky(rerun_filter=delay_rerun)]

# thanks to Rob Kennedy on S.O. for this bit of code
@contextmanager
def captured_output():
    """
    Capture stdout and stderr
    """
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO

    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class TestCheckRunner():
    environ = 'mastertest'
    app.set_stage('test')
    connection = app_utils.init_connection(environ)
    # set up a queue for test checks
    stage_info = utils.get_stage_info()
    queue = utils.get_sqs_queue()

    def clear_queue_and_runners(self):
        """
        Ensure that the SQS queue is empty and give existing check runners
        a chance to finish
        """
        tries = 0
        found_clear = True
        while tries < 10:
            sqs_attrs = utils.get_sqs_attributes(self.queue.url)
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            if vis_messages == 0 and invis_messages == 0:
                # allow existing checks to terminate via long polling
                if found_clear:
                    break
                time.sleep(12)
                found_clear = True
            elif invis_messages > 0:
                # if orphaned messages are in the queue, eat them up
                app_utils.run_check_runner({'sqs_url': self.queue.url}, propogate=False)
                tries += 1
                found_clear = False
                time.sleep(2)
            else:
                # wait less time to see if processing is finished
                tries += 1
                found_clear = False
                time.sleep(2)
        return found_clear

    def test_queue_basics(self):
        # ensure we have the right queue and runner names
        assert (self.stage_info['queue_name'] == 'foursight-test-check_queue')
        assert (self.stage_info['runner_name'] == 'foursight-dev-check_runner')

    def test_check_runner_manually(self):
        """
        Queue a check and make sure it is run
        Invoke run_check_runner manually, not via AWS lambda.
        This test can fail if other self-propogating check runner sare hanging
        around, so run this before other checks that queue
        """
        cleared = self.clear_queue_and_runners()
        assert (cleared)
        check = run_result.CheckResult(self.connection, 'test_random_nums')
        prior_res = check.get_latest_result()
        # first, bad input
        bad_res = app_utils.run_check_runner({'sqs_url': None})
        assert (bad_res is None)
        # queue a check without invoking runner. Get resulting run uuid
        to_send = ['test_checks/test_random_nums', {}, []]
        tries = 0
        test_success = False
        while tries < 10 and not test_success:
            tries += 1
            run_uuid = app_utils.send_single_to_queue(self.environ, to_send, None, invoke_runner=False)
            time.sleep(1)
            with captured_output() as (out, err):
                # invoke runner manually (without a lamba)
                res = app_utils.run_check_runner({'sqs_url': self.queue.url}, propogate=False)
            read_out = out.getvalue().strip()
            if res and res.get('uuid') == run_uuid:
                # check the result from run_check_runner
                assert (res['name'] == 'test_random_nums')
                assert (res['uuid'] == run_uuid)
                assert ('_run_info' in res['kwargs'])
                assert (res['kwargs']['_run_info']['run_id'] == run_uuid)
                # check a couple things about printed runner output
                assert ('%s (uuid)' % run_uuid in read_out)
                assert ('Finished: test_checks/test_random_nums' in read_out)
                test_success = True
        assert (test_success)
        # check the stored result as well
        post_res = check.get_result_by_uuid(run_uuid)
        assert (post_res is not None)
        assert ('_run_info' in post_res['kwargs'])
        assert ({'run_id', 'receipt', 'sqs_url'} <= set(post_res['kwargs']['_run_info'].keys()))
        assert (post_res['kwargs']['_run_info']['run_id'] == run_uuid)

    # Dependent on action records, needs some discussion on how to fix
    @pytest.mark.skip
    def test_check_runner_manually_with_associated_action(self):
        cleared = self.clear_queue_and_runners()
        assert (cleared)
        # queue a check with queue_action="dev" kwarg, meaning the associated
        # action will automatically be queued after completion
        check = utils.init_check_res(self.connection, 'test_random_nums')
        action = utils.init_action_res(self.connection, 'add_random_test_nums')
        to_send = ['test_checks/test_random_nums', {'primary': True, 'queue_action': 'dev'}, []]
        # send the check to the queue; the action will be queue automatically
        run_uuid = app_utils.send_single_to_queue(self.environ, to_send, None, invoke_runner=False)
        # both check and action separately must make it through queue
        check_done = False
        action_done = False
        tries = 0
        while (not check_done or not action_done) and tries < 20:
            tries += 1
            time.sleep(1)
            app_utils.run_check_runner({'sqs_url': self.queue.url}, propogate=False)
            if not check_done:
                latest_check_res = check.get_latest_result()
                if latest_check_res and latest_check_res['uuid'] >= run_uuid:
                    check_done = True
            elif not action_done:
                latest_act_res = action.get_latest_result()
                if latest_act_res and latest_act_res['uuid'] >= run_uuid:
                    action_done = True
        assert (check_done and action_done)
        # get the check and action by run_uuid
        run_check = check.get_result_by_uuid(run_uuid)
        assert (run_check is not None)
        run_action = action.get_result_by_uuid(run_uuid)
        assert (run_action is not None)
        # confirm some fields on final result
        assert (run_action['kwargs']['check_name']) == 'test_random_nums'
        assert (run_action['kwargs']['called_by'] == run_uuid)
        assert (run_action['kwargs']['_run_info']['run_id'] == run_uuid)

        # ensure that the action_record was written correctly
        action_rec_key = '/'.join(['test_random_nums/action_records', run_uuid])
        assc_action_key = self.connection.connections['s3'].get_object(action_rec_key)
        assert (assc_action_key is not None)
        assc_action_key = assc_action_key.decode()  # in bytes
        # expect the contents of the action record to be s3 location of action
        expected_key = ''.join([action.name, '/', run_uuid, action.extension])
        assert (assc_action_key == expected_key)
        # further actions cannot be run with the check
        act_kwargs = {'check_name': run_check['name'], 'called_by': run_check['uuid']}
        tries = 0
        test_success = False
        while tries < 10 and not test_success:
            tries += 1
            to_send = ['test_checks/add_random_test_nums', act_kwargs, []]
            app_utils.send_single_to_queue(self.environ, to_send, None, invoke_runner=False)
            time.sleep(1)
            with captured_output() as (out, err):
                # invoke runner manually (without a lamba) and do not propogate
                runner_res = app_utils.run_check_runner({'sqs_url': self.queue.url},
                                                        propogate=False)
            read_out = out.getvalue().strip()
            if 'Found existing action record' in read_out:
                test_success = True
        assert (test_success)
        assert (runner_res is None)

    @pytest.mark.skip # same issue as above
    def test_queue_check_group(self):
        # find the checks we will be using
        use_schedule = 'ten_min_checks'
        check_schedule = check_utils.get_check_schedule(use_schedule)
        use_checks = [cs[0].split('/')[1] for env in check_schedule for cs in check_schedule[env]]
        # get a reference point for check results
        prior_res = check_utils.get_check_results(self.connection, checks=use_checks, use_latest=True)
        run_input = app_utils.queue_scheduled_checks(self.environ, 'ten_min_checks')
        assert (self.stage_info['queue_name'] in run_input.get('sqs_url'))
        finished_count = 0  # since queue attrs are approximate
        error_count = 0
        # wait for queue to empty
        while finished_count < 2:
            time.sleep(1)
            sqs_attrs = utils.get_sqs_attributes(run_input.get('sqs_url'))
            vis_messages = int(sqs_attrs.get('ApproximateNumberOfMessages'))
            invis_messages = int(sqs_attrs.get('ApproximateNumberOfMessagesNotVisible'))
            if vis_messages == 0 and invis_messages == 0:
                finished_count += 1
            else:
                error_count += 1
                # eat up residual messages
                app_utils.run_check_runner({'sqs_url': self.queue.url}, propogate=False)
            if error_count > 60:  # test should fail
                print('Could not find an empty foursight-test-queue.')
                assert (False)
        # queue should be empty. check results
        time.sleep(4)
        post_res = check_utils.get_check_results(self.connection, checks=use_checks, use_latest=True)
        # compare the runtimes to ensure checks have run
        res_compare = {}
        for check_res in post_res:
            res_compare[check_res['name']] = {'post': check_res['uuid']}
        for check_res in prior_res:
            res_compare[check_res['name']]['prior'] = check_res['uuid']
        for check_name in res_compare:
            assert ('post' in res_compare[check_name] and 'prior' in res_compare[check_name])
            assert (res_compare[check_name]['prior'] != res_compare[check_name]['post'])

    def test_queue_check(self):
        check = utils.init_check_res(self.connection, 'test_random_nums')
        run_uuid = app_utils.queue_check(self.environ, 'test_random_nums')
        # both check and action separately must make it through queue
        tries = 0
        while True:
            time.sleep(1)
            latest_check_res = check.get_latest_result()
            if latest_check_res and latest_check_res['uuid'] >= run_uuid:
                break
            else:
                tries += 1
            if tries > 60:  # test should fail
                print('Could not find an empty foursight-test-queue.')
                assert (False)
        # get the check by run_uuid
        run_check = check.get_result_by_uuid(run_uuid)
        assert (run_check is not None)
        assert (run_check['kwargs']['uuid'] == run_uuid)

    def test_queue_action(self):
        # this action will fail because it has no check-related kwargs
        action = utils.init_action_res(self.connection, 'add_random_test_nums')
        run_uuid = app_utils.queue_action(self.environ, 'add_random_test_nums')
        # both check and action separately must make it through queue
        tries = 0
        while True:
            time.sleep(1)
            latest_act_res = action.get_latest_result()
            if latest_act_res and latest_act_res['uuid'] >= run_uuid:
                break
            else:
                tries += 1
            if tries > 60:  # test should fail
                print('Could not find an empty foursight-test-queue.')
                assert (False)
        # get the action by run_uuid
        run_action = action.get_result_by_uuid(run_uuid)
        assert (run_action is not None)
        assert (run_action['kwargs']['uuid'] == run_uuid)
        assert (run_action['kwargs']['_run_info']['run_id'] == run_uuid)
        assert ('Action failed to run' in run_action['description'])

    def test_get_sqs_attributes(self):
        # bad sqs url
        bad_sqs_attrs = utils.get_sqs_attributes('not_a_queue')
        assert (bad_sqs_attrs.get('ApproximateNumberOfMessages') == bad_sqs_attrs.get('ApproximateNumberOfMessagesNotVisible') == 'ERROR')

    def test_record_and_collect_run_info(self):
        check = utils.init_check_res(self.connection, 'not_a_real_check')
        check.kwargs['_run_info'] = {'run_id': 'test_run_uuid'}
        resp = check.record_run_info()
        assert (resp is not None)
        found_ids = utils.collect_run_info('test_run_uuid')
        assert (set(['test_run_uuid/not_a_real_check']) <= found_ids)
