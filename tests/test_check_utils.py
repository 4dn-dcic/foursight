from conftest import *

class TestCheckUtils():
    environ = 'mastertest'  # hopefully this is up
    connection = app_utils.init_connection(environ)

    def test_get_check_strings(self):
        # do this for every check
        all_check_strs = check_utils.get_check_strings()
        for check_str in all_check_strs:
            get_check = check_str.split('/')[1]
            chalice_resp = app_utils.run_get_check(self.environ, get_check)
            body = chalice_resp.body
            if body.get('status') == 'success':
                assert (chalice_resp.status_code == 200)
                if body.get('data') is None:  # check not run yet
                    continue
                assert (body.get('data', {}).get('name') == get_check)
                assert (body.get('data', {}).get('status') in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'])
            elif body.get('status') == 'error':
                error_msg = "Not a valid check or action."
                assert (body.get('description') == error_msg)
        # test a specific check
        one_check_str = check_utils.get_check_strings('indexing_progress')
        assert (one_check_str == 'system_checks/indexing_progress')
        assert (one_check_str in all_check_strs)
        # test a specific check that doesn't exist
        bad_check_str = check_utils.get_check_strings('not_a_real_check')
        assert (bad_check_str is None)

    def test_validate_check_setup(self):
        assert (check_utils.validate_check_setup(check_utils.CHECK_SETUP) == check_utils.CHECK_SETUP)
        # make sure modules were added
        for check in check_utils.CHECK_SETUP.values():
            assert ('module' in check)
        # do a while bunch of validation failure cases
        bad_setup = {'not_a_check': {}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('does not have a proper check function defined' in str(exc.value))
        bad_setup = {'indexing_progress': []}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must be a dictionary' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': {}, 'group': {}, 'blah': {}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have the required keys' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': {}, 'group': {}, 'schedule': []}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a string value for field' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': []}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a dictionary value for field' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a list of "display" environments' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': []}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a dictionary value' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'not_an_env': []}}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('is not an existing environment' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': []}}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a dictionary value' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {'kwargs': []}}}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a dictionary value' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {'dependencies': {}}}}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('must have a list value' in str(exc.value))
        bad_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {'dependencies': ['not_a_real_check']}}}}}
        with pytest.raises(utils.BadCheckSetup) as exc:
            check_utils.validate_check_setup(bad_setup)
        assert ('is not a valid check name that shares the same schedule' in str(exc.value))
        # this one will work -- display provided
        okay_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {}, 'display': ['data']}}
        okay_validated = check_utils.validate_check_setup(okay_setup)
        assert (okay_validated['indexing_progress'].get('module') == 'system_checks')
        # this one adds kwargs and id to setup
        okay_setup = {'indexing_progress': {'title': '', 'group': '', 'schedule': {'fake_sched': {'all': {}}}}}
        okay_validated = check_utils.validate_check_setup(okay_setup)
        assert ({'kwargs', 'dependencies'} <= set(okay_validated['indexing_progress']['schedule']['fake_sched']['all'].keys()))

    def test_get_action_strings(self):
        all_action_strings = check_utils.get_action_strings()
        for act_str in all_action_strings:
            assert (len(act_str.split('/')) == 2)
        # test a specific action
        one_act_str = check_utils.get_action_strings('patch_file_size')
        assert (one_act_str == 'wrangler_checks/patch_file_size')
        assert (one_act_str in all_action_strings)
        # test an action that doesn't exist
        bad_act_str = check_utils.get_check_strings('not_a_real_action')
        assert (bad_act_str is None)

    def test_get_schedule_names(self):
        schedules = check_utils.get_schedule_names()
        assert (isinstance(schedules, list))
        assert (len(schedules) > 0)

    def test_get_check_title_from_setup(self):
        title = check_utils.get_check_title_from_setup('indexing_progress')
        assert (title == check_utils.CHECK_SETUP['indexing_progress']['title'])

    def test_get_check_schedule(self):
        schedule = check_utils.get_check_schedule('morning_checks')
        assert (len(schedule) > 0)
        for env in schedule:
            assert (isinstance(schedule[env], list))
            for check_info in schedule[env]:
                assert len(check_info) == 3

        # test with conditions
        schedule_cond1 = check_utils.get_check_schedule('morning_checks', conditions=['put_env'])
        assert (0 < len(schedule_cond1) < len(schedule))
        # test with conditions that don't exist (ALL must match)
        schedule_cond2 = check_utils.get_check_schedule('morning_checks',
                                                        conditions=['put_env', 'fake_condition'])
        assert (len(schedule_cond2) == 0)

    def test_get_checks_within_schedule(self):
        checks_in_sched = check_utils.get_checks_within_schedule('morning_checks')
        assert (len(checks_in_sched) > 0)
        checks_in_sched = check_utils.get_checks_within_schedule('not_a_real_schedule')
        assert (len(checks_in_sched) == 0)

    @pytest.mark.parametrize('use_es', [True, False])
    def test_get_check_results(self, use_es):
        # dict to compare uuids
        uuid_compares = {}
        # will get primary results by default
        if not use_es:
            self.connection.connections['es'] = None
        all_res_primary = check_utils.get_check_results(self.connection)
        for check_res in all_res_primary:
            assert (isinstance(check_res, dict))
            assert ('name' in check_res)
            assert ('status' in check_res)
            assert ('uuid' in check_res)
            uuid_compares[check_res['name']] = check_res['uuid']
        # compare to latest results (which should be the same or newer)
        all_res_latest = check_utils.get_check_results(self.connection, use_latest=True)
        for check_res in all_res_latest:
            assert (isinstance(check_res, dict))
            assert ('name' in check_res)
            assert ('status' in check_res)
            assert ('uuid' in check_res)
            if check_res['name'] in uuid_compares:
                assert (check_res['uuid'] >= uuid_compares[check_res['name']])
        # get a specific check
        one_res = check_utils.get_check_results(self.connection, checks=['indexing_progress'])
        assert (len(one_res) == 1)
        assert (one_res[0]['name'] == 'indexing_progress')
        # bad check name
        test_res = check_utils.get_check_results(self.connection, checks=['not_a_real_check'])
        assert (len(test_res) == 0)

    def test_get_grouped_check_results(self):
        grouped_results = check_utils.get_grouped_check_results(self.connection)
        for group in grouped_results:
            assert ('_name' in group)
            assert (isinstance(group['_statuses'], dict))
            assert (len(group.keys()) > 2)

    @pytest.mark.flaky
    def test_run_check_or_action(self):
        test_uuid = datetime.datetime.utcnow().isoformat()
        check = run_result.CheckResult(self.connection, 'test_random_nums')
        # with a check (primary is True)
        test_info = ['test_checks/test_random_nums', {'primary': True, 'uuid': test_uuid}, [], 'xxx']
        check_res = check_utils.run_check_or_action(self.connection, test_info[0], test_info[1])
        assert (isinstance(check_res, dict))
        assert ('name' in check_res)
        assert ('status' in check_res)
        # make sure runtime is in kwargs and pop it
        assert ('runtime_seconds' in check_res.get('kwargs'))
        check_res.get('kwargs').pop('runtime_seconds')
        assert (check_res.get('kwargs') == {'primary': True, 'uuid': test_uuid, 'queue_action': 'Not queued'})
        primary_uuid = check_res.get('uuid')
        time.sleep(5)
        primary_res = check.get_primary_result()
        assert (primary_res.get('uuid') == primary_uuid)
        latest_res = check.get_latest_result()
        assert (latest_res.get('uuid') == primary_uuid)
        # with a check and no primary=True flag
        check_res = check_utils.run_check_or_action(self.connection, test_info[0], {})
        latest_uuid = check_res.get('uuid')
        assert ('runtime_seconds' in check_res.get('kwargs'))
        check_res.get('kwargs').pop('runtime_seconds')
        assert (check_res.get('kwargs') == {'primary': False, 'uuid': latest_uuid, 'queue_action': 'Not queued'})
        # latest res will be more recent than primary res now
        latest_res = check.get_latest_result()
        assert (latest_res.get('uuid') == latest_uuid)
        primary_res = check.get_primary_result()
        assert (primary_uuid < latest_uuid)

        # with an action
        action = run_result.ActionResult(self.connection, 'add_random_test_nums')
        act_kwargs = {'primary': True, 'uuid': test_uuid, 'check_name': 'test_random_nums',
                      'called_by': test_uuid}
        test_info_2 = ['test_checks/add_random_test_nums', act_kwargs, [] ,'xxx']
        action_res = check_utils.run_check_or_action(self.connection, test_info_2[0], test_info_2[1])
        assert (isinstance(action_res, dict))
        assert ('name' in action_res)
        assert ('status' in action_res)
        assert ('output' in action_res)
        # pop runtime_seconds kwarg
        assert ('runtime_seconds' in action_res['kwargs'])
        action_res['kwargs'].pop('runtime_seconds')
        assert (action_res.get('kwargs') == {'primary': True, 'offset': 0, 'uuid': test_uuid, 'check_name': 'test_random_nums', 'called_by': test_uuid})
        act_uuid = action_res.get('uuid')
        act_res = action.get_result_by_uuid(act_uuid)
        assert (act_res['uuid'] == act_uuid)
        latest_res = action.get_latest_result()
        assert (latest_res['uuid'] == act_uuid)
        # make sure the action can get its associated check result
        assc_check = action.get_associated_check_result(act_kwargs)
        assert (assc_check is not None)
        assert (assc_check['name'] == act_kwargs['check_name'])
        assert (assc_check['uuid'] == act_uuid)


    def test_run_check_errors(self):
        bad_check_group = [
            ['indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/item_counts_by_type', 'should_be_a_dict', [], 'xx1'],
            ['syscks/indexing_progress', {}, [], 'xx1'],
            ['wrangler_checks/iteasdts_by_type', {}, [], 'xx1'],
            ['test_checks/test_function_unused', {}, [], 'xx1']
        ]
        for bad_check_info in bad_check_group:
            check_res = check_utils.run_check_or_action(self.connection, bad_check_info[0], bad_check_info[1])
            assert not (isinstance(check_res, dict))
            assert ('ERROR' in check_res)

    def test_run_check_exception(self):
        check_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_check_error', {})
        assert (check_res['status'] == 'ERROR')
        # this output is a list
        assert ('by zero' in ''.join(check_res['full_output']))
        assert (check_res['description'] == 'Check failed to run. See full output.')

    def test_run_action_no_check_name_called_by(self):
        action_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_action_error', {})
        assert (action_res['status'] == 'FAIL')
        # this output is a list
        assert ('Action requires check_name and called_by in its kwargs' in ''.join(action_res['output']))
        assert (action_res['description'] == 'Action failed to run. See output.')

    def test_run_action_exception(self):
        action_res = check_utils.run_check_or_action(self.connection, 'test_checks/test_action_error', {'check_name': '', 'called_by': None})
        assert (action_res['status'] == 'FAIL')
        # this output is a list
        assert ('by zero' in ''.join(action_res['output']))
        assert (action_res['description'] == 'Action failed to run. See output.')
