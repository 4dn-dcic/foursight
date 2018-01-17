from __future__ import print_function, unicode_literals
from .utils import get_methods_by_deco, check_method_deco, init_check_res, init_action_res, CHECK_DECO, ACTION_DECO
from .check_groups import *
import sys
import traceback
import importlib
import datetime
import copy

# import modules that contain the checks
for check_mod in CHECK_MODULES:
    try:
        globals()[check_mod] = importlib.import_module('.'.join(['chalicelib', check_mod]))
    except ImportError:
        print(''.join(['ERROR importing checks from ', check_mod]), file=sys.stderr)


def get_check_strings(specific_check=None):
    """
    Return check formatted check strings (<module>/<check_name>) for checks.
    By default runs on all checks (specific_check == None), but can be used
    to get the check string of a certain check name as well.

    IMPORTANT: any checks in test_checks module are excluded from 'all'
    """
    all_checks = []
    for check_mod in CHECK_MODULES:
        if globals().get(check_mod):
            methods = get_methods_by_deco(globals()[check_mod], CHECK_DECO)
            for method in methods:
                check_str = '/'.join([check_mod, method.__name__])
                if specific_check and specific_check == method.__name__:
                    return check_str
                elif check_mod != 'test_checks': # exclude test checks from 'all'
                    all_checks.append(check_str)
    if specific_check:
        # if we've gotten here, it means the specific check was not checks_found
        return None
    else:
        return list(set(all_checks))


def run_check_group(connection, name):
    """
    This is a test function, deprecated in favor of app_utils.queue_check_group.
    The issue is that run_check_group will run checks synchronously in one lambda.
    """
    check_results = []
    check_group = fetch_check_group(name)
    if not check_group:
        return check_results
    group_timestamp = datetime.datetime.utcnow().isoformat()
    for check_info in check_group:
        if len(check_info) != 4:
            check_results.append(' '.join(['ERROR with', str(check_info), 'in group:', name]))
        else:
            # add uuid to each kwargs dict if not already specified
            # this will have the effect of giving all checks the same id
            # and combining results from repeats in the same check_group
            [check_str, check_kwargs, check_deps, dep_id] = check_info
            if 'uuid' not in check_kwargs:
                check_kwargs['uuid'] = group_timestamp
            # nothing done with dependencies yet
            result = run_check_or_action(connection, check_str, check_kwargs)
            if result:
                check_results.append(result)
    return check_results


def get_check_group_latest(connection, name):
    """
    Initialize check results for each check in a group and get latest results,
    sorted alphabetically
    """
    latest_results = []
    check_group = fetch_check_group(name)
    if not check_group:
        return latest_results
    for check_info in check_group:
        if len(check_info) != 4:
            continue
        check_name = check_info[0].strip().split('/')[1]
        tempCheck = init_check_res(connection, check_name)
        found = tempCheck.get_latest_result()
        # checks with no records will return None. Skip IGNORE checks
        if found and found.get('status') != 'IGNORE':
            latest_results.append(found)
    # sort them alphabetically
    latest_results = sorted(latest_results, key=lambda v: v['name'].lower())
    return latest_results


def fetch_check_group(name):
    """
    Will be none if the group is not defined.
    Special case for all_checks, which gets all checks and uses default kwargs
    """
    if name == 'all':
        all_checks_strs = get_check_strings()
        # dependecy id's are not used (since there are no dependencies) so arbitrarily set to '_'
        all_checks_group = [[check_str, {}, [], '_'] for check_str in all_checks_strs]
        return all_checks_group
    group = CHECK_GROUPS.get(name, None)
    # maybe it's a test groups
    if not group:
        group = TEST_CHECK_GROUPS.get(name, None)
    # ensure it is non-empty list
    if not isinstance(group, list) or len(group) == 0:
        return None
    # copy it and return
    return copy.deepcopy(group)


def fetch_action_group(name):
    """
    Used only for ACTION_GROUPS, which mix actions and checks. Does NOT use 'all'
    """
    group = ACTION_GROUPS.get(name, None)
    # maybe it's a test groups
    if not group:
        group = TEST_ACTION_GROUPS.get(name, None)
    # ensure it is non-empty list
    if not isinstance(group, list) or len(group) == 0:
        return None
    # copy it and return
    return copy.deepcopy(group)


def run_check_or_action(connection, check_str, check_kwargs):
    """
    Does validation of check_str and check_kwargs that would be passed to either run_check or run_action.
    Determines by decorator whether the method is a check or action, then passes it to the appropriate
    function (run_check or run_action)

    Takes a FS_connection object, a check string formatted as: <str check module/name>
    and a dictionary of check arguments.
    For example:
    check_str: 'system_checks/my_check'
    check_kwargs: '{"foo":123}'
    Fetches the check function and runs it (returning whatever it returns)
    Return a string for failed results, CheckResult/ActionResult object otherwise.
    If the check code itself fails, then an Errored CheckResult is stored for
    easier debugging.
    """
    # make sure parameters are good
    error_str = ' '.join(['Info: CHECK:', str(check_str), 'KWARGS:', str(check_kwargs)])
    if len(check_str.strip().split('/')) != 2:
        return ' '.join(['ERROR. Check string must be of form module/check_name.', error_str])
    check_mod_str = check_str.strip().split('/')[0]
    check_name_str = check_str.strip().split('/')[1]
    if not isinstance(check_kwargs, dict):
        return ' '.join(['ERROR. Check kwargs must be a dict.', error_str])
    check_mod = globals().get(check_mod_str)
    if not check_mod:
        return ' '.join(['ERROR. Check module is not valid.', error_str])
    check_method = check_mod.__dict__.get(check_name_str)
    if not check_method:
        return ' '.join(['ERROR. Check name is not valid.', error_str])
    if check_method_deco(check_method, CHECK_DECO):
        return run_check(connection, check_name, check_method, check_kwargs)
    elif check_method_deco(check_method, ACTION_DECO):
        return run_action(connection, check_name, check_method, check_kwargs)
    else:
        return ' '.join(['ERROR. Ensure the correct function decorator is present.', error_str]), None, None
    return 'PASS', check_name, check_method




def run_check(connection, check_name, check_method, check_kwargs):
    """
    Meant to be run from run_check_or_action.
    Takes a connection, str check_name, check method (fxn), and dict check_kwargs.
    Runs the check and returns a dict of results. On an error, stores a stack trace of the error in
    full_output and stores the check with an ERROR.
    """
    try:
        check_result = check_method(connection, **check_kwargs)
    except Exception as e:
        err_check = init_check_res(connection, check_name)
        err_check.status = 'ERROR'
        err_check.description = 'Check failed to run. See full output.'
        err_check.full_output = traceback.format_exc().split('\n')
        check_result = err_check.store_result()
    return check_result


def run_action(connection, act_name, act_method, act_kwargs):
    """
    Same as run_check, but meant for action. Arguments should be formatted the same way.
    On error, stack trace is present in output and status will be set to FAIL.
    """
    try:
        act_result = act_method(connection, **act_kwargs)
    except Exception as e:
        err_action = init_check_res(connection, act_name)
        err_action.status = 'FAIL'
        err_action.description = 'Action failed to run. See full output.'
        err_action.output = traceback.format_exc().split('\n')
        act_result = err_action.store_result()
    return act_result
