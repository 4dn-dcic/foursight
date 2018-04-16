from __future__ import print_function, unicode_literals
from .utils import (
    get_methods_by_deco,
    check_method_deco,
    init_check_res,
    init_action_res,
    CHECK_DECO,
    ACTION_DECO
)
from .check_groups import *
import sys
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
    Return a list of all formatted check strings (<module>/<check_name>) in system.
    By default runs on all checks (specific_check == None), but can be used
    to get the check string of a certain check name as well.

    IMPORTANT: any checks in test_checks module are excluded.
    """
    all_checks = []
    for check_mod in CHECK_MODULES:
        if globals().get(check_mod):
            methods = get_methods_by_deco(globals()[check_mod], CHECK_DECO)
            for method in methods:
                check_str = '/'.join([check_mod, method.__name__])
                if specific_check and specific_check == method.__name__:
                    return check_str
                elif check_mod != 'test_checks':
                    all_checks.append(check_str)
    if specific_check:
        # if we've gotten here, it means the specific check was not checks_found
        return None
    else:
        return list(set(all_checks))


def get_action_strings(specific_action=None):
    """
    Basically the same thing as get_check_strings, but for actions...
    """
    all_actions = []
    for check_mod in CHECK_MODULES:
        if globals().get(check_mod):
            methods = get_methods_by_deco(globals()[check_mod], ACTION_DECO)
            for method in methods:
                act_str = '/'.join([check_mod, method.__name__])
                if specific_action and specific_action == method.__name__:
                    return act_str
                elif check_mod != 'test_checks':
                    all_actions.append(act_str)
    if specific_action:
        # if we've gotten here, it means the specific action was not found
        return None
    else:
        return list(set(all_actions))


def get_check_group_results(connection, name, use_latest=False):
    """
    Initialize check results for each check in a group and get latest results,
    sorted alphabetically
    By default, gets the 'primary' results. If use_latest is True, get the
    'latest' results instead.
    Using name = 'all' will return all non-test check strings
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
        if use_latest:
            found = tempCheck.get_latest_result()
        else:
            found = tempCheck.get_primary_result()
        # checks with no records will return None. Skip IGNORE checks
        if found and found.get('status') != 'IGNORE':
            latest_results.append(found)
    # sort them alphabetically
    latest_results = sorted(latest_results, key=lambda v: v['name'].lower())
    return latest_results


def fetch_check_group(name):
    """
    Will be none if the group is not defined.
    Special case for 'all', which gets all checks and uses default kwargs
    """
    if name == 'all':
        all_checks = get_check_strings()
        return [[check_str, {}, [], ''] for check_str in all_checks]
    group = CHECK_GROUPS.get(name, None)
    # maybe it's a test groups
    if not group:
        group = TEST_CHECK_GROUPS.get(name, None)
    # ensure it is non-empty list
    if not isinstance(group, list) or len(group) == 0:
        return None
    # copy it and return
    return copy.deepcopy(group)


def run_check_or_action(connection, check_str, check_kwargs):
    """
    Does validation of proviced check_str, it's module, and kwargs.
    Determines by decorator whether the method is a check or action, then runs
    it. All errors are taken care of within the running of the check/action.

    Takes a FS_connection object, a check string formatted as: <str check module/name>
    and a dictionary of check arguments.
    For example:
    check_str: 'system_checks/my_check'
    check_kwargs: '{"foo":123}'
    Fetches the check function and runs it (returning whatever it returns)
    Return a string for failed results, CheckResult/ActionResult object otherwise.
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
    if not check_method_deco(check_method, CHECK_DECO) and not check_method_deco(check_method, ACTION_DECO):
        return ' '.join(['ERROR. Check or action must use a decorator.', error_str])
    return check_method(connection, **check_kwargs)


def init_check_or_action_res(connection, check):
    """
    Use in cases where a string is provided that could be a check or an action
    Returns None if neither are valid. Tries checks first then actions.
    If successful, returns a CheckResult or ActionResult
    """
    is_action = False
    # determine whether it is a check or action
    check_str = get_check_strings(check)
    if not check_str:
        check_str = get_action_strings(check)
        is_action = True
    if not check_str: # not a check or an action. abort
        return None
    return init_action_res(connection, check) if is_action else init_check_res(connection, check)
