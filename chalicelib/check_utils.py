from __future__ import print_function, unicode_literals
from .utils import (
    get_methods_by_deco,
    check_method_deco,
    init_check_res,
    init_action_res,
    CHECK_DECO,
    ACTION_DECO,
    BadCheckSetup,
    basestring
)
# import modules that contain the checks
from .checks import *
from .checks import __all__ as CHECK_MODULES
from os.path import dirname
import glob
import sys
import importlib
import datetime
import copy
import json

# read in the check_setup.json and parse it
setup_paths = glob.glob(dirname(__file__)+"/check_setup.json")
if not len(setup_paths) == 1:
    raise BadCheckSetup('Exactly one check_setup.json must be present in chalicelib!')
with open(setup_paths[0], 'r') as jfile:
    CHECK_SETUP = json.load(jfile)
# a bit confusing, but the next two functions must be defined and run
# to validate CHECK_SETUP and process it


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


def validate_check_setup(check_setup):
    """
    Go through the check_setup json that was read in and make sure everything
    is properly formatted. Since scheduled kwargs and dependencies are
    optional, add those in at this point.

    Also takes care of ensuring that multiple checks were not written with the
    same name and adds check module information to the check setup. Accordingly,
    verifies that each check in the check_setup is a real check.
    """
    found_checks = {}
    all_check_strings = get_check_strings()
    # validate all checks
    for check_string in all_check_strings:
        check_mod, check_name = check_string.split('/')
        if check_name in found_checks:
            raise BadCheckSetup('More than one check with name "%s" was found. See module "%s"' % (check_name, check_mod))
        found_checks[check_name] = check_mod
    for check_name in check_setup:
        if check_name not in found_checks:
            raise BadCheckSetup('Check with name %s was in check_setup.json but does not have a proper check function defined.' % check_name)
        if not isinstance(check_setup[check_name], dict):
            raise BadCheckSetup('Entry for "%s" in check_setup.json must be a dictionary.' % check_name)
        # these fields are required
        if not {'title', 'group', 'schedule'} <= set(check_setup[check_name].keys()):
            raise BadCheckSetup('Entry for "%s" in check_setup.json must have the required keys: "title", "group", and "schedule".' % check_name)
        # these fields must be strings
        for field in ['title', 'group']:
            if not isinstance(check_setup[check_name][field], basestring):
                raise BadCheckSetup('Entry for "%s" in check_setup.json must have a string value for field "%s".' % (check_name, field))
        if not isinstance(check_setup[check_name]['schedule'], dict):
            raise BadCheckSetup('Entry for "%s" in check_setup.json must have a dictionary value for field "schedule".' % check_name)
        # now validate and add defaults to the schedule
        for sched_name, schedule in check_setup[check_name]['schedule'].items():
            if not isinstance(schedule, dict):
                raise BadCheckSetup('Schedule "%s" for "%s" in check_setup.json must have a dictionary value.' % (sched_name, check_name))
            for env_name, env_detail in schedule.items():
                if not isinstance(env_detail, dict):
                    raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must have a dictionary value.' % (env_name, sched_name, check_name))
                if 'id' not in env_detail:
                    raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must have a value for field "id"' % (env_name, sched_name, check_name))
                if not 'kwargs' in env_detail:
                    # default value
                    env_detail['kwargs'] = {'primary': True}
                if not 'dependencies' in env_detail:
                    env_detail['dependencies'] = []
        # lastly, add the check module information to each check in the setup
        check_setup[check_name]['module'] = found_checks[check_name]
    return check_setup


# Validate and finalize CHECK_SETUP
CHECK_SETUP = validate_check_setup(CHECK_SETUP)


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


def get_check_results(connection, checks=[], use_latest=False):
    """
    Initialize check results for each check in a group and get results stored
    in s3, sorted alphabetically
    By default, gets the 'primary' results. If use_latest is True, get the
    'latest' results instead.
    Using name = 'all' will return all non-test check strings
    """
    check_results = []
    if not checks:
        checks = [check_str.split('/')[1] for check_str in get_check_strings()]
    for check_name in checks:
        tempCheck = init_check_res(connection, check_name)
        if use_latest:
            found = tempCheck.get_latest_result()
        else:
            found = tempCheck.get_primary_result()
        # checks with no records will return None. Skip IGNORE checks
        if found and found.get('status') != 'IGNORE':
            check_results.append(found)
    # sort them alphabetically
    return sorted(check_results, key=lambda v: v['name'].lower())


def get_check_schedule(schedule_name):
    """
    Go through CHECK_SETUP and return all the required info for to run a given
    schedule for any environment.

    Returns a dictionary keyed by schedule, with inner dicts keyed by environ.
    The check running info is the standard format of:
    [<check_mod/check_str>, <kwargs>, <dependencies>, <id>]
    """
    check_schedule = {}
    for check_name, detail in CHECK_SETUP.items():
        if not schedule_name in detail['schedule']:
            continue
        for env_name, env_detail in detail['schedule'][schedule_name].items():
            check_str = '/'.join([detail['module'], check_name])
            run_info = [check_str, env_detail['kwargs'], env_detail['dependencies'], env_detail['id']]
            if env_name in check_schedule:
                check_schedule[env_name].append(run_info)
            else:
                check_schedule[env_name] = [run_info]
    # although not strictly necessary right now, this is a precaution
    return copy.deepcopy(check_schedule)


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
