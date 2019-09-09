from __future__ import print_function, unicode_literals
from .utils import (
    get_methods_by_deco,
    check_method_deco,
    init_check_res,
    init_action_res,
    CHECK_DECO,
    ACTION_DECO,
    BadCheckSetup,
    basestring,
    list_environments
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
# a bit confusing, but the next three functions must be defined and run
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


def get_checks_within_schedule(schedule_name):
    """
    Simply return a list of string check names within the given schedule
    """
    checks_in_schedule = []
    for check_name, detail in CHECK_SETUP.items():
        if not schedule_name in detail['schedule']:
            continue
        checks_in_schedule.append(check_name)
    return checks_in_schedule


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
    all_environments = list_environments() + ['all', 'all_4dn']
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
        # make sure a display is set up if there is no schedule
        if check_setup[check_name]['schedule'] == {} and check_setup[check_name].get('display') is None:
            raise BadCheckSetup('Entry for "%s" in check_setup.json must have a list of "display" environments if it lacks a schedule.' % check_name)
        # now validate and add defaults to the schedule
        for sched_name, schedule in check_setup[check_name]['schedule'].items():
            if not isinstance(schedule, dict):
                raise BadCheckSetup('Schedule "%s" for "%s" in check_setup.json must have a dictionary value.' % (sched_name, check_name))
            for env_name, env_detail in schedule.items():
                if not env_name in all_environments:
                    raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json is not an existing'
                                        ' environment. Create with PUT to /environments endpoint.'
                                        % (env_name, sched_name, check_name))
                if not isinstance(env_detail, dict):
                    raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must have a dictionary value.' % (env_name, sched_name, check_name))
                # default values
                if not 'kwargs' in env_detail:
                    env_detail['kwargs'] = {'primary': True}
                else:
                    if not isinstance(env_detail['kwargs'], dict):
                        raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must have a dictionary value for "kwargs".' % (env_name, sched_name, check_name))
                if not 'dependencies' in env_detail:
                    env_detail['dependencies'] = []
                else:
                    if not isinstance(env_detail['dependencies'], list):
                        raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must have a list value for "dependencies".' % (env_name, sched_name, check_name))
                    else:
                        # confirm all dependencies are legitimate check names
                        for dep_id in env_detail['dependencies']:
                            if dep_id not in get_checks_within_schedule(sched_name):
                                raise BadCheckSetup('Environment "%s" in schedule "%s" for "%s" in check_setup.json must has a dependency "%s" that is not a valid check name that shares the same schedule.' % (env_name, sched_name, check_name, dep_id))

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


def get_schedule_names():
    """
    Simply return a list of all valid schedule names, as defined in CHECK_SETUP
    """
    schedules = set()
    for _, detail in CHECK_SETUP.items():
        for schedule in detail.get('schedule', []):
            schedules.add(schedule)
    return list(schedules)


def get_check_title_from_setup(check_name):
    """
    Return a title of a check from CHECK_SETUP
    If not found, just return check_name
    """
    return CHECK_SETUP.get(check_name, {}).get("title", check_name)


def get_check_schedule(schedule_name, conditions=None):
    """
    Go through CHECK_SETUP and return all the required info for to run a given
    schedule for any environment.

    If a list of conditions is provided, filter the schedule to only include
    checks that match ALL of the conditions.

    Returns a dictionary keyed by environ.
    The check running info is the standard format of:
    [<check_mod/check_str>, <kwargs>, <dependencies>]
    """
    check_schedule = {}
    for check_name, detail in CHECK_SETUP.items():
        if not schedule_name in detail['schedule']:
            continue
        # skip the check if conditions provided and any are not met
        if conditions and isinstance(conditions, list):
            check_conditions = detail.get('conditions', [])
            if any([cond not in check_conditions for cond in conditions]):
                continue
        for env_name, env_detail in detail['schedule'][schedule_name].items():
            check_str = '/'.join([detail['module'], check_name])
            run_info = [check_str, env_detail['kwargs'], env_detail['dependencies']]
            if env_name in check_schedule:
                check_schedule[env_name].append(run_info)
            else:
                check_schedule[env_name] = [run_info]
    # although not strictly necessary right now, this is a precaution
    return copy.deepcopy(check_schedule)


def get_check_results(connection, checks=[], use_latest=False):
    """
    Initialize check results for each desired check and get results stored
    in s3, sorted by status and then alphabetically by title.
    May provide a list of string check names as `checks`; otherwise get all
    checks by default.
    By default, gets the 'primary' results. If use_latest is True, get the
    'latest' results instead.
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
    # sort them by status and then alphabetically by check_setup title
    stat_order = ['ERROR', 'FAIL', 'WARN', 'PASS']
    return sorted(
        check_results,
        key=lambda v: (stat_order.index(v['status']) if v['status'] in stat_order else 9, get_check_title_from_setup(v['name']).lower())
    )


def get_grouped_check_results(connection):
    """
    Return a group-centric view of the information from get_check_results for
    given connection (i.e. fs environment).
    Returns a list of dicts dict that contains dicts of check results
    keyed by title and also counts of result statuses and group name.
    All groups are returned
    """
    grouped_results = {}
    check_res = get_check_results(connection)
    for res in check_res:
        setup_info = CHECK_SETUP.get(res['name'])
        # this should not happen, but fail gracefully
        if not setup_info:
            print('-VIEW-> Check %s not found in CHECK_SETUP for env %s' % (res['name'], connection.fs_env))
            continue
        # make sure this environment displays this check
        used_envs = [env for sched in setup_info['schedule'].values() for env in sched]
        used_envs.extend(setup_info.get('display', []))
        if (connection.fs_env in used_envs or 'all' in used_envs or
            ('all_4dn' in used_envs and 'cgap' not in connection.fs_env)):
            group = setup_info['group']
            if group not in grouped_results:
                grouped_results[group] = {}
                grouped_results[group]['_name'] = group
                grouped_results[group]['_statuses'] = {'ERROR': 0, 'FAIL': 0, 'WARN': 0, 'PASS': 0}
            grouped_results[group][setup_info['title']] = res
            if res['status'] in grouped_results[group]['_statuses']:
                grouped_results[group]['_statuses'][res['status']] += 1
    # format into a list and sort alphabetically
    grouped_list = [group for group in grouped_results.values()]
    return sorted(grouped_list, key=lambda v: v['_name'])


def run_check_or_action(connection, check_str, check_kwargs):
    """
    Does validation of provided check_str, it's module, and kwargs.
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
