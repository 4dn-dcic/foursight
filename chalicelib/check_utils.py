from __future__ import print_function, unicode_literals
from .utils import get_methods_by_deco, check_method_deco, check_function
from .checkresult import CheckResult
from .check_groups import *
import sys
import importlib

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
    """
    all_checks = []
    for check_mod in CHECK_MODULES:
        if globals().get(check_mod):
            methods = get_methods_by_deco(globals()[check_mod], check_function)
            for method in methods:
                check_str = '/'.join([check_mod, method.__name__])
                if specific_check and specific_check == method.__name__:
                    return check_str
                else:
                    all_checks.append(check_str)
    if specific_check:
        # if we've gotten here, it means the specific check was not checks_found
        return None
    else:
        return list(set(all_checks))


def run_check_group(connection, name):
    """
    For now return a simple list of check results
    """
    check_results = []
    check_group = fetch_check_group(name)
    if not check_group:
        return check_results
    for check_info in check_group:
        if len(check_info) != 3:
            check_results.append(' '.join(['ERROR with', str(check_info), 'in group:', name]))
        else:
            # nothing done with dependencies yet
            check_results.append(run_check(connection, check_info[0], check_info[1]))
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
        if len(check_info) != 3:
            continue
        check_name = check_info[0].strip().split('/')[1]
        TempCheck = CheckResult(connection.s3_connection, check_name)
        latest_results.append(TempCheck.get_latest_check())
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
        all_checks_group = [[check_str, {}, []] for check_str in all_checks_strs]
        return all_checks_group
    group = globals().get(name, None)
    # ensure it is non-empty list
    if not isinstance(group, list) or len(group) == 0:
        return None
    return group


def run_check(connection, check_str, check_kwargs):
    """
    Takes a FS_connection object, a check string formatted as: <str check module/name>
    and a dictionary of check arguments.
    For example:
    check_str: 'system_checks/my_check'
    check_kwargs: '{"foo":123}'
    Fetches the check function and runs it (returning whatever it returns)
    Return a string for failed results, CheckResult object otherwise
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
    if not check_method_deco(check_method, check_function):
        return ' '.join(['ERROR. Ensure the check_function decorator is present.', error_str])
    return check_method(connection, **check_kwargs)


"""
This class represents the entirety of the checks run in Foursight.
To create a new check, simply create a method for this class and add
the '@daily_check' or '@rate_check decorator to it.
This decorator MUST be used or the check will not be shown in fourfront.
@daily_check should be used for checks that are scheduled to run every day
using the app.run function, which runs at 10am UTC.
@rate_check should be used for any non-daily check that will have a cron/
rate defined for it in app.py.
@daily_check methods MAY be run at custom intervals, but @rate_check
methods will never run daily.

Each check method should initialize a CheckResult object, which holds the
name, status, output, and more for the check. This object should be
initialized using the init_check function, which MUST be passed a name
argument EXACTLY equal to the check name (i.e. method name).

For example, the 'elastic_beanstalk_health' check initilizes a CheckResult like so:
check = self.init_check('elastic_beanstalk_health')
Then, fields on that CheckResult (named check) can be easily set:
>> check.status = 'PASS'
Lastly, once the check is finished, finalize and store S3 results using:
>> return check.store_result()
Returning the result from store_result(), or a custom value, is
encouraged because the /run/ function uses these to report that tests
have succesfully run.
Returning None or no value without calling store_result() will effectively
abort the check.

You can get results from past/latest checks with any name in any check
method by initializing a CheckResult with the corresponding name.
For example, get the result of 'item_counts_by_type' check 24 hours ago:
>> counts_check = self.init_check('item_counts_by_type')
>> prior = counts_check.get_closest_check(24)
get_closest_check() returns a Python dict of the check result, which
can be interrogated in ways such as:
>> prior['status']
There is also a get_latest_check() method that returns the same type
of object for the latest result of a given check.
"""
