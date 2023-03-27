import time
import datetime

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


@check_function(action="migrate_checks_to_es")
def elasticsearch_s3_count_diff(connection, **kwargs):
    """ Reports the difference between the number of files on s3 and es """
    check = CheckResult(connection, 'elasticsearch_s3_count_diff')
    check.action = 'migrate_checks_to_es'
    s3 = connection.connections['s3']
    es = connection.connections['es']
    n_s3_keys = s3.get_size()
    n_es_keys = es.get_size()
    difference = n_s3_keys - n_es_keys
    full_output = {}
    full_output['n_s3_keys'] = n_s3_keys
    full_output['n_es_keys'] = n_es_keys
    full_output['difference'] = difference
    if difference > 1000:
        check.status = 'FAIL'
        check.allow_action = True
        check.summary = check.description = 'There are >1000 checks not on ES'
    elif difference > 100:
        check.status = 'WARN'
        check.allow_action = True
        check.summary = check.description = 'There are >100 but <1000 checks not on ES'
    else:
        check.status = 'PASS'
        check.allow_action = False
        check.summary = check.description = 'There are <100 checks not on ES'
    check.full_output = full_output
    return check


@action_function(timeout=270)
def migrate_checks_to_es(connection, **kwargs):
    """
    Migrates checks from s3 to es. If a check name is given only those
    checks will be migrated
    """
    t0 = time.time()
    time_limit = 270 if kwargs.get('timeout') is None else kwargs.get('timeout')
    action = ActionResult(connection, 'migrate_checks_to_es')
    action_logs = {'time out': False}
    s3 = connection.connections['s3']
    es = connection.connections['es']
    check = kwargs.get('check')
    if check is not None:
        action.description = 'Migrating check %s from s3 to ES' % check
        s3_keys = s3.list_all_keys_w_prefix(check)
    else:
        action.description = 'Migrating all checks from s3 to ES'
        s3_keys = s3.list_all_keys()
    n_migrated = 0
    for key in s3_keys:
        if kwargs.get('timeout') and round(time.time() - t0, 2) > time_limit:
            action_logs['time out'] = True
            break
        if 'action_records' in key: # ignore action_records for now
            continue
        if es.put_object(key, s3.get_object(key)): # put object by default
            n_migrated += 1
    action.status = 'DONE'
    action_logs['n_migrated'] = n_migrated
    action.output = action_logs
    return action


@check_function(timeout=270, days=30, to_clean=None)
def clean_s3_es_checks(connection, **kwargs):
    """
    Cleans old checks from both s3 and es older than one month. Must be called
    from a specific check as it will take too long otherwise.
    """
    check_to_clean = kwargs.get('to_clean')
    time_limit = kwargs.get('timeout')
    days_back = kwargs.get('days')
    check = CheckResult(connection, 'clean_s3_es_checks')
    full_output = {}
    if check_to_clean is None:
        check.status = 'WARN'
        check.summary = check.description = 'A check must be given to be cleaned'
        check.full_output = full_output
        return check
    clean_check = CheckResult(connection, check_to_clean)
    past_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_back)
    n_deleted_s3, n_deleted_es = clean_check.delete_results(prior_date=past_date, timeout=time_limit)
    full_output['check_cleared'] = check_to_clean
    full_output['n_deleted_s3'] = n_deleted_s3
    full_output['n_deleted_es'] = n_deleted_es
    check.status = 'DONE'
    check.full_output = full_output
    return check
