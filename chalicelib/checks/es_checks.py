import time
from ..run_result import CheckResult, ActionResult
from ..utils import (
    check_function,
    action_function,
)

@check_function()
def elasticsearch_s3_count_diff(connection, **kwargs):
    """ Reports the difference between the number of files on s3 and es """
    check = CheckResult(connection, 'elasticsearch_s3_count_diff')
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
        check.summary = check.description = 'There are >1000 checks not on ES'
    elif difference > 100:
        check.status = 'WARN'
        check.summary = check.description = 'There are >100 but <1000 checks not on ES'
    else:
        check.status = 'PASS'
        check.summary = check.description = 'There are <100 checks not on ES'
    check.full_output = full_output
    return check

@action_function()
def migrate_checks_to_es(connection, **kwargs):
    """
    Migrates checks from s3 to es. If a check name is given only those
    checks will be migrated
    """
    t0 = time.time()
    time_limit = 10000000 # very long as this is very slow too
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
        if round(time.time() - t0, 2) > time_limit:
            action.status = 'FAIL'
            action_logs['time out'] = True
            action_logs['n_migrated'] = n_migrated
            action.output = action_logs
            return action
        if es.get_object(key) is None:
            es.put_object(key, s3.get_object(key))
            n_migrated += 1
    action.status = 'DONE'
    action_logs['n_migrated'] = n_migrated
    action.output = action_logs
    return action

@action_function()
def clean_s3_es_checks(connection, **kwargs):
    """
    Cleans old checks from both s3 and es older than one month. Must be called
    from a specific check as it will take too long otherwise.
    """
    check_to_clean = kwargs.get('called_by')
    if not check_to_clean:
        action.status = 'FAIL'
        action.output = action_logs
        return action
    action = ActionResult(connection, check_to_clean)
    action_logs = {'time out': False}
    s3 = connection.connections['s3']
    es = connection.connections['es']
    one_month_ago = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    n_deleted = action.delete_results(prior_date=one_month_ago) # still needs to delete es
    action_logs['n_deleted'] = n_deleted
    action.status = 'DONE'
    action.output = action_logs
    return action