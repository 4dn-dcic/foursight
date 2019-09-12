from ..run_result import CheckResult, ActionResult
from ..utils import (
    check_function,
    action_function,
)

@check_function()
def elasticsearch_s3_count_diff(connection):
    """ Reports the difference between the number of files on s3 and es """
    check = CheckResult(connection, 'elasticsearch_s3_count_diff')
    s3 = connection.connections['s3']
    es = connection.connections['es']
    n_s3_keys = len(s3.list_all_keys())
    n_es_keys = len(es.list_all_keys())
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
def migrate_checks_to_es(connection, check=None):
    """
    Migrates checks from s3 to es. If a check name is given only those
    checks will be migrated
    """
    t0 = time.time()
    time_limit = 270 # 4.5 minutes
    action = ActionResult(connection, 'migrate_checks_to_es')
    action_logs = {'time out': False}
    s3 = connection.connections['s3']
    es = connection.connections['es']
    s3_keys = s3.get_all_keys()
    if check is not None:
        s3_keys = list(filter(lambda k: return check in k, s3_keys))
    n_migrated = 0
    for key in s3_keys:
        if round(time.time() - t0, 2) > time_limit:
            action.status = 'FAIL'
            action_logs['time out'] = True
            action.output = action_logs
            return action
        if es.get_object(key) is None:
            es.put_object(key, s3.get_object(key))
            n_migrated += 1
    action.status = 'DONE'
    action_logs['n_migrated'] = n_migrated
    action.output = action_logs
    return action
