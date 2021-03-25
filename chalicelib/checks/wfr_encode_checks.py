from datetime import datetime
from dcicutils import ff_utils
from dcicutils import s3Utils
from .helpers import wfr_utils
from .helpers import wfrset_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *

lambda_limit = wfr_utils.lambda_limit


@check_function(lab_title=None, start_date=None)
def chipseq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'in_situ_hic_status')
    my_auth = connection.ff_keys
    check.action = "in_situ_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'ChIP-seq'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check

            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(completed)

    # complete check values
    check.summary = ""
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['skipped']:
        check.summary += str(len(check.full_output['skipped'])) + ' skipped|'
        check.status = 'WARN'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'
    return check


@action_function(start_runs=True, patch_completed=True)
def chipseq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'chipseq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    chipseq_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = chipseq_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = chipseq_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action
