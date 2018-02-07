from __future__ import print_function, unicode_literals
from .utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from .wrangler_utils import get_s3_utils_obj, get_FDN_connection
from dcicutils import ff_utils, s3_utils
import requests
import sys
import json
import datetime
import boto3

#### HELPER FUNCTIONS ####

def extract_info(res, fields):
    return {field: res[field] for field in fields if field in res}


def extract_list_info(res_list, fields, key_field):
    if not res_list:
        return {}
    results = {}
    for res in res_list:
        if key_field not in res:
            continue
        results[res[key_field]] = extract_info(res, fields)
    return results


def generate_report(curr_res, prev_res, field_path=[]):
    """
    Takes a dictionary current res and previous res and generates a report
    from them. Fields looked at are in report_fields and it will also
    recursively generate a report for all dictionary children that contain
    a uuid and are in children_fields.
    Returns None if there is no significant reporting; dict report otherwise.
    """
    significant_fields = {
        'status': ['released', 'released_to_project'] # significant statuses
    }
    report_fields = ['status']
    include_fields = ['@id']
    children_fields = ['experiments_in_set', 'files', 'processed_files']
    this_report = {'changes': []}
    for key, val in curr_res.items():
        if key in children_fields and isinstance(val, dict):
            for c_key, c_val in val.items():
                if isinstance(c_val, dict) and '@id' in c_val:
                    prev_child = prev_res.get(key, {}).get(c_key, {})
                    child_path = field_path + [key]
                    child_report = generate_report(c_val, prev_child, child_path)
                    if child_report:
                        this_report['changes'].extend(child_report['changes'])
        elif key in report_fields:
            curr_val = curr_res.get(key)
            prev_val = prev_res.get(key)
            if key in significant_fields:
                significant = significant_fields[key]
                is_sig = curr_val in significant or prev_val in significant
            else:
                # if this field is not in significant_fields, just see if changed
                is_sig = True
            if is_sig and curr_val != prev_val:
                this_report['changes'].append({
                    'field': '.'.join(field_path + [key]),
                    'previous': prev_val,
                    'current': curr_val,
                    '@id': curr_res['@id'],
                    'reason': ' '.join([key, 'changed from', str(prev_val), 'to', str(curr_val)])
                })
        elif key in include_fields:
            this_report[key] = val
    return this_report if this_report.get('changes') else None

#### CHECKS / ACTIONS #####

@check_function()
def experiment_set_reporting_data(connection, **kwargs):
    """
    Get a snapshot of all experiment sets, their experiments, and files of
    all of the above. Include uuid, accession, status, and md5sum (for files).
    """
    check = init_check_res(connection, 'experiment_set_reporting_data')
    check.status = 'PASS'
    fdn_conn = get_FDN_connection(connection)
    if not (fdn_conn and fdn_conn.check):
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check
    exp_sets = {}
    last_total = None
    curr_from = 0
    limit = 20
    while not last_total or last_total == limit:
        # sort by accession and grab 20 at a time to keep memory usage down
        search_query = ''.join(['/search/?type=ExperimentSetReplicate&experimentset_type=replicate&from=', str(curr_from), '&limit=', str(limit), '&sort=-date_created'])
        search_res = ff_utils.search_metadata(search_query, connection=fdn_conn, frame='embedded')
        if not search_res: # 0 results
            break
        last_total = len(search_res)
        curr_from += last_total
        for exp_set in search_res:
            exp_set_res = extract_info(exp_set, ['@id', 'status'])
            exp_set_res['processed_files'] = extract_list_info(
                exp_set.get('processed_files'),
                ['@id', 'status', 'file_type'],
                'accession'
            )
            exps = {}
            for exp in exp_set.get('experiments_in_set', []):
                exp_res = extract_info(exp, ['@id', 'status', 'experiment_type'])
                exp_res['files'] = extract_list_info(
                    exp.get('files'),
                    ['@id', 'status', 'file_type'],
                    'accession'
                )
                exp_res['processed_files'] = extract_list_info(
                    exp.get('processed_files'),
                    ['@id', 'status', 'file_type'],
                    'accession'
                )
                exps[exp['accession']] = exp_res
            exp_set_res['experiments_in_set'] = exps
            exp_sets[exp_set['accession']] = exp_set_res
    check.full_output = exp_sets
    return check


@check_function(auto_uuids=True, old_uuid=None, new_uuid=None)
def experiment_set_reporting(connection, **kwargs):
    """
    Diff two results of 'experiment_set_reporting_data' check.
    If auto_uuids is True, automatically finrd the uuids to compare:
    use the primary run and closest result to a day ago. Otherwise, use
    old_uuid and new_uuid are check uuids that are used to diff two results.

    Stores the information used by that action to build reports.
    """
    check = init_check_res(connection, 'experiment_set_reporting')
    check.action = 'build_experiment_set_reports'
    # find needed experiment_set_reporting_data results
    data_check = init_check_res(connection, 'experiment_set_reporting_data')
    if kwargs.get('auto_uuids') == False: # use manual uuids
        new_data_result = data_check.get_result_by_uuid(kwargs.get('new_uuid', 'miss'))
        old_data_result = data_check.get_result_by_uuid(kwargs.get('old_uuid', 'miss'))
    else:
        new_data_result = data_check.get_primary_result()
        old_data_result = data_check.get_closest_result(diff_hours=24)
    if not new_data_result or not old_data_result:
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are not available.'
        return check
    kwargs['new_uuid'] = new_data_result['uuid']
    kwargs['old_uuid'] = old_data_result['uuid']
    new_output = new_data_result.get('full_output')
    old_output = old_data_result.get('full_output')
    if not isinstance(new_output, dict) or not isinstance(old_output, dict):
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are malformed.'
        return check
    reports = []
    ### CREATE REPORTS... assuming experiment sets will NOT be deleted from DB
    for exp_set in new_output:
        old_res = old_output.get(exp_set, {})
        exp_set_report = generate_report(new_output[exp_set], old_res)
        if exp_set_report is not None:
            reports.append(exp_set_report)
    check.full_output = reports
    if reports:
        check.status = 'WARN'
        check.description = 'Ready to generate new experiment set reports.'
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.description = 'There are no new experiment set reports.'
    return check


@action_function()
def publish_experiment_set_reports(connection, **kwargs):
    action = init_action_res(connection, 'build_experiment_set_reports')
    report_check = init_check_res(connection, 'experiment_set_reporting')
    report_uuid = kwargs['called_by']
    report_result = report_check.get_result_by_uuid(report_uuid)
    report_output = report_result.get('full_output')
    action.output = {
        'reports': report_output,
        'called_by': report_uuid
    }
    action.status = 'DONE'
    return action
