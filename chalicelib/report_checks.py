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


@check_function()
def experiment_set_reporting_data(connection, **kwargs):
    """
    Get a snapshot of all experiment sets, their experiments, and files of
    all of the above. Include uuid, accession, status, and md5sum (for files).
    """
    # helper functions
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

    check = init_check_res(connection, 'experiment_set_reporting_data')
    check.status = 'IGNORE'
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


@check_function()
def experiment_set_reporting(connection, **kwargs):
    """
    Diff two results of 'experiment_set_reporting_data' check.
    uuid of the previous result to compare with is found from latest run
    'build_experiment_set_reports' action.
    Stores the information used by that action to build reports.
    """
    # helper function
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

    check = init_check_res(connection, 'experiment_set_reporting', runnable=True)
    check.action = 'build_experiment_set_reports'
    # build reference to the check that provides data and get information
    data_check = init_check_res(connection, 'experiment_set_reporting_data')
    latest_data_result = data_check.get_primary_result()
    if not latest_data_result:
        check.status = 'ERROR'
        check.description = 'experiment_set_reporting_data results are not available.'
        return check
    action_result = init_action_res(connection, 'build_experiment_set_reports')
    latest_action = action_result.get_latest_result()
    if latest_action is None or latest_action['status'] != 'DONE':
        # the action has not run before
        # store action as a reference point but don't actually run
        action_result.status = 'DONE'
        action_result.output = {
            'last_data_used': latest_data_result['uuid'],
            'reports': []
        }
        action_result.store_result()
        check.status = 'PASS'
        check.description = 'Experiment set reporting is initialized.'
        return check
    last_data_used = latest_action['output']['last_data_used']
    last_data_key = ''.join([data_check.name, '/', last_data_used, data_check.extension])
    last_data_result = data_check.get_s3_object(last_data_key)
    latest_output = latest_data_result['full_output']
    last_output = last_data_result['full_output']
    if not isinstance(latest_output, dict) or not isinstance(last_output, dict):
        check.status = 'ERROR'
        check.description = 'experiment_set_reporting_data results are malformed.'
        return check
    reports = []
    ### CREATE REPORTS... assuming experiment sets will NOT be deleted from DB
    for exp_set in latest_output:
        last_res = last_output.get(exp_set, {})
        exp_set_report = generate_report(latest_output[exp_set], last_res)
        if exp_set_report is not None:
            reports.append(exp_set_report)
    check.full_output = reports
    if reports:
        # store new reference report in admin_output
        check.admin_output = latest_data_result['uuid']
        check.status = 'WARN'
        check.description = 'Ready to generate new experiment set reports.'
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.description = 'There are no new experiment set reports.'
    return check


@action_function()
def build_experiment_set_reports(connection, **kwargs):
    action = init_action_res(connection, 'build_experiment_set_reports')
    report_check = init_check_res(connection, 'experiment_set_reporting')
    report_result = report_check.get_primary_result()
    report_output = report_result.get('full_output')
    report_reference = report_result.get('admin_output')
    action.output = {
        'last_data_used': report_reference,
        'reports': report_output
    }
    action.status = 'DONE'
    return action
