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
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(':')] = int(split_str[1])
        ret[split_str[2].strip(':')] = int(split_str[3])
        return ret

    check = init_check_res(connection, 'item_counts_by_type', runnable=True)
    # run the check
    item_counts = {}
    warn_item_counts = {}
    server = connection.ff
    req_location = ''.join([server,'counts?format=json'])
    try:
        counts_res = requests.get(req_location, timeout=20)
    except:
        counts_res = None
    if counts_res is None or counts_res.status_code != 200:
        check.status = 'ERROR'
        check.description = 'Error connecting to the counts endpoint at: %s' % req_location
        return check
    counts_json = json.loads(counts_res.text)
    for index in counts_json['db_es_compare']:
        counts = process_counts(counts_json['db_es_compare'][index])
        item_counts[index] = counts
        if counts['DB'] != counts['ES']:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json['db_es_total'])
    item_counts['ALL'] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = 'FAIL'
        check.description = 'Error on fourfront health page.'
    elif warn_item_counts:
        check.status = 'WARN'
        check.description = 'DB and ES counts are not equal.'
        check.brief_output = warn_item_counts
    else:
        check.status = 'PASS'
    check.full_output = item_counts
    return check


@check_function()
def change_in_item_counts(connection, **kwargs):
    # use this check to get the comparison
    check = init_check_res(connection, 'change_in_item_counts', runnable=True)
    counts_check = init_check_res(connection, 'item_counts_by_type')
    latest = counts_check.get_primary_result()
    # get_item_counts run closest to 10 mins
    prior = counts_check.get_closest_result(diff_hours=24)
    if not latest.get('full_output') or not prior.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no counts_check results to run this check with.'
        return check
    diff_counts = {}
    # drill into full_output
    latest = latest['full_output']
    prior = prior['full_output']
    # get any keys that are in prior but not latest
    prior_unique = list(set(prior.keys()) - set(latest.keys()))
    for index in latest:
        if index == 'ALL':
            continue
        if index not in prior:
            diff_counts[index] = latest[index]['DB']
        else:
            diff_DB = latest[index]['DB'] - prior[index]['DB']
            if diff_DB != 0:
                diff_counts[index] = diff_DB
    for index in prior_unique:
        diff_counts[index] = -1 * prior[index]['DB']
    if diff_counts:
        check.status = 'WARN'
        check.full_output = diff_counts
        check.description = 'DB counts have changed in past day; positive numbers represent an increase in current counts.'
    else:
        check.status = 'PASS'
    return check


@check_function(item_type='Item')
def items_created_in_the_past_day(connection, **kwargs):
    item_type = kwargs.get('item_type')
    init_uuid = kwargs.get('uuid')
    check = init_check_res(connection, 'items_created_in_the_past_day', init_uuid=init_uuid, runnable=True)
    fdn_conn = get_FDN_connection(connection)
    if not (fdn_conn and fdn_conn.check):
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check
    # date string of approx. one day ago in form YYYY-MM-DD
    date_str = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    search_query = ''.join(['/search/?type=', item_type, '&limit=all&q=date_created:>=', date_str])
    search_res = ff_utils.search_metadata(search_query, connection=fdn_conn, frame='object')
    full_output = check.full_output if check.full_output else {}
    item_output = []
    for res in search_res:
        item_output.append({
            'uuid': res.get('uuid'),
            '@id': res.get('@id'),
            'date_created': res.get('date_created')
        })
    if item_output:
        full_output[item_type] = item_output
    check.full_output = full_output
    if full_output:
        check.status = 'WARN'
        check.description = 'Items have been created in the past day.'
        # create a ff_link
        check.ff_link = ''.join([connection.ff, 'search/?type=Item&limit=all&q=date_created:>=', date_str])
        # test admin output
        check.admin_output = check.ff_link
    else:
        check.status = 'PASS'
        check.description = 'No items have been created in the past day.'
    return check


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
            exp_set_res = extract_info(exp_set, ['@id', 'status','lab', 'award'])
            exp_set_res['lab'] = exp_set_res['lab']['@id']
            exp_set_res['award'] = exp_set_res['award']['@id']
            exp_set_res['processed_files'] = extract_list_info(
                exp_set.get('processed_files'),
                ['@id', 'status', 'md5sum'],
                'accession'
            )
            exps = {}
            for exp in exp_set.get('experiments_in_set', []):
                exp_res = extract_info(exp, ['@id', 'status'])
                exp_res['files'] = extract_list_info(
                    exp.get('files'),
                    ['@id', 'status', 'md5sum'],
                    'accession'
                )
                exp_res['processed_files'] = extract_list_info(
                    exp.get('processed_files'),
                    ['@id', 'status', 'md5sum'],
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
        report_fields = ['status', 'md5sum']
        include_fields = ['lab', 'award']
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
                        'item': curr_res['@id'],
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
    if latest_action is None or latest_action.get('output', {}).get('last_data_used') is None:
        # the action has not run before
        # store action as a reference point but don't actually run
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
    report_output = report_check.get('full_output')
    report_reference = report_check.get('admin_output')
    action.output = {
        'last_data_used': report_reference,
        'reports': report_output
    }
    if report_output:
        action.status = 'PASS'
    return action


@check_function(search_add_on='&limit=all')
def identify_files_without_filesize(connection, **kwargs):
    check = init_check_res(connection, 'identify_files_without_filesize', runnable=True)
    # must set this to be the function name of the action. also see ACTION_GROUPS in check_groups.py
    check.action = "patch_file_size"
    fdn_conn = get_FDN_connection(connection)
    if not (fdn_conn and fdn_conn.check):
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check
    search_url = '/search/?type=File&status=released%20to%20project&status=released&status=uploaded' + kwargs.get('search_add_on', '')
    search_res = ff_utils.search_metadata(search_url, connection=fdn_conn, frame='object')
    problem_files = []
    for hit in search_res:
        if hit.get('file_size') is None:
            hit_dict = {
                'accession': hit.get('accession'),
                'uuid': hit.get('uuid'),
                '@type': hit.get('@type'),
                'upload_key': hit.get('upload_key')
            }
            problem_files.append(hit_dict)
    check.full_output = problem_files
    if problem_files:
        check.status = 'WARN'
        check.description = "One or more files that are released/released to project/uploaded don't have file_size."
        check.allow_action = True # allows the action to be run
    else:
        check.status = 'PASS'
    return check


@action_function()
def patch_file_size(connection, **kwargs):
    action = init_action_res(connection, 'patch_file_size')
    s3_obj = get_s3_utils_obj(connection)
    fdn_conn = get_FDN_connection(connection)
    action_logs = {'s3_file_not_found': [], 'patch_failure': [], 'patch_success': []}
    # get latest results from identify_files_without_filesize
    filesize_check = init_check_res(connection, 'identify_files_without_filesize')
    check_latest = filesize_check.get_primary_result() # what we want is in full_output
    for hit in check_latest.get('full_output', []):
        bucket = s3_obj.outfile_bucket if 'FileProcessed' in hit['@type'] else s3_obj.raw_file_bucket
        head_info = s3_obj.does_key_exist(hit['upload_key'], bucket)
        if not head_info:
            action_logs['s3_file_not_found'].append(hit['accession'])
        else:
            patch_data = {'file_size': head_info['ContentLength']}
            try:
                ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], connection=fdn_conn)
            except Exception as e:
                acc_and_error = '\n'.join([hit['accession'], str(e)])
                action_logs['patch_failure'].append(acc_and_error)
            else:
                action_logs['patch_success'].append(hit['accession'])
    action.status = 'DONE'
    action.output = action_logs
    return action
