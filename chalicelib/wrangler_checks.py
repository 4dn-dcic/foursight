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

    check = init_check_res(connection, 'item_counts_by_type')
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
    check = init_check_res(connection, 'change_in_item_counts')
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
    check = init_check_res(connection, 'items_created_in_the_past_day', init_uuid=init_uuid)
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


@check_function(search_add_on='&limit=all')
def identify_files_without_filesize(connection, **kwargs):
    check = init_check_res(connection, 'identify_files_without_filesize')
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
        check.action_message = "Will attempt to patch file_size for %s files." % str(len(problem_files))
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
    filesize_check_result = filesize_check.get_result_by_uuid(kwargs['called_by'])
    for hit in filesize_check_result.get('full_output', []):
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
