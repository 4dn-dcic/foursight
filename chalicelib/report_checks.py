from __future__ import print_function, unicode_literals
from .utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from .wrangler_utils import get_FDN_connection
from dcicutils import ff_utils
import copy
import itertools

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


def calculate_report_from_change(path, prev, curr, add_ons):
    exp_type = add_ons.get('exp_type', 'UNKNOWN_TYPE')
    file_type = add_ons.get('file_type', 'UNKNOWN_TYPE')
    item_id = add_ons.get('@id', 'UNKNOWN_ID')
    set_id = add_ons.get('set_@id', 'UNKNOWN_ID')
    released_exp_set = add_ons.get('released_exp_set', False)
    released_exp = add_ons.get('released_exp', False)
    field = path.split('.')[-1]
    # these dictionaries define the reports
    report_info_w_released_exp_set = {
        'experiments_in_set.status': {
            '*/released' : {
                'severity': 1,
                'priority': 3,
                'summary': 'New Replicate Experiment has been added to a released %s Replicate Set.' % exp_type
            },
            '*/*': {
                'severity': 3,
                'priority': 4,
                'summary': 'WARNING! Replicate Experiment with status %s has been added to a released %s Replicate Set.' % (curr, exp_type)
            },
        },
        'processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New %s file added to released %s Replicate Set' % (file_type, exp_type)
            }
        }
    }
    report_info_w_released_exp = {
        'experiments_in_set.files.status': {
            '*/released': {
                'severity': 2,
                'priority': 5,
                'summary': 'New raw %s file added to released %s Replicate Experiment' % (file_type, exp_type)
            }
        },
        'experiments_in_set.files.status': {
            '*/*': {
                'severity': 3,
                'priority': 6,
                'summary': 'WARNING! New unreleased raw %s file added to released %s Replicate Experiment' % (file_type, exp_type)
            }
        },
        'experiments_in_set.processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New %s file added to released %s Replicate Experiment' % (file_type, exp_type)
            }
        }
    }
    report_info = {
        'status': {
            '*/released' : {
                'severity': 0,
                'priority': 0, # 0 is highest priority
                'summary': 'New %s Replicate Set has been released.' % exp_type
            },
            'replaced/released' : {
                'severity': 0,
                'priority': 0,
                'summary': 'New %s Replicate Set has been released.' % exp_type
            },
            'released/replaced' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s Replicate Set has been replaced.' % exp_type
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'WARNING! Released %s Replicate Set has changed to %s.' % (exp_type, curr)
            }
        },
        'experiments_in_set.status': {
            'released/replaced' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s Experiment has been replaced.' % exp_type
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'WARNING! Released %s Experiment has changed to %s.' % (exp_type, curr)
            },

        },
        'processed_files.status': {
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s Replicate Set has been replaced' % (file_type, exp_type)
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'WARNING! Released %s file has changed to %s' % (file_type, curr)
            }
        },
        'experiment_in_set.processed_files.status': {
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s Replicate Experiment has been replaced' % (file_type, exp_type)
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'WARNING! Released %s file has changed to %s' % (file_type, curr)
            }
        },
        'experiments_in_set.files.status': {
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s Replicate Experiment has been replaced' % (file_type, exp_type)
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'WARNING! Released %s file has changed to %s' % (file_type, curr)
            }
        }
    }
    if released_exp_set:
        report_info.update(report_info_w_released_exp_set)
    if released_exp:
        report_info.update(report_info_w_released_exp)
    # create the prev/val key
    # treat all equivalents as the first item in the list of equiv values
    # i.e. status = 'released_to_project' == status = 'released'
    equivalents = {
        'status': ['released', 'released to project']
    }
    significants = {
        'status': ['released', 'replaced']
    }
    if field in equivalents:
        prev = equivalents[field][0] if prev in equivalents[field] else prev
        curr = equivalents[field][0] if curr in equivalents[field] else curr
    if field in significants:
        prev_key = prev if prev in equivalents[field] else '*'
        curr_key = curr if curr in equivalents[field] else '*'
    else:
        prev_key, curr_key = prev, curr
    level_1 = report_info.get(path)
    if level_1:
        level_2 = level_1.get('/'.join([prev_key, curr_key]))
        if level_2:
            level_2['@id'] = item_id
            level_2['set_@id'] = set_id
            return level_2
    # no report found
    return None


def generate_exp_set_report(curr_res, prev_res, field_path=[], add_ons={}):
    add_ons = copy.copy(add_ons)
    reports = []
    report_fields = ['status']
    children_fields = ['experiments_in_set', 'files', 'processed_files']
    released_statuses = ['released', 'released to project']
    # set some top level exp set attributes in add_ons
    # top level because all exp types should be identical in a replicate set
    if field_path == []:
        exps_in_set = curr_res.get('experiments_in_set')
        if exps_in_set:
            first_exp_type = exps_in_set[list(exps_in_set.keys())[0]].get('experiment_type')
            if first_exp_type:
                add_ons['exp_type'] = first_exp_type
        add_ons['set_@id'] = curr_res.get('@id')
    for key, val in curr_res.items():
        path = '.'.join(field_path + [key])
        curr_val = curr_res.get(key)
        prev_val = prev_res.get(key)
        # START add_ons
        if path == 'status' and curr_val in released_statuses and prev_val in released_statuses:
            add_ons['released_exp_set'] = True
        if path == 'experiments_in_set.status' and curr_val in released_statuses and prev_val in released_statuses:
            add_ons['released_exp'] = True
        # END add_ons
        if key in children_fields and isinstance(val, dict):
            for c_key, c_val in val.items():
                if isinstance(c_val, dict) and '@id' in c_val:
                    prev_child = prev_res.get(key, {}).get(c_key, {})
                    child_path = field_path + [key]
                    child_reports = generate_exp_set_report(c_val, prev_child, child_path, add_ons)
                    if child_reports:
                        reports.append(child_reports)
        elif key in report_fields:
            add_ons['@id'] = curr_res['@id']
            report = calculate_report_from_change(path, prev_val, curr_val, add_ons)
            if report:
                reports.append(report)
    if reports:
        reports.sort(key = lambda r: r['priority'])
    return reports[0] if reports else None

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
            exp_set_res = extract_info(exp_set, ['@id', 'status', 'tags'])
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


@check_function(old_uuid=None, new_uuid=None)
def experiment_set_reporting(connection, **kwargs):
    """
    Diff two results of 'experiment_set_reporting_data' check.
    If old_uuid and new_uuid are provided, use those timestamps to find
    experiment_set_reporting_data results to diff. Otherwise, use the primary
    result and the one closest to 24 hours ago.

    Stores the information used by that action to build reports.
    """
    check = init_check_res(connection, 'experiment_set_reporting')
    check.action = 'publish_experiment_set_reports'
    # find needed experiment_set_reporting_data results
    data_check = init_check_res(connection, 'experiment_set_reporting_data')
    if kwargs.get('old_uuid') and kwargs.get('new_uuid'): # use manual uuids
        new_data_result = data_check.get_result_by_uuid(kwargs.get('new_uuid', 'miss'))
        old_data_result = data_check.get_result_by_uuid(kwargs.get('old_uuid', 'miss'))
    else:
        new_data_result = data_check.get_primary_result()
        old_data_result = data_check.get_closest_result(diff_hours=24)
    if not new_data_result or not old_data_result:
        import pdb; pdb.set_trace()
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are not available.'
        return check
    kwargs['new_uuid'] = new_data_result['uuid']
    kwargs['old_uuid'] = old_data_result['uuid']
    used_res_strings = ' Compared results for %s (new) to %s (old)' % (kwargs['new_uuid'], kwargs['old_uuid'])
    new_output = new_data_result.get('full_output')
    old_output = old_data_result.get('full_output')
    if not isinstance(new_output, dict) or not isinstance(old_output, dict):
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are malformed.' + used_res_strings
        return check
    reports = []
    used_tag = '4DN Joint Analysis 2018'
    ### CREATE REPORTS... assuming experiment sets will NOT be deleted from DB
    for exp_set in new_output:
        new_res = new_output[exp_set]
        if used_tag not in new_res.get('tags', []):
            continue
        old_res = old_output.get(exp_set, {})
        exp_set_report = generate_exp_set_report(new_res, old_res)
        if exp_set_report is not None:
            reports.append(exp_set_report)
    # reports are not grouped at all. sort them by summary first
    reports.sort(key=lambda r: r['summary'])
    check.full_output = reports
    group_reports = []
    # group reports by summary
    for key, group in itertools.groupby(reports, lambda r: r['summary']):
        # first item
        first_report = group.__next__()
        group_report = {r_key: first_report[r_key] for r_key in ['severity', 'summary']}
        first_report_items = {'set_@id': first_report['set_@id'], '@id': first_report['@id']}
        group_report['items'] = [first_report_items]
        # use this checks uuid as the timestamp for the reports
        group_report['timestamp'] = kwargs['uuid']
        group_report['report_tag'] = used_tag
        group_report['newer_bound'] = kwargs['new_uuid']
        group_report['older_bound'] = kwargs['old_uuid']
        for gr in group: # iterate through the rest of the items
            group_report['items'].append({'set_@id': gr['set_@id'], '@id': gr['@id']})
        group_reports.append(group_report)
    check.brief_output = group_reports
    if group_reports:
        check.status = 'WARN'
        check.description = 'Ready to publish new experiment set reports.' + used_res_strings
        check.action_message = 'Will publish %s  grouped reports. See brief_output.' % str(len(group_reports))
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.description = 'There are no new experiment set reports.' + used_res_strings
    return check


@action_function()
def publish_experiment_set_reports(connection, **kwargs):
    action = init_action_res(connection, 'publish_experiment_set_reports')
    report_check = init_check_res(connection, 'experiment_set_reporting')
    report_uuid = kwargs['called_by']
    report_result = report_check.get_result_by_uuid(report_uuid)
    report_output = report_result.get('brief_output')
    action.description = "Does not actually do anything yet!"
    action.output = {
        'reports': report_output
    }
    action.status = 'DONE'
    return action
