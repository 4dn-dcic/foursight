from __future__ import print_function, unicode_literals
from .utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from .wrangler_utils import (
    get_FDN_connection,
    safe_search_with_callback,
    parse_datetime_to_utc
)
from dcicutils import ff_utils
import copy
import itertools
import datetime

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
    equivalents = add_ons.get('equivalents', {})
    significants = add_ons.get('significants', {})
    field = path.split('.')[-1]
    # these dictionaries define the reports
    report_info_w_released_exp_set = {
        'experiments_in_set.status': {
            '*/released' : {
                'severity': 1,
                'priority': 3,
                'summary': 'New replicate experiment has been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'new replicate experiments have been added to released %s replcate sets.' % exp_type,
            },
            '*/*': {
                'severity': 3,
                'priority': 4,
                'summary': 'Replicate experiment with status %s has been added to a released %s replicate set.' % (curr, exp_type),
                'summary_plural': 'new replicate experiments with status %s have been added to released %s replicate sets.' % (curr, exp_type),
            },
        },
        'processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New %s file has been added to released %s replicate ret.' % (file_type, exp_type),
                'summary_plural': 'new %s files have been added to released %s replicate sets.' % (file_type, exp_type),
            }
        }
    }
    report_info_w_released_exp = {
        'experiments_in_set.files.status': {
            '*/released': {
                'severity': 2,
                'priority': 5,
                'summary': 'New raw %s file has been added to released %s replicate experiment.' % (file_type, exp_type),
                'summary_plural': 'new raw %s files have been added to released %s replicate experiments.' % (file_type, exp_type),
            }
        },
        'experiments_in_set.files.status': {
            '*/*': {
                'severity': 3,
                'priority': 6,
                'summary': 'New unreleased raw %s file has been added to released %s replicate experiments.' % (file_type, exp_type),
                'summary_plural': 'new unreleased raw %s files have been added to released %s replicate experiments.' % (file_type, exp_type)
            }
        },
        'experiments_in_set.processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New %s file has been added to released %s replicate experiment.' % (file_type, exp_type),
                'summary_plural': 'new %s files have been added to released %s replicate experiments.' % (file_type, exp_type),
            }
        }
    }
    report_info = {
        'status': {
            '*/released' : {
                'severity': 0,
                'priority': 0, # 0 is highest priority
                'summary': 'New %s replicate set has been released.' % exp_type,
                'summary_plural': 'new %s replicate sets have been released.' % exp_type,
            },
            'archived/released' : {
                'severity': 0,
                'priority': 0,
                'summary': 'New %s replicate set has been released.' % exp_type,
                'summary_plural': 'new %s replicate set have been released.' % exp_type,
            },
            'released/archived' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s replicate set has been archived.' % exp_type,
                'summary_plural': 'released %s replicate sets have been archived.' % exp_type,
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'Released %s replicate set has changed to %s.' % (exp_type, curr),
                'summary_plural': 'released %s replicate sets have changed to %s.' % (exp_type, curr),
            }
        },
        'experiments_in_set.status': {
            'released/archived' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s experiment has been archived.' % exp_type,
                'summary_plural': 'released %s experiments have been archived.' % exp_type,
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'Released %s experiment has changed to %s.' % (exp_type, curr),
                'summary_plural': 'released %s experiments have changed to %s.' % (exp_type, curr),
            },

        },
        'processed_files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s replicate set has been archived.' % (file_type, exp_type),
                'summary_plural': 'released %s files from %s replicate set have been archived.' % (file_type, exp_type),
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Released %s file has changed to %s.' % (file_type, curr),
                'summary_plural': 'released %s files have changed to %s.' % (file_type, curr),
            }
        },
        'experiment_in_set.processed_files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s replicate experiment has been archived.' % (file_type, exp_type),
                'summary_plural': 'released %s files from %s replicate experiments have been archived.' % (file_type, exp_type),
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Released %s file has changed to %s.' % (file_type, curr),
                'summary_plural': 'released %s files have changed to %s.' % (file_type, curr),
            }
        },
        'experiments_in_set.files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released %s file from %s replicate experiment has been archived.' % (file_type, exp_type),
                'summary_plural': 'released %s files from %s replicate experiment have been archived.' % (file_type, exp_type),
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Released %s file has changed to %s.' % (file_type, curr),
                'summary_plural': 'released %s files have changed to %s.' % (file_type, curr),
            }
        }
    }
    # update the reporting framework if necessary
    if released_exp_set:
        report_info.update(report_info_w_released_exp_set)
    if released_exp:
        report_info.update(report_info_w_released_exp)

    if field in significants:
        # get the equivalent value for the field if it matches
        if field in equivalents:
            prev = equivalents[field].get(prev, prev)
            curr = equivalents[field].get(curr, curr)
        # if the field does not have a significant value, set to '*'
        prev_key = prev if prev in significants[field] else '*'
        curr_key = curr if curr in significants[field] else '*'
        level_1 = report_info.get(path)
        if level_1:
            level_2 = level_1.get('/'.join([prev_key, curr_key]))
            if level_2:
                level_2['@id'] = item_id
                level_2['set_@id'] = set_id
                return level_2
    # no report found
    return None


def generate_exp_set_report(curr_res, prev_res, **kwargs):
    """
    curr_res and prev_res are dictionary objects to compare.
    kwargs should include:
    - add_ons (dict important info passed to calculate_report_from_change)
    - field_path (list representing the path of objects we have traversed)
    - report_fields (list of which fields are significant to report on)
    - released_statuses (list of which statuses to consider "released")
        Maybe this should be deprecated in favor of using existing equivalents in add_ons...
    - children_fields (list of which fields could be child objects)
    """
    reports = []
    # kwargs
    add_ons = copy.copy(kwargs.get('add_ons', {}))
    field_path = kwargs.get('field_path', [])
    report_fields = kwargs.get('report_fields', [])
    released_statuses = kwargs.get('released_statuses', [])
    children_fields = kwargs.get('children_fields', [])
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
        curr_val = curr_res.get(key, None)
        prev_val = prev_res.get(key, None)
        # catch cases where certain fields are missing, which is probably a schema adjustment issue
        if (curr_res and curr_val is None) or (prev_res and prev_val is None):
            continue
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
                    child_reports = generate_exp_set_report(
                        c_val,
                        prev_child,
                        report_fields=report_fields,
                        released_statuses=released_statuses,
                        children_fields=children_fields,
                        field_path=child_path,
                        add_ons=add_ons
                    )
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
    # callback fxns
    def search_callback(exp_set, exp_sets):
        exp_set_res = extract_info(exp_set, ['@id', 'status', 'tags'])
        exp_set_res['award.project'] = exp_set.get('award', {}).get('project')
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

    check = init_check_res(connection, 'experiment_set_reporting_data')
    check.status = 'IGNORE'
    fdn_conn = get_FDN_connection(connection)
    if not (fdn_conn and fdn_conn.check):
        check.status = 'ERROR'
        check.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return check
    exp_sets = {}
    search_query = '/search/?type=ExperimentSetReplicate&experimentset_type=replicate&sort=-date_created'
    safe_search_with_callback(fdn_conn, search_query, exp_sets, search_callback, limit=20, frame='embedded')
    # run a second search for status=deleted
    # add results using the same callback function
    search_query_del = '/search/?type=ExperimentSetReplicate&experimentset_type=replicate&sort=-date_created&status=deleted'
    safe_search_with_callback(fdn_conn, search_query_del, exp_sets, search_callback, limit=20, frame='embedded')
    check.full_output = exp_sets
    return check


@check_function(start_date=None, end_date=None, update_tag='DISCARD', tag_filter=None, project_filter='4DN', is_internal=False)
def data_release_updates(connection, **kwargs):
    """
    Diff two results of 'experiment_set_reporting_data' check.

    start_date and end_date are dates in form YYYY-MM-DD.
    Check will find experiment_set_reporting_data results closest to 12pm UTC
    for both those dates. If None is provided, it will consider it an empty
    experiment_set_reporting_data result for start_date; this may
    be used to effectively generate reports from a clean slate.

    If None is provided to the end date, the primary result of
    experiment_set_reporting_data is used.

    update_tag is the tag used to organize updates in Fourfront.

    tag_filter is a string value required to be in the 'tags' field of a given
    replicate set for the set to be considered by this check. It defaults to
    None, which means no tag is required. Another likely value is
    '4DN Joint Analysis 2018'.

    project_filter is a string value that is required to match the award.project
    field of the replicate set to be considered by this check. If set to None,
    all replicate sets will be used regardless of project.

    Stores the information used by that action to actually build reports.
    """
    check = init_check_res(connection, 'data_release_updates')
    check.action = 'publish_data_release_updates'
    # find needed experiment_set_reporting_data results
    data_check = init_check_res(connection, 'experiment_set_reporting_data')
    if kwargs.get('start_date', None) is not None:
        start_date = parse_datetime_to_utc(kwargs.get('start_date'), '%Y-%m-%d')
        # use 11 am UTC
        start_date = start_date.replace(hour=11)
        start_date_str = datetime.datetime.strftime(start_date, '%Y-%m-%d')
        start_data_result = data_check.get_closest_result(override_date=start_date)
    else:
        start_data_result = {}
        start_date_str = '2017-01-01'  # Arbitrary earliest time to display

    if kwargs.get('end_date'):
        end_date = parse_datetime_to_utc(kwargs.get('end_date'), '%Y-%m-%d')
        # use 11 am UTC by default
        end_date = end_date.replace(hour=11)
        end_date_str = datetime.datetime.strftime(end_date, '%Y-%m-%d')
        end_data_result = data_check.get_closest_result(override_date=end_date)
    else:  # use primary result if end_data_result is not supplied
        end_data_result = data_check.get_primary_result()
        end_date_str = end_data_result['uuid'][:10] #YYYY-MM-DD
    # start_date must be before end_date
    if start_date_str > end_date_str:
        check.status = 'ERROR'
        check.description = 'start_date cannot be greater than end_date.'
        return check
    if (kwargs.get('start_date') and not start_data_result) or not end_data_result:
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are not available.'
        return check
    tag_filter = kwargs['tag_filter']
    project_filter = kwargs.get('project_filter')
    used_res_strings = ' Compared results from %s (start) to %s (end). UUIDs are %s (start) and %s (end). Used filter on replicate sets tag is %s and the filter on award.project is %s (None means no filter in both cases).' % (start_date_str, end_date_str, start_data_result.get('uuid', 'None'), end_data_result['uuid'], tag_filter, project_filter)
    start_output = start_data_result.get('full_output', {})  # this can be empty
    end_output = end_data_result.get('full_output')  # this cannot be empty
    if not isinstance(start_output, dict) or not isinstance(end_output, dict):
        check.status = 'ERROR'
        check.description = 'One or both experiment_set_reporting_data results are malformed.' + used_res_strings
        return check

    reports = []
    ### INFO USED TO CONTROL REPORTING
    report_fields = ['status']  # fields we care about reporting on
    children_fields = ['experiments_in_set', 'files', 'processed_files']  # possible linkTo fields
    add_ons = {}
    add_ons['significants'] = {
        'status': ['released', 'archived']
    }

    ### THIS DIFFERENTIATES PUBLIC VS PUBLIC + INTERNAL RELEASE UPDATES
    if kwargs['is_internal'] == True:
        # effectively consider released and released_to_project the same
        # same with archived and archived_to_project and replaced
        released_statuses = ['released', 'released to project']
        add_ons['equivalents'] = {
            'status': {
                'released to project': 'released',
                'archived_to_project': 'archived',
                'replaced': 'archived'
            }
        }
    else:
        # only consider archived and replaced the same
        released_statuses = ['released']
        add_ons['equivalents'] = {
            'status': {
                'replaced': 'archived'
            }
        }

    ### CREATE REPORTS... assuming experiment sets will NOT be deleted from DB
    for exp_set in end_output:
        end_res = end_output[exp_set]
        # apply filters
        if tag_filter and tag_filter not in end_res.get('tags', []):
            continue
        if project_filter and project_filter != end_res.get('award.project'):
            continue
        start_res = start_output.get(exp_set, {})
        exp_set_report = generate_exp_set_report(
            end_res,
            start_res,
            report_fields=report_fields,
            released_statuses=released_statuses,
            children_fields=children_fields,
            add_ons=add_ons
        )
        if exp_set_report is not None:
            reports.append(exp_set_report)

    ### GROUP UPDATES BY SUMMARY AND BUILD METADATA
    reports.sort(key=lambda r: r['summary'])
    check.full_output = reports
    group_reports = []
    # group reports by summary
    for key, group in itertools.groupby(reports, lambda r: r['summary']):
        # first item
        first_report = group.__next__()
        group_report = {r_key: first_report[r_key] for r_key in ['severity', 'summary', 'summary_plural']}
        first_report_items = {'primary_id': first_report['set_@id'], 'secondary_id': first_report['@id']}
        group_report['update_items'] = [first_report_items]
        # use this checks uuid as the timestamp for the reports
        group_report['foursight_uuid'] = kwargs['uuid'] + '+00:00'
        group_report['update_tag'] = kwargs['update_tag']
        group_report['end_date'] = end_date_str
        group_report['start_date'] = start_date_str
        # handle parameters like tag_filter and project_filter used to create report
        group_report['parameters'] = []
        if tag_filter:
            group_report['parameters'].append('tags=' + tag_filter)
        if project_filter:
            group_report['parameters'].append('award.project=' + project_filter)
        # use 4DN DCIC lab and award, with released status
        group_report['lab'] = '4dn-dcic-lab'
        group_report['award'] = '1U01CA200059-01'
        group_report['status'] = 'released'
        group_report['is_internal'] = kwargs['is_internal']
        for gr in group: # iterate through the rest of the items
            group_report['update_items'].append({'primary_id': gr['set_@id'], 'secondary_id': gr['@id']})
        group_reports.append(group_report)
    # update summaries for plural cases
    for group_report in group_reports:
        num_items = len(group_report['update_items'])
        if num_items > 1:
            group_report['summary'] = ' '.join([str(num_items), group_report['summary_plural']])
        del group_report['summary_plural']
    # lastly make the static section for this update_tag
    static_proj = project_filter if project_filter else ''
    static_scope = 'network members' if kwargs['is_internal'] else 'public'
    static_tag = 'with %s tag' % tag_filter if tag_filter else ''
    static_content = ' '.join(['All', static_proj, 'data released to', static_scope, 'between', start_date_str, 'and', end_date_str, static_tag])
    report_static_section = {
        'name': 'release-updates.' + kwargs['update_tag'],
        'body': '<h4 style=\"margin-top:0px;font-weight:400\">' + static_content + '</h4>'
    }
    if group_reports:
        check.brief_output = {
            'release_updates': group_reports,
            'static_section': report_static_section
        }
        check.status = 'WARN'
        check.description = 'Ready to publish new data release updates.' + used_res_strings
        check.action_message = 'Will publish %s  grouped updates with the update_tag: %s. See brief_output.' % ( str(len(group_reports)), kwargs['update_tag'])
        check.allow_action = True
    else:
        check.brief_output = {
            'release_updates': [],
            'static_section': None
        }
        check.status = 'PASS'
        check.description = 'There are no data release updates for the given dates and filters.' + used_res_strings
    return check


@action_function()
def publish_data_release_updates(connection, **kwargs):
    action = init_action_res(connection, 'publish_data_release_updates')
    report_check = init_check_res(connection, 'data_release_updates')
    report_uuid = kwargs['called_by']
    report_result = report_check.get_result_by_uuid(report_uuid)
    action.description = "Publish data release updates to Fourfront."
    updates_to_post = report_result.get('brief_output', {}).get('release_updates', [])
    section_to_post = report_result.get('brief_output', {}).get('static_section')
    # post items to FF
    fdn_conn = get_FDN_connection(connection)
    if not (fdn_conn and fdn_conn.check):
        action.status = 'FAIL'
        action.description = ''.join(['Could not establish a FDN_Connection using the FF env: ', connection.ff_env])
        return action
    posted_updates = []
    for update in updates_to_post:
        # should be in good shape to post as-is
        resp = ff_utils.post_to_metadata(update, 'data-release-updates', connection=fdn_conn)
        posted_updates.append({'update': update, 'response': resp})
    if section_to_post:
        resp = ff_utils.post_to_metadata(section_to_post, 'static-sections', connection=fdn_conn)
        posted_section = {'static_section': section_to_post, 'response': resp}
    else:
        posted_section = None
    action.output = {
        'updates_posted': posted_updates,
        'section_posted': posted_section
    }
    action.status = 'DONE'
    return action
