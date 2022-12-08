import os
from foursight_core.checks.helpers.sys_utils import (
    parse_datetime_to_utc,
)
from dcicutils import ff_utils
from .helpers.google_utils import GoogleAPISyncer
import copy
import itertools
import datetime

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


#### HELPER FUNCTIONS ####

# cache results from requests to FF
ITEM_CACHE = {'titles': {}, 'replacing': {}}

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


def add_to_report(exp_set, exp_sets):
    """
    Used to process search hits in experiment_set_reporting_data
    """
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


def find_item_title(item_id):
    """
    Data is always used for release updates, so hardcode here (dirty hack)
    Use cache to improve performance
    Return display title of given item, None if item can't be found
    """
    if item_id == 'UNKNOWN_ID':
        return None
    if item_id in ITEM_CACHE['titles']:
        return ITEM_CACHE['titles'][item_id]
    item_obj = ff_utils.get_metadata(item_id, ff_env='data', add_on='frame=object')
    title = item_obj.get('display_title')
    ITEM_CACHE['titles'][item_id] = title
    return title


def find_replacing_item(item_id):
    """
    Data is always used for release updates, so hardcode here (dirty hack)
    Use cache to improve performance
    Return the @id of replacing item, None if item can't be found
    """
    if item_id == 'UNKNOWN_ID':
        return None
    if item_id in ITEM_CACHE['replacing']:
        return ITEM_CACHE['replacing'][item_id]
    # need to get accession manually from replaced item to find the replacing item
    replaced = ff_utils.get_metadata(item_id, ff_env='data', add_on='frame=object')
    replaced_accession = replaced['accession']
    replacing = ff_utils.get_metadata(replaced_accession, ff_env='data', add_on='frame=object')
    if not replacing.get('alternate_accessions'):  # see if it is actually a replacing item
        replacing = None
    else:
        replacing = replacing.get('@id')
    ITEM_CACHE['replacing'][item_id] = replacing
    return replacing


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
    # *NUM* is replaced by the number of reports
    # to save time, calculate conditional functions within report_info here
    # calculate info for REPLACED items
    if curr in significants[field] and curr == 'replaced':
        replacing_secondary_id = find_replacing_item(item_id)
        if item_id == set_id:  # we are on an exp set
            replacing_add_info = 'Replaced by'
        else:
            replacing_add_info = '%s replaced by' % find_item_title(item_id)
    else:
        replacing_secondary_id = None
        replacing_add_info = ''

    report_info_w_released_exp_set = {
        'experiments_in_set.status': {
            '*/released' : {
                'severity': 1,
                'priority': 3,
                'summary': 'New replicate experiments have been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'New replicate experiments have been added to *NUM* released %s replicate sets.' % exp_type,
            },
            '*/*': {
                'severity': 3,
                'priority': 4,
                'summary': 'Replicate experiments with status %s have been added to a released %s replicate set.' % (curr, exp_type),
                'summary_plural': 'Replicate experiments with status %s have been added to *NUM* released %s replicate sets.' % (curr, exp_type),
            },
        },
        'processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New processed files have been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'New processed files have been added to *NUM* released %s replicate sets.' % exp_type,
            }
        }
    }
    report_info_w_released_exp = {
        'experiments_in_set.files.status': {
            '*/released': {
                'severity': 2,
                'priority': 5,
                'summary': 'New raw files have been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'New raw files have been added to *NUM* released %s replicate sets.' % exp_type,
            }
        },
        'experiments_in_set.files.status': {
            '*/*': {
                'severity': 3,
                'priority': 6,
                'summary': 'New unreleased raw files have been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'New unreleased raw files have been added to *NUM* released %s replicate sets.' % exp_type,
            }
        },
        'experiments_in_set.processed_files.status': {
            '*/released': {
                'severity': 0,
                'priority': 7,
                'summary': 'New processed files have been added to a released %s replicate set.' % exp_type,
                'summary_plural': 'New processed files have been added to *NUM* released %s replicate sets.' % exp_type,
            }
        }
    }
    report_info = {
        'status': {
            '*/released' : {
                'severity': 0,
                'priority': 0, # 0 is highest priority
                'summary': 'New %s replicate set has been released.' % exp_type,
                'summary_plural': '*NUM* new %s replicate sets have been released.' % exp_type,
            },
            'archived/released' : {
                'severity': 3,
                'priority': 0,
                'summary': 'New %s replicate set has changed from archived to released.' % exp_type,
                'summary_plural': '*NUM* new %s replicate sets have changed from archived to released.' % exp_type,
            },
            'replaced/released' : {
                'severity': 3,
                'priority': 0,
                'summary': 'New %s replicate set has changed from replaced to released.' % exp_type,
                'summary_plural': '*NUM* new %s replicate sets have changed from replaced to released.' % exp_type,
            },
            'released/archived' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s replicate set has been archived.' % exp_type,
                'summary_plural': '*NUM* released %s replicate sets have been archived.' % exp_type,
            },
            'released/replaced' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released %s replicate set has been replaced.' % exp_type,
                'summary_plural': '*NUM* released %s replicate sets have been replaced.' % exp_type,
                'secondary_id': replacing_secondary_id,
                'add_info': replacing_add_info
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'Released %s replicate set has changed to %s.' % (exp_type, curr),
                'summary_plural': '*NUM* released %s replicate sets have changed to %s.' % (exp_type, curr),
            }
        },
        'experiments_in_set.status': {
            'released/archived' : {
                'severity': 1,
                'priority': 1,
                'summary': 'Released replicate experiments from a %s replicate set have been archived.' % exp_type,
                'summary_plural': 'Released replicate experiments from *NUM* %s replicate sets have been archived.' % exp_type,
            },
            'released/replaced': {
                'severity': 1,
                'priority': 1,
                'summary': 'Released replicate experiments from a %s replicate set have been replaced.' % exp_type,
                'summary_plural': 'Released replicate experiments from *NUM* %s replicate sets have been replaced.' % exp_type,
                'secondary_id': replacing_secondary_id,
                'add_info': replacing_add_info
            },
            'released/*': {
                'severity': 3,
                'priority': 2,
                'summary': 'Replicate experiments from a %s replicate set have changed from released to %s.' % (exp_type, curr),
                'summary_plural': 'Replicate experiments from *NUM* %s replicate sets have changed from released to %s.' % (exp_type, curr),
            },
        },
        'processed_files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released processed files from a %s replicate set have been archived.' % exp_type,
                'summary_plural': 'Released processed files from *NUM* %s replicate sets have been archived.' % exp_type,
            },
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released processed files from a %s replicate set have been replaced.' % exp_type,
                'summary_plural': 'Released processed files from *NUM* %s replicate sets have been replaced.' % exp_type,
                'secondary_id': replacing_secondary_id,
                'add_info': replacing_add_info
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Processed files from a %s replicate set have been changed from released to %s.' % (exp_type, curr),
                'summary_plural': 'Processed files from *NUM* %s replicate sets have been changed from released to %s.' % (exp_type, curr),
            }
        },
        'experiment_in_set.processed_files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released processed files from a %s replicate set have been archived.' % exp_type,
                'summary_plural': 'Released processed files from *NUM* %s replicate sets have been archived.' % exp_type,
            },
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released processed files from a %s replicate set have been replaced.' % exp_type,
                'summary_plural': 'Released processed files from *NUM* %s replicate sets have been replaced.' % exp_type,
                'secondary_id': replacing_secondary_id,
                'add_info': replacing_add_info
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Processed files from a %s replicate set have been changed from released to %s.' % (exp_type, curr),
                'summary_plural': 'Processed files from *NUM* %s replicate sets have been changed from released to %s.' % (exp_type, curr),
            }
        },
        'experiments_in_set.files.status': {
            'released/archived': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released raw files from a %s replicate set have been archived.' % exp_type,
                'summary_plural': 'Released raw files from *NUM* %s replicate sets have been archived.' % exp_type,
            },
            'released/replaced': {
                'severity': 1,
                'priority': 8,
                'summary': 'Released raw files from a %s replicate set have been replaced.' % exp_type,
                'summary_plural': 'Released raw files from *NUM* %s replicate sets have been replaced.' % exp_type,
                'secondary_id': replacing_secondary_id,
                'add_info': replacing_add_info
            },
            'released/*': {
                'severity': 3,
                'priority': 9,
                'summary': 'Raw files from a %s replicate set have changed from released to %s.' % (exp_type, curr),
                'summary_plural': 'Raw files from *NUM* %s replicate sets have changed from released to %s.' % (exp_type, curr),
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
                secondary_id = level_2.get('secondary_id')
                level_2['items'] = [{'secondary_id': secondary_id if secondary_id else item_id,
                                     'additional_info': level_2.get('add_info', '')}]
                level_2['set_@id'] = set_id
                # clean up unneeded fields
                if 'secondary_id' in level_2:
                    del level_2['secondary_id']
                if 'add_info' in level_2:
                    del level_2['add_info']
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
    # handle file_type add_ons. cannot be both processed_files and files
    # currently this add_on isn't used for anything
    if field_path and field_path[-1] in ['processed_files', 'files']:
        file_type = curr_res.get('file_type')
        if file_type:
            add_ons['file_type'] = file_type
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
    # merge reports with the same summary to combine @ids into 'items' field
    merging = {}
    for report in reports:
        if report['summary'] not in merging:
            merging[report['summary']] = report
        else:
            merging[report['summary']]['items'].extend(report['items'])
    merged_reports = list(merging.values())
    if merged_reports:
        merged_reports.sort(key = lambda r: r['priority'])
    return merged_reports[0] if merged_reports else None


#### CHECKS / ACTIONS #####

# TODO: This check has been removed from the schedule and should be revisited and refactored
# @check_function()
def experiment_set_reporting_data(connection, **kwargs):
    """
    Get a snapshot of all experiment sets, their experiments, and files of
    all of the above. Include uuid, accession, status, and md5sum (for files).
    """
    check = CheckResult(connection, 'experiment_set_reporting_data')
    check.status = 'IGNORE'
    exp_sets = {}
    search_query = '/search/?type=ExperimentSetReplicate&experimentset_type=replicate&sort=-date_created'
    set_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=20)
    # run a second search for status=deleted and status=replaced
    set_hits_del = ff_utils.search_metadata(search_query + '&status=deleted&status=replaced',
                                            key=connection.ff_keys, page_limit=20)
    set_hits.extend(set_hits_del)
    for hit in set_hits:
        add_to_report(hit, exp_sets)
    check.full_output = exp_sets
    return check


def _old_data_release_updates(connection, **kwargs):
    """
    OLD VERSION of the data_release_updates check with docstring.

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
    check = CheckResult(connection, 'data_release_updates')
    check.action = 'publish_data_release_updates'
    # find needed experiment_set_reporting_data results
    data_check = CheckResult(connection, 'experiment_set_reporting_data')
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
        end_date_str = end_data_result['uuid'][:10]  # YYYY-MM-DD
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
    used_res_strings = ' Compared results from %s (start) to %s (end). UUIDs are %s (start) and %s (end). Used filter on replicate sets tag is %s and the filter on award.project is %s (None means no filter in both cases).' % (
    start_date_str, end_date_str, start_data_result.get('uuid', 'None'), end_data_result['uuid'], tag_filter,
    project_filter)
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
        'status': ['released', 'archived', 'replaced']
    }

    ### THIS DIFFERENTIATES PUBLIC VS PUBLIC + INTERNAL RELEASE UPDATES
    if kwargs['is_internal'] == True:
        # effectively consider released and released_to_project the same
        released_statuses = ['released', 'released to project']
        add_ons['equivalents'] = {
            'status': {
                'released to project': 'released',
                'archived_to_project': 'archived'
            }
        }
    else:
        released_statuses = ['released']

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
        first_report_items = {'primary_id': first_report['set_@id'], 'secondary_ids': first_report['items']}
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
        for gr in group:  # iterate through the rest of the items
            group_report['update_items'].append({'primary_id': gr['set_@id'], 'secondary_ids': gr['items']})
        group_reports.append(group_report)
    # update summaries for plural cases
    for group_report in group_reports:
        num_items = len(group_report['update_items'])
        if num_items > 1:
            group_report['summary_plural'] = group_report['summary_plural'].replace('*NUM*', str(num_items))
            group_report['summary'] = group_report['summary_plural']
        del group_report['summary_plural']
    # lastly make the static section for this update_tag
    static_proj = project_filter if project_filter else ''
    static_scope = 'network members' if kwargs['is_internal'] else 'public'
    static_tag = 'with %s tag' % tag_filter if tag_filter else ''
    static_content = ' '.join(
        ['All', static_proj, 'data released to', static_scope, 'between', start_date_str, 'and', end_date_str,
         static_tag])
    report_static_section = {
        'name': 'release-updates.' + kwargs['update_tag'],
        'body': '<h4 style=\"margin-top:0px;font-weight:400\">' + static_content + '</h4>'
    }
    check.status = 'PASS'
    if group_reports:
        check.brief_output = {
            'release_updates': group_reports,
            'static_section': report_static_section
        }
        check.description = 'Ready to publish new data release updates.' + used_res_strings
        check.action_message = 'Will publish %s  grouped updates with the update_tag: %s. See brief_output.' % (
        str(len(group_reports)), kwargs['update_tag'])
        check.allow_action = True
    else:
        check.brief_output = {
            'release_updates': [],
            'static_section': None
        }
        check.description = 'There are no data release updates for the given dates and filters.' + used_res_strings
    return check


# TODO: This check has been removed from the schedule and should be revisited and refactored
# @check_function(start_date=None, end_date=None, update_tag='DISCARD', tag_filter=None, project_filter='4DN', is_internal=False)
def data_release_updates(connection, **kwargs):
    """ TODO: New version of this check - for now, does nothing - see old version above. """
    check = CheckResult(connection, 'data_release_updates')
    check.status = 'PASS'
    check.brief_output = check.full_output = 'This check still needs to be implemented!'
    return check


# TODO: This action has been removed from the schedule and should be revisited and refactored
# @action_function
def publish_data_release_updates(connection, **kwargs):
    """ TODO: This action probably needs rewriting as well as it based on the OLD data_release_updates check. """
    action = ActionResult(connection, 'publish_data_release_updates')
    report_result = action.get_associated_check_result(kwargs)
    action.description = "Publish data release updates to Fourfront."
    updates_to_post = report_result.get('brief_output', {}).get('release_updates', [])
    section_to_post = report_result.get('brief_output', {}).get('static_section')
    # post items to FF
    posted_updates = []
    for update in updates_to_post:
        # should be in good shape to post as-is
        resp = ff_utils.post_metadata(update, 'data-release-updates', key=connection.ff_keys)
        posted_updates.append({'update': update, 'response': resp})
    if section_to_post:
        resp = ff_utils.post_metadata(section_to_post, 'static-sections', key=connection.ff_keys)
        posted_section = {'static_section': section_to_post, 'response': resp}
    else:
        posted_section = None
    action.output = {
        'updates_posted': posted_updates,
        'section_posted': posted_section
    }
    action.status = 'DONE'
    return action


@check_function(start_date=None, end_date=None)
def sync_google_analytics_data(connection, **kwargs):
    '''
    This checks the last time that analytics data was fetched (if any) and then
    triggers an action to fill up fourfront with incremented google_analytics TrackingItems.

    TODO: No use case yet, but we could accept start_date and end_date here & maybe in action eventually.
    '''
    check = CheckResult(connection, 'sync_google_analytics_data')

    if os.environ.get('chalice_stage', 'dev') != 'prod':
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check

    recent_passing_run = False
    recent_runs, total_unused = check.get_result_history(0, 20, after_date=datetime.datetime.now() - datetime.timedelta(hours=3))
    for run in recent_runs:
        # recent_runs is a list of lists. [status, None, kwargdict]
        # Status is at index 0.
        if run[0] == 'PASS':
            recent_passing_run = True
            break

    if recent_passing_run:
        check.summary = check.description = 'This check was run within last 3 hours; skipping because need time for TrackingItems to be indexed.'
        check.status = 'FAIL'
        return check

    google = GoogleAPISyncer(connection.ff_keys)

    action_logs = { 'daily_created' : [], 'monthly_created' : [] }

    res = google.analytics.fill_with_tracking_items('daily')
    action_logs['daily_created'] = res.get('created', [])

    res = google.analytics.fill_with_tracking_items('monthly')
    action_logs['monthly_created'] = res.get('created', [])

    check.full_output = action_logs
    check.status = 'PASS' if (len(action_logs['daily_created']) > 0 or len(action_logs['monthly_created']) > 0) else 'WARN'
    check.description = 'Created %s daily items and %s monthly Items.' % (str(len(action_logs['daily_created'])), str(len(action_logs['monthly_created'])))
    return check
