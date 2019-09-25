from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    action_function,
)
from ..run_result import CheckResult, ActionResult
from .helpers import qc_utils
from dcicutils import ff_utils
import time


@check_function(file_type=None, status=None, search_add_on=None)
def identify_files_without_qc_summary_pairs(connection, **kwargs):
    fileformat = 'pairs'
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    check = CheckResult(connection, 'identify_files_without_qc_summary_pairs')
    # must set this to be the function name of the action
    check.action = 'patch_quality_metric_summary_pairs'
    default_filetype = 'FileProcessed'  # skip fastq
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object'.format(filetype, stati)
    search_query += '&file_format.file_format=' + fileformat
    addon = kwargs.get('search_add_on')
    if addon is not None:
        if not addon.startswith('&'):
            addon = '&' + addon
        search_query += addon
    problem_files = []
    file_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    for hit in file_hits:
        if round(time.time() - t0, 2) > time_limit:
            break
        if hit.get('quality_metric') and not hit.get('quality_metric_summary', ''):
            hit_dict = {
                'accession': hit.get('accession'),
                'uuid': hit.get('uuid'),
                '@type': hit.get('@type'),
                'upload_key': hit.get('upload_key'),
                'file_format': hit.get('file_format'),
                'quality_metric': hit.get('quality_metric')
            }
            problem_files.append(hit_dict)
    check.summary = '{} files with no quality metric summary'.format(len(problem_files))
    check.full_output = problem_files
    if problem_files:
        check.status = 'WARN'
        check.summary = 'File metadata found without quality_metric_summary'
        status_str = 'pre-release/released/released to project/uploaded'
        if kwargs.get('status'):
            status_str = kwargs.get('status')
        type_str = ''
        if kwargs.get('file_type'):
            type_str = kwargs.get('file_type') + ' '
        ff_str = ''
        if kwargs.get('file_format'):
            ff_str = kwargs.get('file_format') + ' '
        check.description = "{cnt} {type}{ff}files that are {st} don't have quality_metric_summary.".format(
            cnt=len(problem_files), type=type_str, st=status_str, ff=ff_str)
        check.action_message = "Will attempt to patch quality_metric_summary for %s files." % str(len(problem_files))
        check.allow_action = True  # allows the action to be run
    else:
        check.status = 'PASS'
    return check


@check_function(file_type=None, status=None, search_add_on=None)
def identify_files_without_qc_summary_bb(connection, **kwargs):
    fileformat = 'bigbed'
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    check = CheckResult(connection, 'identify_files_without_qc_summary_bb')
    # must set this to be the function name of the action
    check.action = 'patch_quality_metric_summary_bb'
    default_filetype = 'FileProcessed'  # skip fastq
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object'.format(filetype, stati)
    search_query += '&file_format.file_format=' + fileformat
    addon = kwargs.get('search_add_on')
    if addon is not None:
        if not addon.startswith('&'):
            addon = '&' + addon
        search_query += addon
    problem_files = []
    file_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    for hit in file_hits:
        if round(time.time() - t0, 2) > time_limit:
            break
        if hit.get('quality_metric') and not hit.get('quality_metric_summary', ''):
            hit_dict = {
                'accession': hit.get('accession'),
                'uuid': hit.get('uuid'),
                '@type': hit.get('@type'),
                'upload_key': hit.get('upload_key'),
                'file_format': hit.get('file_format'),
                'quality_metric': hit.get('quality_metric')
            }
            problem_files.append(hit_dict)
    check.summary = '{} files with no quality metric summary'.format(len(problem_files))
    check.full_output = problem_files
    if problem_files:
        check.status = 'WARN'
        check.summary = 'File metadata found without quality_metric_summary'
        status_str = 'pre-release/released/released to project/uploaded'
        if kwargs.get('status'):
            status_str = kwargs.get('status')
        type_str = ''
        if kwargs.get('file_type'):
            type_str = kwargs.get('file_type') + ' '
        ff_str = ''
        if kwargs.get('file_format'):
            ff_str = kwargs.get('file_format') + ' '
        check.description = "{cnt} {type}{ff}files that are {st} don't have quality_metric_summary.".format(
            cnt=len(problem_files), type=type_str, st=status_str, ff=ff_str)
        check.action_message = "Will attempt to patch quality_metric_summary for %s files." % str(len(problem_files))
        check.allow_action = True  # allows the action to be run
    else:
        check.status = 'PASS'
    return check


@check_function(file_type=None, status=None, search_add_on=None)
def identify_files_without_qc_summary_bed(connection, **kwargs):
    fileformat = 'bed'
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    check = CheckResult(connection, 'identify_files_without_qc_summary_bed')
    # must set this to be the function name of the action
    check.action = 'patch_quality_metric_summary_bed'
    default_filetype = 'FileProcessed'  # skip fastq
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object'.format(filetype, stati)
    search_query += '&file_format.file_format=' + fileformat
    addon = kwargs.get('search_add_on')
    if addon is not None:
        if not addon.startswith('&'):
            addon = '&' + addon
        search_query += addon
    problem_files = []
    file_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    for hit in file_hits:
        if round(time.time() - t0, 2) > time_limit:
            break
        if hit.get('quality_metric') and not hit.get('quality_metric_summary', ''):
            hit_dict = {
                'accession': hit.get('accession'),
                'uuid': hit.get('uuid'),
                '@type': hit.get('@type'),
                'upload_key': hit.get('upload_key'),
                'file_format': hit.get('file_format'),
                'quality_metric': hit.get('quality_metric')
            }
            problem_files.append(hit_dict)
    check.summary = '{} files with no quality metric summary'.format(len(problem_files))
    check.full_output = problem_files
    if problem_files:
        check.status = 'WARN'
        check.summary = 'File metadata found without quality_metric_summary'
        status_str = 'pre-release/released/released to project/uploaded'
        if kwargs.get('status'):
            status_str = kwargs.get('status')
        type_str = ''
        if kwargs.get('file_type'):
            type_str = kwargs.get('file_type') + ' '
        ff_str = ''
        if kwargs.get('file_format'):
            ff_str = kwargs.get('file_format') + ' '
        check.description = "{cnt} {type}{ff}files that are {st} don't have quality_metric_summary.".format(
            cnt=len(problem_files), type=type_str, st=status_str, ff=ff_str)
        check.action_message = "Will attempt to patch quality_metric_summary for %s files." % str(len(problem_files))
        check.allow_action = True  # allows the action to be run
    else:
        check.status = 'PASS'
    return check


@action_function()
def patch_quality_metric_summary_pairs(connection, **kwargs):
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    action = ActionResult(connection, 'patch_quality_metric_summary_pairs')
    action_logs = {'time out': False, 'skipping_format': [], 'patch_failure': [], 'patch_success': []}
    # get latest results from identify_files_without_qc_summary
    filesize_check_result = action.get_associated_check_result(kwargs)
    for hit in filesize_check_result.get('full_output', []):
        if round(time.time() - t0, 2) > time_limit:
            action.status = 'FAIL'
            action_logs['time out'] = True
            action.output = action_logs
            return action
        if qc_utils.parse_formatstr(hit['file_format']) == 'pairs':
            if hit['quality_metric'].startswith('/quality-metrics-pairsqc/'):
                try:
                    qc_utils.calculate_qc_metric_pairsqc(hit['uuid'], key=connection.ff_keys)
                    action_logs['patch_success'].append(hit['accession'])
                except Exception as e:
                    acc_and_error = ': '.join([hit['accession'], str(e)])
                    action_logs['patch_failure'].append(acc_and_error)

            elif hit['quality_metric'].startswith('/quality-metrics-margi/'):
                try:
                    qc_utils.calculate_qc_metric_margi_pairsqc(hit['uuid'], key=connection.ff_keys)
                    action_logs['patch_success'].append(hit['accession'])
                except Exception as e:
                    acc_and_error = ': '.join([hit['accession'], str(e)])
                    action_logs['patch_failure'].append(acc_and_error)
        else:
            acc_and_format = ': '.join([hit['accession'], hit['file_format']])
            action_logs['skipping_format'].append(acc_and_format)
    action.status = 'DONE'
    action.output = action_logs
    return action


@action_function()
def patch_quality_metric_summary_bb(connection, **kwargs):
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    action = ActionResult(connection, 'patch_quality_metric_summary_bb')
    action_logs = {'time out': False, 'skipping_format': [], 'patch_failure': [], 'patch_success': []}
    # get latest results from identify_files_without_qc_summary_bb
    filesize_check_result = action.get_associated_check_result(kwargs)
    for hit in filesize_check_result.get('full_output', []):
        if round(time.time() - t0, 2) > time_limit:
            action.status = 'FAIL'
            action_logs['time out'] = True
            action.output = action_logs
            return action
        if qc_utils.parse_formatstr(hit['file_format']) == 'bigbed':
            try:
                qc_utils.calculate_qc_metric_atacseq_bb(hit['uuid'], key=connection.ff_keys)
                action_logs['patch_success'].append(hit['accession'])
            except Exception as e:
                acc_and_error = ': '.join([hit['accession'], str(e)])
                action_logs['patch_failure'].append(acc_and_error)
        else:
            acc_and_format = ': '.join([hit['accession'], hit['file_format']])
            action_logs['skipping_format'].append(acc_and_format)
    action.status = 'DONE'
    action.output = action_logs
    return action


@action_function()
def patch_quality_metric_summary_bed(connection, **kwargs):
    t0 = time.time()  # keep track of how start time
    time_limit = 270  # 4.5 minutes
    action = ActionResult(connection, 'patch_quality_metric_summary_bed')
    action_logs = {'time out': False, 'skipping_format': [], 'patch_failure': [], 'patch_success': []}
    # get latest results from identify_files_without_qc_summary_bed
    filesize_check_result = action.get_associated_check_result(kwargs)
    for hit in filesize_check_result.get('full_output', []):
        if round(time.time() - t0, 2) > time_limit:
            action.status = 'FAIL'
            action_logs['time out'] = True
            action.output = action_logs
            return action
        if qc_utils.parse_formatstr(hit['file_format']) == 'bed':
            try:
                qc_utils.calculate_qc_metric_tagalign(hit['uuid'], key=connection.ff_keys)
                action_logs['patch_success'].append(hit['accession'])
            except Exception as e:
                acc_and_error = ': '.join([hit['accession'], str(e)])
                action_logs['patch_failure'].append(acc_and_error)
        else:
            acc_and_format = ': '.join([hit['accession'], hit['file_format']])
            action_logs['skipping_format'].append(acc_and_format)
    action.status = 'DONE'
    action.output = action_logs
    return action
