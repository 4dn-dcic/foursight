from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
from dcicutils import s3Utils

from .. import wfr_utils

import requests
import sys
import json
from datetime import datetime

import time
import boto3


@check_function()
def md5run_status_extra_file(connection, **kwargs):
    """Searches for extra files that are uploaded to s3, but not went though md5 run.
    no action is associated, we don't have any case so far.
    Will be implemented if this check gets WARN"""
    check = init_check_res(connection, 'md5run_status_extra_file')
    my_auth = ff_utils.get_authentication_with_server({}, ff_env=connection.ff_env)
    check.status = 'PASS'

    # Build the query
    query = '/search/?type=File&extra_files.status=uploading&extra_files.status=upload+failed'
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'Nothing to see, move along'
        return check
    else:
        check.status = 'WARN'
        check.brief_output = 'There are user submitted extra files without md5runs'
        check.full_output = {'extra_files_missing_md5': [i['accession'] for i in res]}
        return check


@check_function(file_type='File', lab_title=None, start_date=None, run_hours=24)
def md5run_status(connection, **kwargs):
    """Searches for files that are uploaded to s3, but not went though md5 run.
    This check makes certain assumptions
    -all files that have a status<= uploaded, went through md5run
    -all files status uploading/upload failed, and no s3 file are pending, and skipped by this check.
    if you change status manually, it might fail to show up in this checkself.

    Keyword arguments:
    file_type -- limit search to a file type, i.e. FileFastq (default=File)
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'md5run_status')
    my_auth = ff_utils.get_authentication_with_server({}, ff_env=connection.ff_env)

    check.action = "md5run_start"
    check.allow_action = True
    check.brief_output = "Result Summary"
    check.full_output = {}
    check.status = 'PASS'

    # Build the query
    query = '/search/?status=uploading&status=upload failed'
    # add file type
    f_type = kwargs.get('file_type')
    query += '&type=' + f_type
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'Nothing to see, move along'
        return check

    # if there are files, make sure they are not on s3
    no_s3_file = []
    running = []
    missing_md5 = []
    not_switched_status = []

    my_s3_util = s3Utils(env=connection.ff_env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket

    for a_file in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > 280:
            break
        # find bucket
        if 'FileProcessed' in a_file['@type']:
                my_bucket = out_bucket
        elif 'FileVistrack' in a_file['@type']:
                my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                my_bucket = raw_bucket
        # check if file is in s3
        file_id = a_file['accession']
        head_info = my_s3_util.does_key_exist(a_file['upload_key'], my_bucket)
        if not head_info:
            no_s3_file.append(file_id)
            continue

        run_time = kwargs.get('run_hours')
        md5_report = wfr_utils.get_wfr_out(a_file, "md5", my_auth, md_qc=True)
        if md5_report['status'] == 'running':
            running.append(file_id)
            continue

        # Most probably the trigger did not work, and we run it manually
        if md5_report['status'] != 'complete':
            missing_md5.append(file_id)
            continue

        # There is a successful run, but status is not switched, happens when a file is reuploaded.
        if md5_report['status'] == 'complete':
            not_switched_status.append(file_id)
            continue

    if no_s3_file:
        check.summary = 'Some files are pending upload'
        check.brief_output = '\n' + str(len(no_s3_file)) + '(uploading/upload failed) files waiting for upload'
        check.full_output['files_pending_upload'] = no_s3_file

    if running:
        check.summary = 'Some files are running md5run'
        check.brief_output += '\n' + str(len(running)) + ' files are still running md5run.'
        check.full_output['files_running_md5'] = running

    if missing_md5:
        check.summary = 'Some files are missing md5 runs'
        check.brief_output += '\n' + str(len(missing_md5)) + ' files lack a successful md5 run'
        check.full_output['files_without_md5run'] = missing_md5
        check.status = 'WARN'

    if not_switched_status:
        check.summary += ' Some files are have wrong status with a successful run'
        check.brief_output += '\n' + str(len(not_switched_status)) + ' files are have wrong status with a successful run'
        check.full_output['files_with_run_and_wrong_status'] = not_switched_status
        check.status = 'WARN'
    check.summary = check.summary.strip()
    check.brief_output = check.brief_output.strip()
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5run_start(connection, **kwargs):
    """Start md5 runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'md5run_start')
    action_logs = {'runs_started': [], 'runs_errored': []}
    my_auth = ff_utils.get_authentication_with_server({}, ff_env=connection.ff_env)
    # get latest results from identify_files_without_filesize
    md5run_check = init_check_res(connection, 'md5run_status')
    md5run_check_result = md5run_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    targets = []
    if kwargs.get('start_missing'):
        targets.extend(md5run_check_result.get('missing_md5', []))
    if kwargs.get('start_not_switched'):
        targets.extend(md5run_check_result.get('files_with_run_and_wrong_status', []))

    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > 280:
            action.description = 'Did not complete, due to time limitations, rerun the check and action'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_file': a_file['@id']}
        wfr_setup = wfr_utils.step_settings('md5', 'no_organism', attributions)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        action_logs['started_runs'].append(url)
    action.output = action_logs
    action.status = 'DONE'
    return action
