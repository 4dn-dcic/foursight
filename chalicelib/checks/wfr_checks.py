from datetime import datetime
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
from dcicutils import s3Utils

from .helpers import wfr_utils
from .helpers import wfrset_utils

# import requests
# import sys
# import json
# import time
# import boto3


# TODO: Collect required items and do a combined get request (or get_es_metadata)
# TODO:
# TODO:

lambda_limit = 270  # 300 - 30 sec


@check_function()
def md5run_status_extra_file(connection, **kwargs):
    """Searches for extra files that are uploaded to s3, but not went though md5 run.
    no action is associated, we don't have any case so far.
    Will be implemented if this check gets WARN"""
    check = init_check_res(connection, 'md5run_status_extra_file')
    my_auth = connection.ff_keys
    check.status = 'PASS'

    # Build the query
    query = '/search/?type=File&extra_files.status=uploading&extra_files.status=upload+failed'
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    else:
        check.status = 'WARN'
        check.brief_output = ['There are user submitted extra files without md5runs']
        check.full_output = {'extra_files_missing_md5': [i['accession'] for i in res]}
        return check


@check_function(file_type='File', lab_title=None, start_date=None)
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
    my_auth = connection.ff_keys

    check.action = "md5run_start"
    check.brief_output = []
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
        check.summary = 'All Good!'
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
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
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
        md5_report = wfr_utils.get_wfr_out(file_id, "md5", my_auth, md_qc=True)
        if md5_report['status'] == 'running':
            running.append(file_id)
        # Most probably the trigger did not work, and we run it manually
        elif md5_report['status'] != 'complete':
            missing_md5.append(file_id)
        # There is a successful run, but status is not switched, happens when a file is reuploaded.
        elif md5_report['status'] == 'complete':
            not_switched_status.append(file_id)
    print(check.brief_output)
    if no_s3_file:
        check.summary = 'Some files are pending upload'
        msg = str(len(no_s3_file)) + '(uploading/upload failed) files waiting for upload'
        print(msg)
        check.brief_output.append(msg)
        check.full_output['files_pending_upload'] = no_s3_file

    if running:
        check.summary = 'Some files are running md5run'
        msg = str(len(running)) + ' files are still running md5run.'
        check.brief_output.append(msg)
        check.full_output['files_running_md5'] = running

    if missing_md5:
        check.allow_action = True
        check.summary = 'Some files are missing md5 runs'
        print(check.brief_output)
        msg = str(len(missing_md5)) + ' files lack a successful md5 run'
        check.brief_output.append(msg)
        check.full_output['files_without_md5run'] = missing_md5
        check.status = 'WARN'

    if not_switched_status:
        check.allow_action = True
        check.summary += ' Some files are have wrong status with a successful run'
        msg = str(len(not_switched_status)) + ' files are have wrong status with a successful run'
        check.brief_output.append(msg)
        check.full_output['files_with_run_and_wrong_status'] = not_switched_status
        check.status = 'WARN'
    if not check.brief_output:
        check.brief_output = ['All Good!', ]
    check.summary = check.summary.strip()
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5run_start(connection, **kwargs):
    """Start md5 runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'md5run_start')
    action_logs = {'runs_started': []}
    my_auth = connection.ff_keys
    # get latest results from identify_files_without_filesize
    md5run_check = init_check_res(connection, 'md5run_status')
    md5run_check_result = md5run_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    action_logs['check_output'] = md5run_check_result
    targets = []
    if kwargs.get('start_missing'):
        targets.extend(md5run_check_result.get('files_without_md5run', []))
    if kwargs.get('start_not_switched'):
        targets.extend(md5run_check_result.get('files_with_run_and_wrong_status', []))
    action_logs['targets'] = targets
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_file': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('md5', 'no_organism', attributions)

        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        action_logs['runs_started'].append(url)
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
def fastqc_status(connection, **kwargs):
    """Searches for fastq files that don't have fastqc

    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'fastqc_status')
    my_auth = connection.ff_keys

    check.action = "fastqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # Build the query (skip to be uploaded by workflow)
    query = "/search/?type=FileFastq&quality_metric.uuid=No+value&status=pre-release&status=released&status=released%20to%20project&status=uploaded"
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
        check.summary = 'All Good!'
        return check

    # missing run
    missing_fastqc = []
    # if there is a successful run but no qc
    missing_qc = []
    running = []

    for a_fastq in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
            break
        file_id = a_fastq['accession']
        report = wfr_utils.get_wfr_out(file_id, 'fastqc-0-11-4-1',  my_auth, md_qc=True)
        if report['status'] == 'running':
            running.append(file_id)
            continue
        # Most probably the trigger did not work, and we run it manually
        if report['status'] != 'complete':
            missing_fastqc.append(file_id)
            continue
        # There is a successful run, but no qc, previously happened when a file was reuploaded.
        if report['status'] == 'complete':
            missing_qc.append(file_id)
            continue

    if running:
        check.summary = 'Some files are running'
        check.brief_output.append(str(len(running)) + ' files are still running.')
        check.full_output['files_running_fastqc'] = running

    if missing_fastqc:
        check.allow_action = True
        check.summary = 'Some files are missing fastqc runs'
        check.brief_output.append(str(len(missing_fastqc)) + ' files lack a successful fastqc run')
        check.full_output['files_without_fastqc'] = missing_fastqc
        check.status = 'WARN'

    if missing_qc:
        check.allow_action = True
        check.summary = 'Some files are missing fastqc runs'
        check.brief_output.append(str(len(missing_qc)) + ' files have successful run but no qc')
        check.full_output['files_without_qc'] = missing_qc
        check.status = 'WARN'

    check.summary = check.summary.strip()
    if not check.brief_output:
        check.brief_output = ['All Good!']
    return check


@action_function(start_fastqc=True, start_qc=True)
def fastqc_start(connection, **kwargs):
    """Start fastqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'fastqc_start')
    action_logs = {'runs_started': []}
    my_auth = ff_utils.get_authentication_with_server({}, ff_env=connection.ff_env)
    # get latest results from identify_files_without_filesize
    fastqc_check = init_check_res(connection, 'fastqc_status')
    fastqc_check_result = fastqc_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    targets = []
    if kwargs.get('start_fastqc'):
        targets.extend(fastqc_check_result.get('files_without_fastqc', []))
    if kwargs.get('start_qc'):
        targets.extend(fastqc_check_result.get('files_without_qc', []))

    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_fastq': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('fastqc-0-11-4-1', 'no_organism', attributions)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        action_logs['runs_started'].append(url)
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
def in_situ_hic_status(connection, **kwargs):
    """Searches for fastq files that don't have fastqc

    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'in_situ_hic_status')
    my_auth = connection.ff_keys

    check.action = "hic_start"
    check.brief_output = []
    check.full_output = {'skipped': [],
                         'needs_runs': [],
                         'needs_finish': []}
    check.status = 'PASS'

    exp_type = 'in situ Hi-C'
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check

    for a_set in res:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            break
        # missing run
        missing_run = []
        # still running
        running = []

        set_acc = a_set['accession']
        set_summary = ""
        part3 = 'done'
        # references dict content
        # pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size, lab
        exp_files, refs = wfr_utils.find_pairs(a_set, my_auth)
        set_summary = " - ".join([set_acc, refs['organism'], refs['enzyme'], refs['f_size'], refs['lab']])
        # skip if missing reference
        if not refs['bwa_ref'] or not refs['chrsize_ref'] or not refs['enz_ref']:
            set_summary += "| skipped - no enz/chrsize/bwa"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append([set_acc, 'skipped - no enz/chrsize/bwa'])
            check.status = 'WARN'
            continue
        set_pairs = []
        # cycle through the experiments, skip the ones without usable files
        for exp in exp_files.keys():
            if not exp_files.get(exp):
                continue
            # Check Part 1 and See if all are okay
            exp_bams = []
            part1 = 'done'
            part2 = 'done'
            for pair in exp_files[exp]:
                step1_result = wfr_utils.get_wfr_out(pair[0], 'bwa-mem', my_auth, ['0.2.6'])
                # if successful
                if step1_result['status'] == 'complete':
                    exp_bams.append(step1_result['bam'])
                    continue
                # if still running
                elif step1_result['status'] == 'running':
                    part1 = 'not done'
                    print('part1 still running')
                    continue
                # if run is not successful
                else:
                    part1 = 'not done'
                    if add_wfr:
                        # RUN PART 1
                        inp_f = {'fastq1':pair[0], 'fastq2':pair[1], 'bwa_index':bwa_ref}
                        name_tag = pair[0].split('/')[2]+'_'+pair[1].split('/')[2]
                        run_missing_wfr(step_settings('bwa-mem', organism, attributions), inp_f, name_tag, my_auth, my_env)
            # stop progress to part2
            if part1 is not 'done':
                print(exp, 'has missing Part1 runs')
                part2 = 'not ready'
                part3 = 'not ready'
                continue
            print(exp, 'part1 complete')

            #make sure all input bams went through same last step2
            all_step2s = []
            for bam in exp_bams:
                step2_result = get_wfr_out(bam, 'hi-c-processing-bam', my_auth, ['0.2.6'])
                all_step2s.append((step2_result['status'],step2_result.get('bam')))
            if len(list(set(all_step2s))) != 1:
                print('inconsistent step2 run for input bams')
                # this run will be repeated if add_wfr
                step2_result['status'] = 'inconsistent run'

            #check if part 2 is run already, it not start the run
            # if successful
            if step2_result['status'] == 'complete':
                set_pairs.append(step2_result['pairs'])
                if add_pc:
                    add_preliminary_processed_files(exp, [step2_result['bam'],step2_result['pairs']], my_auth)
                print(exp, 'part2 complete')
                continue
            # if still running
            elif step2_result['status'] == 'running':
                part2 = 'not done'
                part3 = 'not ready'
                print(exp, 'part2 still running')
                continue
            # if run is not successful
            else:
                part2 = 'not done'
                part3 = 'not ready'
                print(exp, 'is missing Part2')
                if add_wfr:
                    # RUN PART 2
                    inp_f = {'input_bams':exp_bams, 'chromsize':chrsize_ref}
                    run_missing_wfr(step_settings('hi-c-processing-bam', organism, attributions), inp_f, exp, my_auth, my_env)


        if part3 is not 'done':
            print('Part3 not ready')
            continue
        if not set_pairs:
            print('no pairs can be produced from this set')
            continue

        #make sure all input bams went through same last step3
        all_step3s = []
        for a_pair in set_pairs:
            step3_result = get_wfr_out(a_pair, step3, my_auth, ['0.2.6'])
            all_step3s.append((step3_result['status'], step3_result.get('mcool')))
        if len(list(set(all_step3s))) != 1:
            print('inconsistent step3 run for input pairs')
            # this run will be repeated if add_wfr
            step3_result['status'] = 'inconsistent run'
        #check if part 3 is run already, it not start the run
        # if successful
        if step3_result['status'] == 'complete':
            completed += 1
            completed_acc.append(a_set['accession'])
            #add competed flag to experiment
            if add_tag:
                ff_utils.patch_metadata({"completed_processes":["HiC_Pipeline_0.2.5"]}, obj_id=a_set['accession'] , key=my_auth)
            # add processed files to set
            if add_pc:
                add_preliminary_processed_files(a_set['accession'],
                                                [step3_result['pairs'],
                                                 step3_result['hic'],
                                                 step3_result['mcool']],
                                                my_auth)
            print(a_set['accession'], 'part3 complete')
        # if still running
        elif step3_result['status'] == 'running':
            print('part3 still running')
            continue
        # if run is not successful
        else:
            print(a_set['accession'], 'is missing Part3')
            if add_wfr:
                # RUN PART 3
                inp_f = {'input_pairs':set_pairs, 'chromsizes':chrsize_ref}
                if recipe_no in [0,2,4]:
                    inp_f['restriction_file'] = enz_ref
                run_missing_wfr(step_settings(step3, organism, attributions), inp_f, a_set['accession'], my_auth, my_env)

    print(completed)
    print(completed_acc)
















    for a_set in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            break

        file_id = a_fastq['accession']
        report = wfr_utils.get_wfr_out(file_id, 'fastqc-0-11-4-1',  my_auth, md_qc=True)
        if report['status'] == 'running':
            running.append(file_id)
            continue
        # Most probably the trigger did not work, and we run it manually
        if report['status'] != 'complete':
            missing_fastqc.append(file_id)
            continue
        # There is a successful run, but no qc, previously happened when a file was reuploaded.
        if report['status'] == 'complete':
            missing_qc.append(file_id)
            continue

    if running:
        check.summary = 'Some files are running'
        check.brief_output.append(str(len(running)) + ' files are still running.')
        check.full_output['files_running_fastqc'] = running

    if missing_fastqc:
        check.allow_action = True
        check.summary = 'Some files are missing fastqc runs'
        check.brief_output.append(str(len(missing_fastqc)) + ' files lack a successful fastqc run')
        check.full_output['files_without_fastqc'] = missing_fastqc
        check.status = 'WARN'

    if missing_qc:
        check.allow_action = True
        check.summary = 'Some files are missing fastqc runs'
        check.brief_output.append(str(len(missing_qc)) + ' files have successful run but no qc')
        check.full_output['files_without_qc'] = missing_qc
        check.status = 'WARN'

    check.summary = check.summary.strip()
    if not check.brief_output:
        check.brief_output = ['All Good!']
    return check