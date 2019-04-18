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

lambda_limit = wfr_utils.lambda_limit


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
        md5_report = wfr_utils.get_wfr_out(a_file, "md5", key=my_auth, md_qc=True)
        if md5_report['status'] == 'running':
            running.append(file_id)
        # Most probably the trigger did not work, and we run it manually
        elif md5_report['status'] != 'complete':
            missing_md5.append(file_id)
        # There is a successful run, but status is not switched, happens when a file is reuploaded.
        elif md5_report['status'] == 'complete':
            not_switched_status.append(file_id)
    if no_s3_file:
        check.summary = 'Some files are pending upload'
        msg = str(len(no_s3_file)) + '(uploading/upload failed) files waiting for upload'
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
    action_logs = {'runs_started': [], "runs_failed": []}
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
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
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
    query = ("/search/?type=FileFastq&quality_metric.uuid=No+value"
             "&status=pre-release&status=released&status=released%20to%20project&status=uploaded")
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
        report = wfr_utils.get_wfr_out(a_fastq, 'fastqc-0-11-4-1',  key=my_auth, md_qc=True)
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
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
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
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
def pairsqc_status(connection, **kwargs):
    """Searches for pairs files produced by 4dn pipelines that don't have pairsqc
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'pairsqc_status')
    my_auth = connection.ff_keys
    check.action = "pairsqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?file_format.file_format=pairs&type=FileProcessed"
             "&status=pre-release&status=released&status=released+to+project&status=uploaded"
             "&quality_metric.uuid=No+value&limit=all&source_experiments!=No value")
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
    missing_pairsqc = []
    # if there is a successful run but no qc
    missing_qc = []
    running = []
    for a_pairs in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
            break
        file_id = a_pairs['accession']
        report = wfr_utils.get_wfr_out(a_pairs, 'pairsqc-single',  key=my_auth, md_qc=True)
        if report['status'] == 'running':
            running.append(file_id)
            continue
        if report['status'] != 'complete':
            missing_pairsqc.append(file_id)
            continue
        # There is a successful run, but no qc, previously happened when a file was reuploaded.
        if report['status'] == 'complete':
            missing_qc.append(file_id)
            continue
    if running:
        check.summary = 'Some files are running'
        check.brief_output.append(str(len(running)) + ' files are still running.')
        check.full_output['files_running_pairsqc'] = running
    if missing_pairsqc:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_pairsqc)) + ' files lack a successful pairsqc run')
        check.full_output['files_without_pairsqc'] = missing_pairsqc
        check.status = 'WARN'
    if missing_qc:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_qc)) + ' files have successful run but no qc')
        check.full_output['files_without_qc'] = missing_qc
        check.status = 'WARN'
    check.summary = check.summary.strip()
    if not check.brief_output:
        check.brief_output = ['All Good!']
    return check


@action_function(start_pairsqc=True, start_qc=True)
def pairsqc_start(connection, **kwargs):
    """Start pairsqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'pairsqc_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    # get latest results from identify_files_without_filesize
    pairsqc_check = init_check_res(connection, 'pairsqc_status')
    pairsqc_check_result = pairsqc_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    targets = []
    if kwargs.get('start_pairsqc'):
        targets.extend(pairsqc_check_result.get('files_without_pairsqc', []))
    if kwargs.get('start_qc'):
        targets.extend(pairsqc_check_result.get('files_without_qc', []))

    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        exp_accs = a_file.get('source_experiments')
        nz_num, chrsize, max_distance = wfr_utils.extract_nz_chr(exp_accs[0], my_auth)
        # if there are missing info, max distance should have been replaced by the report
        if not nz_num:
            action_logs['runs_failed'].append([a_target, max_distance])
            continue
        additional_setup = {'parameters': {"enzyme": nz_num, "sample_name": a_target}}
        # human does not need this parameter
        if max_distance:
            additional_setup['parameters']['max_distance'] = max_distance
        inp_f = {'input_pairs': a_file['@id'], 'chromsizes': chrsize}
        wfr_setup = wfrset_utils.step_settings('pairsqc-single', 'no_organism', attributions, overwrite=additional_setup)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
def in_situ_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'in_situ_hic_status')
    my_auth = connection.ff_keys
    check.action = "in_situ_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'in situ Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def in_situ_hic_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'in_situ_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'in_situ_hic_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None)
def dilution_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'dilution_hic_status')
    my_auth = connection.ff_keys
    check.action = "dilution_hic_start"
    check.brief_output = []
    check.summary = ""
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'dilution Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    print(len(res))
    if not res:
        check.summary = 'All Good!'
        return check

    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def dilution_hic_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'dilution_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'dilution_hic_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')

    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None)
def tcc_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'tcc_status')
    my_auth = connection.ff_keys
    check.action = "tcc_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'TCC'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=False, nonorm=False)
    return check


@action_function(start_runs=True, patch_completed=True)
def tcc_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'tcc_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'tcc_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None)
def dnase_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'dnase_hic_status')
    my_auth = connection.ff_keys
    check.action = "dnase_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'DNase Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=True, nonorm=False)
    return check


@action_function(start_runs=True, patch_completed=True)
def dnase_hic_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'dnase_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'dnase_hic_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None)
def capture_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'capture_hic_status')
    my_auth = connection.ff_keys
    check.action = "capture_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'capture Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=False, nonorm=True)
    return check


@action_function(start_runs=True, patch_completed=True)
def capture_hic_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'capture_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'capture_hic_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None)
def micro_c_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'micro_c_status')
    my_auth = connection.ff_keys
    check.action = "micro_c_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'micro-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=True, nonorm=False)
    return check


@action_function(start_runs=True, patch_completed=True)
def micro_c_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'micro_c_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'micro_c_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
def chia_pet_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'chia_pet_status')
    my_auth = connection.ff_keys
    check.action = "chia_pet_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'ChIA-PET'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=True, nonorm=True)
    return check


@action_function(start_runs=True, patch_completed=True)
def chia_pet_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'chia_pet_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'chia_pet_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
def trac_loop_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'trac_loop_status')
    my_auth = connection.ff_keys
    check.action = "trac_loop_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'TrAC-loop'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=True, nonorm=True)
    return check


@action_function(start_runs=True, patch_completed=True)
def trac_loop_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'trac_loop_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'trac_loop_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
def plac_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'plac_seq_status')
    my_auth = connection.ff_keys
    check.action = "plac_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [], 'completed_runs': []}
    check.status = 'PASS'
    exp_type = 'PLAC-seq'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nore=False, nonorm=True)
    return check


@action_function(start_runs=True, patch_completed=True)
def plac_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'plac_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    hic_check = init_check_res(connection, 'plac_seq_status')
    hic_check_result = hic_check.get_result_by_uuid(kwargs['called_by']).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action
