from datetime import datetime, timedelta
from dcicutils import ff_utils
from dcicutils.s3_utils import s3Utils
from .helpers import wfr_utils
from .helpers import wfrset_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *

lambda_limit = wfr_utils.lambda_limit


@check_function()
def md5run_status_extra_file(connection, **kwargs):
    """Searches for extra files that are uploaded to s3, but not went though md5 run.
    no action is associated, we don't have any case so far.
    Will be implemented if this check gets WARN"""
    check = CheckResult(connection, 'md5run_status_extra_file')
    my_auth = connection.ff_keys
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query
    query = ('/search/?type=File&status!=uploading&status!=upload failed&status!=to be uploaded by workflow'
             '&status!=archived&status!=archived to project&extra_files.status!=uploaded'
             '&extra_files.status!=to be uploaded by workflow&extra_files.status!=upload failed'
             '&extra_files.href!=No value&extra_files.md5sum=No value')
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    else:
        check.status = 'WARN'
        check.brief_output = ['There are extra files without md5runs']
        check.full_output = {'extra_files_missing_md5': [i['accession'] for i in res]}
        return check


@action_function(start_missing=True)
def md5run_extra_file_start(connection, **kwargs):
    """Start md5 runs for extra files by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'md5run_extra_file_start')
    action_logs = {'runs_started': [], "runs_failed": []}
    my_auth = connection.ff_keys
    md5run_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = md5run_check_result
    targets = []
    if kwargs.get('start_missing'):
        targets.extend(md5run_check_result.get('extra_files_missing_md5', []))
    action_logs['targets'] = targets
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        wfr_setup = wfrset_utils.step_settings('md5', 'no_organism', attributions)

        # get extra files with missing md5sum and retrieve their format as identifier
        extra_formats = []
        for ext in a_file['extra_files']:
            if 'md5sum' not in ext or not ext['md5sum']:
                extra_formats.append(ext['file_format']['display_title'])  # format of extra file

        for extra_format in extra_formats:
            inp_f = {'input_file': a_file['@id'],
                     'additional_file_parameters': {'input_file': {'format_if_extra': extra_format}}}
            url = wfr_utils.run_missing_wfr(
                wfr_setup, inp_f, a_file['accession'],
                connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
            )
            # aws run url
            if url.startswith('http'):
                action_logs['runs_started'].append(url)
            else:
                action_logs['runs_failed'].append([a_target, extra_format, url])

    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(file_type='File', lab_title=None, start_date=None, action="md5run_start")
def md5run_status(connection, **kwargs):
    """Searches for files that are uploaded to s3, but not went though md5 run.
    This check makes certain assumptions
    -all files that have a status<= uploaded, went through md5run
    -all files status uploading/upload failed, and no s3 file are pending, and skipped by this check.
    if you change status manually, it might fail to show up in this checkself.
    Keyword arguments:
    file_type -- limit search to a file type, i.e. FileFastq (default=File)
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'md5run_status')
    my_auth = connection.ff_keys
    check.action = "md5run_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # check number of total workflow runs in the past 6h
    check, n_runs_available = wfr_utils.limit_number_of_runs(check, my_auth)
    if n_runs_available == 0:
        return check

    # Build the query
    query = '/search/?status=uploading&status=upload failed&status!=archived&status!=archived to project'
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
    # multiple failed runs
    problems = []
    my_s3_util = s3Utils(env=connection.ff_env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    for a_file in res[:n_runs_available]:
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
        elif md5_report['status'].startswith("no complete run, too many"):
            problems.append(file_id)
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
    if problems:
        check.summary = 'Some files have problems'
        msg = str(len(problems)) + ' file(s) have problems.'
        check.brief_output.append(msg)
        check.full_output['problems'] = problems
        check.status = 'WARN'
    if missing_md5:
        check.allow_action = True
        check.summary = 'Some files are missing md5 runs'
        msg = str(len(missing_md5)) + ' file(s) lack a successful md5 run'
        check.brief_output.append(msg)
        check.full_output['files_without_md5run'] = missing_md5
        check.status = 'WARN'
    if not_switched_status:
        check.allow_action = True
        check.summary += ' Some files are have wrong status with a successful run'
        msg = str(len(not_switched_status)) + ' file(s) are have wrong status with a successful run'
        check.brief_output.append(msg)
        check.full_output['files_with_run_and_wrong_status'] = not_switched_status
        check.status = 'WARN'
    if not check.brief_output:
        check.brief_output = ['All Good!', ]
    check.summary = check.summary.strip()
    return check


@check_function(action="md5run_start")
def md5run_uploaded_files(connection, **kwargs):
    """
    Search for Files with status uploaded (and higher) that do not have md5sum.

    Since ENCODE pipelines do not add md5sum, ProcessedFiles from ATAC-seq,
    ChIP-seq, and RNA-seq are expected to trigger this check.
    All other files should have md5sum by the time they are uploaded, but this
    check will find any exception.
    """
    check = CheckResult(connection, 'md5run_uploaded_files')
    my_auth = connection.ff_keys
    check.action = "md5run_start"
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query
    statuses = ['uploaded', 'pre-release', 'released to project', 'released',
                'archived to project', 'archived', 'replaced']
    query = '/search/?type=File&md5sum=No+value' + ''.join(['&status=' + s for s in statuses])

    files = {}
    files['uploaded_without_md5run'] = [f['accession'] for f in ff_utils.search_metadata(
        query + '&workflow_run_inputs.workflow.title%21=md5+0.2.6', key=my_auth)]
    files['uploaded_with_md5run'] = [f['accession'] for f in ff_utils.search_metadata(
        query + '&workflow_run_inputs.workflow.title=md5+0.2.6', key=my_auth)]

    if files['uploaded_without_md5run'] or files['uploaded_with_md5run']:
        check.status = 'WARN'
        check.summary = 'Some files need md5 run before release'
        check.description = 'Some files with status updloaded or higher are missing md5sum'
        check.brief_output = {k: str(len(v)) + ' files' for k, v in files.items()}
        check.full_output = files
        if files['uploaded_without_md5run']:
            check.allow_action = True
        if files['uploaded_with_md5run']:
            check.action_message = ('Action will only run on Files without previous md5 run. ' +
                                    'You need to manually fix those with previous md5 run.')
    else:
        check.summary = 'All Good!'
    return check


@action_function(start_missing=True, start_not_switched=True)
def md5run_start(connection, **kwargs):
    """Start md5 runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'md5run_start')
    action_logs = {'runs_started': [], "runs_failed": []}
    my_auth = connection.ff_keys
    md5run_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = md5run_check_result
    targets = []
    if kwargs.get('start_missing'):
        # get uploading files from md5run_status
        targets.extend(md5run_check_result.get('files_without_md5run', []))
        # get uploaded files from md5run_uploaded_files
        targets.extend(md5run_check_result.get('uploaded_without_md5run', []))
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

        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="fastqc_start")
def fastqc_status(connection, **kwargs):
    """Searches for fastq files that don't have fastqc
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'fastqc_status')
    my_auth = connection.ff_keys
    check.action = "fastqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # check number of total workflow runs in the past 6h
    check, n_runs_available = wfr_utils.limit_number_of_runs(check, my_auth)
    if n_runs_available == 0:
        return check

    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?type=File&file_format.file_format=fastq&quality_metric.uuid=No+value"
             "&status=pre-release&status=released&status=released%20to%20project&status=uploaded")
    # fastqc not properly reporting for long reads
    skip_instruments = ['PromethION', 'GridION', 'MinION', 'PacBio RS II']
    skip_add = "".join(['&instrument!=' + i for i in skip_instruments])
    query += skip_add

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
    check = wfr_utils.check_runs_without_output(res[:n_runs_available], check, 'fastqc', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def fastqc_start(connection, **kwargs):
    """Start fastqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'fastqc_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    fastqc_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(fastqc_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(fastqc_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_fastq': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('fastqc', 'no_organism', attributions)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'],
                                        connection.ff_keys, connection.ff_env, connection.fs_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="pairsqc_start")
def pairsqc_status(connection, **kwargs):
    """Searches for pairs files produced by 4dn pipelines that don't have pairsqc
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'pairsqc_status')
    my_auth = connection.ff_keys
    check.action = "pairsqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    check = wfr_utils.check_runs_without_output(res, check, 'pairsqc-single', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def pairsqc_start(connection, **kwargs):
    """Start pairsqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'pairsqc_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    pairsqc_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(pairsqc_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(pairsqc_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        exp_accs = a_file.get('source_experiments')
        # if not from tibanna, look for calc prop
        if not exp_accs:
            exp_sets = a_file.get('experiment_sets')
            if not exp_sets:
                action_logs['runs_failed'].append([a_target, 'can not find assc. experiment'])
                continue
            my_set = ff_utils.get_metadata(exp_sets[0]['uuid'], my_auth)
            exp_accs = [i['accession'] for i in my_set['experiments_in_set']]
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
        wfr_setup = wfrset_utils.step_settings('pairsqc-single',
                                               'no_organism',
                                               attributions,
                                               overwrite=additional_setup)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=False
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="bg2bw_start")
def bg2bw_status(connection, **kwargs):
    """Searches for pairs files produced by 4dn pipelines that don't have bg2bw
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'bg2bw_status')
    my_auth = connection.ff_keys
    check.action = "bg2bw_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find bg files without bw files)
    query = ("/search/?type=FileProcessed&file_format.file_format=bg"
             "&extra_files.file_format.display_title!=bw"
             "&status!=uploading&status!=to be uploaded by workflow"
             "&status!=archived&status!=archived to project")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab

    # build a second query for checking failed ones
    query_f = ("/search/?type=FileProcessed&file_format.file_format=bg"
               "&extra_files.file_format.display_title=bw"
               "&extra_files.status=uploading"
               "&extra_files.status=to be uploaded by workflow"
               "&status!=uploading&status!=to be uploaded by workflow")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query_f += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query_f += '&lab.display_title=' + lab

    # The search
    res_one = ff_utils.search_metadata(query, key=my_auth)
    res_two = ff_utils.search_metadata(query_f, key=my_auth)
    res = res_one + res_two
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_runs_without_output(res, check, 'bedGraphToBigWig', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bg2bw_start(connection, **kwargs):
    """Start bg2bw runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bg2bw_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bg2bw_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bg2bw_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bg2bw_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = [k for k, v in wfr_utils.mapper.items() if v == a_file['genome_assembly']][0]
        chrsize = wfr_utils.chr_size[org]

        inp_f = {'bgfile': a_file['@id'], 'chromsize': chrsize}
        wfr_setup = wfrset_utils.step_settings('bedGraphToBigWig',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="bed2beddb_start")
def bed2beddb_status(connection, **kwargs):
    """Searches for small bed files uploaded by user in certain types
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'bed2beddb_status')
    my_auth = connection.ff_keys
    check.action = "bed2beddb_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    check.summary = ''

    # These are the accepted file types for this check
    accepted_types = ['LADs', 'boundaries', 'domain calls', 'peaks', 'target regions']

    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find bg files without bw files)
    query = ("/search/?type=File&file_format.file_format=bed"
             "&extra_files.file_format.display_title!=beddb"
             "&status!=uploading&status!=to be uploaded by workflow"
             "&status!=archived&status!=archived to project")
    query += "".join(["&file_type=" + i for i in accepted_types])
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab

    # build a second query for checking failed ones
    query_f = ("/search/?type=File&file_format.file_format=bed"
               "&extra_files.file_format.display_title=beddb"
               "&extra_files.status=uploading"
               "&extra_files.status=to be uploaded by workflow"
               "&status!=uploading&status!=to be uploaded by workflow")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query_f += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query_f += '&lab.display_title=' + lab

    # The search
    res_one = ff_utils.search_metadata(query, key=my_auth)
    res_two = ff_utils.search_metadata(query_f, key=my_auth)
    res_all = res_one + res_two
    missing = []
    for a_file in res_all:
        if not a_file.get('genome_assembly'):
            missing.append(a_file['accession'])
    res_all = [i for i in res_all if i.get('genome_assembly')]
    if not res_all:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_runs_without_output(res_all, check, 'bedtobeddb', my_auth, start)
    if missing:
        check.full_output['missing_assembly'] = missing
        msg = str(len(missing)) + ' files missing genome assembly'
        check.brief_output.insert(0, msg)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bed2beddb_start(connection, **kwargs):
    """Start bed2beddb runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bed2beddb_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bed2beddb_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bed2beddb_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bed2beddb_check_result.get('files_without_changes', []))

    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = [k for k, v in wfr_utils.mapper.items() if v == a_file['genome_assembly']][0]
        chrsize = wfr_utils.chr_size[org]

        inp_f = {'bedfile': a_file['@id'], 'chromsizes_file': chrsize}
        wfr_setup = wfrset_utils.step_settings('bedtobeddb',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="in_situ_hic_start")
def in_situ_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'in_situ_hic_status')
    my_auth = connection.ff_keys
    check.action = "in_situ_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'in situ Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'in_situ_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="dilution_hic_start")
def dilution_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'dilution_hic_status')
    my_auth = connection.ff_keys
    check.action = "dilution_hic_start"
    check.brief_output = []
    check.summary = ""
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'Dilution Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'dilution_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="tcc_start")
def tcc_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'tcc_status')
    my_auth = connection.ff_keys
    check.action = "tcc_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'TCC'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'tcc_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="dnase_hic_start")
def dnase_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'dnase_hic_status')
    my_auth = connection.ff_keys
    check.action = "dnase_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'DNase Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'dnase_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="capture_hic_start")
def capture_hic_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'capture_hic_status')
    my_auth = connection.ff_keys
    check.action = "capture_hic_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'Capture Hi-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'capture_hic_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="micro_c_start")
def micro_c_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'micro_c_status')
    my_auth = connection.ff_keys
    check.action = "micro_c_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'Micro-C'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'micro_c_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="chia_pet_start")
def chia_pet_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'chia_pet_status')
    my_auth = connection.ff_keys
    check.action = "chia_pet_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'ChIA-PET'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'chia_pet_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None, action="in_situ_chia_pet_start")
def in_situ_chia_pet_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'in_situ_chia_pet_status')
    my_auth = connection.ff_keys
    check.action = "in_situ_chia_pet_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'in situ ChIA-PET'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_hic(res, my_auth, tag, check, start, lambda_limit, nonorm=True)
    return check


@action_function(start_runs=True, patch_completed=True)
def in_situ_chia_pet_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'in_situ_chia_pet_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None, action="trac_loop_start")
def trac_loop_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'trac_loop_status')
    my_auth = connection.ff_keys
    check.action = "trac_loop_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'TrAC-loop'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'trac_loop_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="plac_seq_start")
def plac_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'plac_seq_status')
    my_auth = connection.ff_keys
    check.action = "plac_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'PLAC-seq'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
    action = ActionResult(connection, 'plac_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action

@check_function(lab_title=None, start_date=None, action="hichip_start")
def hichip_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'hichip_status')
    my_auth = connection.ff_keys
    check.action = "hichip_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'HiChIP'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
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
def hichip_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'hichip_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True)
    return action


@check_function(lab_title=None, start_date=None, action="repli_2_stage_start")
def repli_2_stage_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'repli_2_stage_status')
    my_auth = connection.ff_keys
    check.action = "repli_2_stage_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = '2-stage Repli-seq'
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_repli(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def repli_2_stage_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'repli_2_stage_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,
                                   move_to_pc=True,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None, action="repli_multi_stage_start")
def repli_multi_stage_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'repli_multi_stage_status')
    my_auth = connection.ff_keys
    check.action = "repli_multi_stage_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'Multi-stage Repli-seq'
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_repli(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def repli_multi_stage_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'repli_multi_stage_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,
                                   move_to_pc=True,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None, action="tsa_seq_start")
def tsa_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'tsa_seq_status')
    my_auth = connection.ff_keys
    check.action = "tsa_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'TSA-seq'
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_repli(res, my_auth, tag, check, start, lambda_limit, winsize=25000)
    return check


@action_function(start_runs=True, patch_completed=True)
def tsa_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'tsa_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    print(check_result)
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,
                                   move_to_pc=False,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None, action="nad_seq_start")
def nad_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'nad_seq_status')
    my_auth = connection.ff_keys
    check.action = "nad_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'NAD-seq'
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_repli(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def nad_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'nad_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,
                                   move_to_pc=False,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None, action="atac_seq_start")
def atac_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'atac_seq_status')
    my_auth = connection.ff_keys
    check.action = "atac_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = ['All Good!']
    check.summary = "All Good!"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'ATAC-seq'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    return check


@action_function(start_runs=True, patch_completed=True)
def atac_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'atac_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None, action="margi_start")
def margi_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'margi_status')
    my_auth = connection.ff_keys
    check.action = "margi_start"
    check.brief_output = []
    check.summary = ""
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'MARGI'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    print(len(res))
    if not res:
        check.summary = 'All Good!'
        return check

    check = wfr_utils.check_margi(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def margi_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'margi_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    margi_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = margi_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = margi_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True, runtype='margi')
    return action


@check_function(lab_title=None, start_date=None, action="bed2multivec_start")
def bed2multivec_status(connection, **kwargs):
    """Searches for bed files states types that don't have bed2multivec
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'bed2multivec_status')
    my_auth = connection.ff_keys
    check.action = "bed2multivec_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find bed files without bed.multires.mv5 files)
    query = ("search/?type=FileProcessed&file_format.file_format=bed&file_type=chromatin states"
             "&extra_files.file_format.display_title!=bed.multires.mv5"
             "&status!=uploading&status!=to be uploaded by workflow"
             "&status!=archived&status!=archived to project")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab

    # build a second query for checking failed ones
    query_f = ("search/?type=FileProcessed&file_format.file_format=bed&file_type=chromatin states"
               "&extra_files.file_format.display_title=bed.multires.mv5"
               "&extra_files.status=uploading"
               "&extra_files.status=to be uploaded by workflow"
               "&status!=uploading&status!=to be uploaded by workflow")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query_f += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query_f += '&lab.display_title=' + lab

    # The search
    res_one = ff_utils.search_metadata(query, key=my_auth)
    res_two = ff_utils.search_metadata(query_f, key=my_auth)
    res = res_one + res_two

    if not res:
        check.summary = 'All Good!'
        return check
    # create 2 lists, one healthy one problematic_run
    prb_res = []
    healthy_res = []

    for a_res in res:
        response, reason = wfr_utils.isthere_states_tag(a_res)
        if response:
            if a_res.get('higlass_defaults'):
                healthy_res.append(a_res)  # only run in files with tags and higlass_defaults
            else:
                prb_res.append((a_res, 'missing higlass_defaults'))

        else:
            prb_res.append((a_res, reason))

    if not healthy_res and not prb_res:
        check.summary = 'All Good!'
        return check

    if not healthy_res and prb_res:
        check.full_output['prob_files'] = [{'missing tag': [i[0]['accession'] for i in prb_res if i[1] == 'missing_tag'],
                                            'unregistered tag': [[i[0]['accession'], i[0]['tags']] for i in prb_res if i[1] == 'unregistered_tag'],
                                            'missing higlass_defaults': [i[0]['accession'] for i in prb_res if i[1] == 'missing higlass_defaults']}]

        check.status = 'WARN'
        return check

    check = wfr_utils.check_runs_without_output(healthy_res, check, 'bedtomultivec', my_auth, start)
    if prb_res:
        check.full_output['prob_files'] = [{'missing tag': [i[0]['accession'] for i in prb_res if i[1] == 'missing_tag'],
                                            'invalid tag': [i[0]['accession'] for i in prb_res if i[1] == 'invalid_tag']}]
        check.status = 'WARN'
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bed2multivec_start(connection, **kwargs):
    """Start bed2multivec runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bed2multivec_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bed2multivec_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bed2multivec_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bed2multivec_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = [k for k, v in wfr_utils.mapper.items() if v == a_file['genome_assembly']][0]
        states_tag = [i for i in a_file['tags'] if 'states' in i][0]

        chrsize = wfr_utils.chr_size[org]
        rows_info = wfr_utils.states_file_type[states_tag]['color_mapper']
        num_rows = wfr_utils.states_file_type[states_tag]['num_states']
        # Add function to calculate resolution automatically
        parameters = {'parameters': {'num_rows': num_rows, 'resolution': 6250}}

        inp_f = {'bedfile': a_file['@id'], 'chromsizes_file': chrsize, 'rows_info': rows_info}
        wfr_setup = wfrset_utils.step_settings('bedtomultivec',
                                               'no_organism',
                                               attributions, parameters)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, connection.fs_env
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="rna_strandedness_start")
def rna_strandedness_status(connection, **kwargs):
    """Searches for fastq files from RNA-seq that don't have beta_actin_count fields
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'rna_strandedness_status')
    my_auth = connection.ff_keys
    check.action = "rna_strandedness_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    problematic = []
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Get Fastq files from RNA-seq that are missing beta-actin counts
    files = ff_utils.search_metadata(
        '/search/?type=FileFastq&file_format.file_format=fastq&track_and_facet_info.experiment_type=RNA-seq'
        + '&experiments.biosample.biosource.organism.name!=No+value'
        + '&beta_actin_sense_count=No+value&beta_actin_antisense_count=No+value'
        + '&status=pre-release&status=released&status=released to project',
        key=my_auth)
    # Get RNA-seq sets that are tagged with skip_processing
    expsets_to_skip = ff_utils.search_metadata(
        '/search/?type=ExperimentSetReplicate&experiments_in_set.experiment_type.display_title=RNA-seq'
        + '&experiments_in_set.biosample.biosource.organism.name!=No+value'
        + '&status=pre-release&status=released&status=released to project'
        + '&tags=skip_processing&field=accession',
        key=my_auth)
    expsets_acc_to_skip = [expset['accession'] for expset in expsets_to_skip]

    targets = []
    for a_file in files:
        replicate_expsets = [es['accession'] for es in a_file['experiments'][0]['experiment_sets'] if es['experimentset_type'] == 'replicate']
        if replicate_expsets and replicate_expsets[0] in expsets_acc_to_skip:
            continue  # skip this file if the expset is tagged with skip_processing

        org = a_file['experiments'][0]['biosample']['biosource'][0]['organism']['name']
        kmer_file = wfr_utils.re_kmer.get(org)
        if kmer_file:
            targets.append(a_file)
        else:
            problematic.append([a_file['accession'], 'missing re_kmer reference file for %s'%(org)])

    if not targets and not problematic:
        check.summary = "All good!"
        return check

    running = []
    missing_run = []
    check.summary = ""

    for a_file in targets:
        strandedness_report = wfr_utils.get_wfr_out(a_file, "rna-strandedness", key=my_auth, versions='v2', md_qc=True)
        if strandedness_report['status'] == 'running':
            running.append(a_file['accession'])
        elif strandedness_report['status'] != 'complete':
            missing_run.append(a_file['accession'])

    if running:
        check.summary += '|' + str(len(running)) + ' running'
        msg = str(len(running)) + ' files are still running rna_strandedness run.'
        check.brief_output.append(msg)
        check.full_output['files_running_rna_strandedness_run'] = running

    if missing_run:
        check.summary += '|' + str(len(missing_run)) + ' missing'
        msg = str(len(missing_run)) + ' file(s) lack a successful rna_strandedness run'
        check.brief_output.append(msg)
        check.full_output['files_without_rna_strandedness_run'] = missing_run
        check.allow_action = True
        check.status = 'WARN'
    if problematic:
        check.summary += '|' + str(len(problematic)) + ' skipped'
        msg = str(len(problematic)) + ' file(s) skipped - missing re_kmer reference file'
        check.brief_output.append(msg)
        check.full_output['problematic_files'] = problematic
        check.status = 'WARN'

    return check


@action_function(start_missing=True)
def rna_strandedness_start(connection, **kwargs):
    """Start rna_strandness runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'rna_strandedness_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    rna_strandedness_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    action_logs['kwargs'] = kwargs
    if kwargs.get('start_missing'):
        targets.extend(rna_strandedness_check_result.get('files_without_rna_strandedness_run', []))

    action_logs['targets'] = targets
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = a_file['experiments'][0]['biosample']['biosource'][0]['organism']['name']
        kmer_file = wfr_utils.re_kmer[org]
        # Add function to calculate resolution automatically
        inp_f = {'fastq': a_file['@id'], 'ACTB_reference': kmer_file}
        wfr_setup = wfrset_utils.step_settings('rna-strandedness',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, connection.fs_env
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, query='', action="rna_seq_start")
def rna_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'rna_seq_status')
    my_auth = connection.ff_keys
    check.action = "rna_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = "All Good!"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'

    exp_type = 'RNA-seq'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    user_query = kwargs.get('query')
    if user_query:
        query = user_query
    else:
        query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    print(query)
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_rna(res, my_auth, tag, check, start, lambda_limit)
    return check


@action_function(start_runs=True, patch_completed=True)
def rna_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'rna_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True, runtype='rnaseq')
    return action


@check_function(lab_title=None, start_date=None, action="bamqc_start")
def bamqc_status(connection, **kwargs):
    """Searches for annotated bam files that do not have a qc object
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'bamqc_status')
    my_auth = connection.ff_keys
    check.action = "bamqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find bam files produced bt the Hi-C Post Alignment Processing wfr)
    default_stati = 'released&status=uploaded&status=released+to+project&status=restricted'
    wfr_outputs = "&workflow_run_outputs.workflow.title=Hi-C+Post-alignment+Processing+0.2.6"
    stati = 'status=' + (kwargs.get('status') or default_stati)
    query = 'search/?file_type=alignments&{}'.format(stati)
    query += '&type=FileProcessed'
    query += wfr_outputs
    query += '&quality_metric.display_title=No+value'
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
        check.action_message = 'No action required at this moment'
        check.summary = 'All Good!'
        return check
    check.summary = '{} files need a bamqc'. format(len(res))
    check.status = 'WARN'
    check = wfr_utils.check_runs_without_output(res, check, 'bamqc', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bamqc_start(connection, **kwargs):
    """Start bamqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bamqc_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bamqc_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bamqc_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bamqc_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        org = [k for k, v in wfr_utils.mapper.items() if v == a_file['genome_assembly']][0]
        chrsize = wfr_utils.chr_size[org]

        inp_f = {'bamfile': a_file['@id'], 'chromsizes': chrsize}
        wfr_setup = wfrset_utils.step_settings('bamqc',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, action="fastq_first_line_start")
def fastq_first_line_status(connection, **kwargs):
    print('Entering the check function')
    """Searches for fastq files that don't have file_first_line field
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'fastq_first_line_status')
    my_auth = connection.ff_keys
    check.action = "fastq_first_line_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # check number of total workflow runs in the past 6h
    check, n_runs_available = wfr_utils.limit_number_of_runs(check, my_auth)
    if n_runs_available == 0:
        return check

    query = ('/search/?status=uploaded&status=pre-release&status=released+to+project&status=released'
             '&type=FileFastq&file_format.file_format=fastq&file_first_line=No value&status=restricted')

    # The search
    print('About to query ES for files')
    res = ff_utils.search_metadata(query, key=my_auth)

    if not res:
        check.summary = "All good!"
        return check

    running = []
    missing_run = []

    print('About to check for workflow runs for each file')
    for a_file in res[:n_runs_available]:
        fastq_formatqc_report = wfr_utils.get_wfr_out(a_file, "fastq-first-line", key=my_auth, md_qc=True)
        if fastq_formatqc_report['status'] == 'running':
            running.append(a_file['accession'])
        elif fastq_formatqc_report['status'] != 'complete':
            missing_run.append(a_file['accession'])
    print('Done! I have the results I want')
    if running:
        check.summary = 'Some files are running fastq_formatqc run'
        msg = str(len(running)) + ' files are still running fastq_formatqc run.'
        check.brief_output.append(msg)
        check.full_output['files_running_fastq_formatqc_run'] = running

    if missing_run:
        check.summary = 'Some files are fastq_formatqc run'
        msg = str(len(missing_run)) + ' file(s) lack a successful fastq_formatqc run'
        check.brief_output.append(msg)
        check.full_output['files_without_fastq_formatqc_run'] = missing_run
        check.allow_action = True
        check.status = 'WARN'

    return check


@action_function(start_missing=True)
def fastq_first_line_start(connection, **kwargs):
    """Start fastq_formatqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'fastq_first_line_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    fastq_formatqc_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    action_logs['kwargs'] = kwargs
    if kwargs.get('start_missing'):
        targets.extend(fastq_formatqc_check_result.get('files_without_fastq_formatqc_run', []))

    action_logs['targets'] = targets
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        # Add function to calculate resolution automatically
        inp_f = {'fastq': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('fastq-first-line',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(action="bam_re_start")
def bam_re_status(connection, **kwargs):
    """Searches for fastq files that don't have bam_re

    If a file has an associated enzyme that isn't in the list of acceptable enzymes,
    or if it has no associated enzyme, it will be added to the list of skipped files.
    """
    # AluI pattern seems to be problematic and disabled until it its fixed
    # ChiA pet needs a new version of this check and disabled on this one
    start = datetime.utcnow()
    check = CheckResult(connection, 'bam_re_status')
    my_auth = connection.ff_keys
    check.action = "bam_re_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    # check, skip = wfr_utils.check_indexing(check, connection)
    # if skip:
    #     return check
    # Build the query (skip to be uploaded by workflow)
    exp_types = ['in+situ+Hi-C',
                 'Dilution+Hi-C',
                 'Capture+Hi-C',
                 'TCC',
                 # 'in+situ+ChIA-PET',
                 'PLAC-seq']
    query = ("/search/?file_format.file_format=bam&file_type=alignments&type=FileProcessed"
             "&status!=uploading&status!=to be uploaded by workflow&tags!=skip_processing")
    exp_type_key = '&track_and_facet_info.experiment_type='
    exp_type_filter = exp_type_key + exp_type_key.join(exp_types)
    exclude_processed = '&percent_clipped_sites_with_re_motif=No value'
    query += exp_type_filter
    query += exclude_processed
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    # check if they were processed with an acceptable enzyme
    # per https://github.com/4dn-dcic/docker-4dn-RE-checker/blob/master/scripts/4DN_REcount.pl#L74
    acceptable_enzymes = [  # "AluI",
        "NotI", "MboI", "DpnII", "HindIII", "NcoI", "MboI+HinfI", "HinfI+MboI",  # from the workflow
        "MspI", "NcoI_MspI_BspHI", "DdeI", "DdeI and DpnII", "MseI","Arima - A1, A2"  # added patterns in action
    ]
    # make a new list of files to work on
    filtered_res = []
    # make a list of skipped files
    missing_nz_files = []
    # files without enzyme info
    no_nz = []
    # make a list of skipped enzymes
    missing_nz = []
    for a_file in res:
        # expecting all files to have an experiment
        nz = a_file.get('experiments')[0].get('digestion_enzyme', {}).get('name')
        if nz in acceptable_enzymes:
            filtered_res.append(a_file)
        # make sure nz is not None
        elif nz:
            missing_nz_files.append(a_file)
            if nz not in missing_nz:
                missing_nz.append(nz)
        else:
            no_nz.append(a_file)


    check = wfr_utils.check_runs_without_output(filtered_res, check, 're_checker_workflow', my_auth, start)
    if missing_nz:
        skipped_files = str(len(res) - len(filtered_res))
        nzs = ', '.join(missing_nz)
        message = 'INFO: skipping files ({}) using {}'.format(skipped_files, nzs)
        check.summary += ', ' + message
        check.brief_output.insert(0, message)
        check.full_output['skipped'] = [i['accession'] for i in missing_nz_files]
        check.status = 'WARN'
    if no_nz:
        message = 'INFO: skipping files ({}) without an associated enzyme'.format(len(no_nz))
        check.summary += ', ' + message
        check.brief_output.insert(0, message)
        check.full_output['skipped_no_enzyme'] = [i['accession'] for i in no_nz]
        check.status = 'WARN'
    check.summary = check.summary.lstrip(',').lstrip()
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bam_re_start(connection, **kwargs):
    """Start bam_re runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bam_re_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bam_re_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    # these enzymes are not covered by workflow, but can be covered with these patterns
    nz_patterns = {
        "DdeI and DpnII": {"motif": {"regex": "CT.AT.AG|CT.AGATC|GATCT.AG|GATCGATC"}},
        "DdeI": {"motif": {"regex": "CT.AT.AG"}},
        "MspI": {"motif": {"regex": "CCGCGG|GGCGCC"}},
        "NcoI_MspI_BspHI": {"motif": {"regex": ("CCATGCATGG|CCATGCATGA|CCATGCGG|TCATGCATGG|TCATGCATGA|TCATGCGG|"
                                                "CCGCATGG|CCGCATGA|CCGCGG|GGTACGTACC|AGTACGTACC|GGCGTACC|GGTACGTACT|"
                                                "AGTACGTACT|GGCGTACT|GGTACGCC|AGTACGCC|GGCGCC")}},
        "MseI": {"motif": {"regex": "TTATA"}},
        "Arima - A1, A2": {"motif": {"regex": "GATCGATC|GA.TGATC|GA.TA.TC|GATCA.TC|"
                                              "CT.ACTAG|CT.AT.AG|CTAGT.AG|CTAGCTAG"}}}

    if kwargs.get('start_missing_run'):
        targets.extend(bam_re_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bam_re_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        nz = a_file.get('experiments')[0].get('digestion_enzyme', {}).get('name')
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'bamfile': a_file['@id']}
        if nz in nz_patterns:
            additional_setup = {"parameters": nz_patterns[nz]}
        else:
            additional_setup = {"parameters": {"motif": {"common_enz": nz}}}
        wfr_setup = wfrset_utils.step_settings('re_checker_workflow',
                                               'no_organism',
                                               attributions,
                                               overwrite=additional_setup)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'],
            connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(action="insulation_scores_and_boundaries_start")
def insulation_scores_and_boundaries_status(connection, **kwargs):
    """Calls insulation scores and boundaries on mcool files produced by the Hi-C pipeline"""

    check = CheckResult(connection, 'insulation_scores_and_boundaries_status')
    my_auth = connection.ff_keys
    check.action = "insulation_scores_and_boundaries_start"
    check.description = ""
    check.brief_output = []
    check.summary = ""
    check.full_output = {'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_types = ['in situ Hi-C', 'Dilution Hi-C', 'Micro-C', 'DNase Hi-C']
    feature = 'insulation_scores_and_boundaries'
    # minimum number of reads in the mcool file (100M)
    reads_cutoff = 100000000
    # completion tag
    tag = wfr_utils.feature_calling_accepted_versions[feature][-1]
    # check indexing queue

    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check

    # Build the first query, experiments that have run the hic pipeline. add date and lab if available
    query = wfr_utils.build_feature_calling_query(exp_types, feature, kwargs)

    # filter expSets by the total number of reads in the mcoolfile (found in the combined-pairs file qc)
    query += '&processed_files.file_format.display_title=pairs'
    query += f'&processed_files.quality_metric.Total reads.from={reads_cutoff}'

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)

    if not res:
        check.summary = 'All Good!'
        return check

    for a_res in res:
        skip = False
        running = []
        completed = {'patch_opf': [], 'add_tag': []}
        missing_run = []
        problematic_run = []
        for pfile in a_res['processed_files']:
            if pfile['file_format']['display_title'] == 'mcool':
                file_meta = ff_utils.get_metadata(pfile['accession'], key=my_auth)
                qc_values = file_meta['quality_metric']['quality_metric_summary'][0]['value']
                problematic_resolutions = qc_values.split('; ')
                # verify if binsize is good
                enz = a_res['experiments_in_set'][0]['digestion_enzyme']['name']
                organism = a_res['experiments_in_set'][0]['biosample']['biosource'][0]['organism']['name']
                re_enz_size = wfr_utils.re_nz_sizes.get(enz)
                if not re_enz_size:
                    if enz in ["MNase", "DNaseI"]:  # Treat MNase and DNaseI as 4-cutter enzymes to determine binsize
                        re_enz_size = "4"
                    else:
                        check.full_output['problematic_runs'].append({a_res['accession']: ['%s missing enz site length' % (enz)]})
                        continue

                if int(re_enz_size) == 4:  # if 4-cutter binsize is 5k
                    binsize = 5000
                elif int(re_enz_size) == 6:  # if 6-cutter binsize is 10k
                    binsize = 10000
                if str(binsize) in problematic_resolutions:
                    skip = True
                    continue
                # # Skip problematic mcools files for now, until qc metrics for mcools are in place
                # if file_meta.get('tags'):
                #     if 'skip_domain_callers' in file_meta['tags']:
                #         skip = True
                #         continue
                insu_and_boun_report = wfr_utils.get_wfr_out(file_meta, "insulation-scores-and-boundaries-caller", key=my_auth)
        if skip:
            continue
        elif insu_and_boun_report['status'] == 'running':
            running.append(pfile['accession'])
        elif insu_and_boun_report['status'].startswith("no complete run, too many"):
            problematic_run.append(['step1', a_res['accession'], pfile['accession']])
        elif insu_and_boun_report['status'] != 'complete':
            overwrite = {'parameters': {"binsize": binsize}}
            inp_f = {'mcoolfile': pfile['accession']}
            missing_run.append(['step1', ['insulation-scores-and-boundaries-caller', organism, overwrite],
                                inp_f, a_res['accession']])
        else:
            patch_data = [insu_and_boun_report['bedfile'], insu_and_boun_report['bwfile']]
            completed['patch_opf'].append([a_res['accession'], patch_data])
            completed['add_tag'] = [a_res['accession'], tag]

        if running:
            check.full_output['running_runs'].append({a_res['accession']: running})
        if missing_run:
            check.full_output['needs_runs'].append({a_res['accession']: missing_run})
        if completed.get('add_tag'):
            assert not running
            assert not missing_run
            check.full_output['completed_runs'].append(completed)
        if problematic_run:
            check.full_output['problematic_runs'].append({a_res['accession']: problematic_run})
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.allow_action = True
        check.status = 'WARN'
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.allow_action = True
        check.status = 'WARN'
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'

    return check


@action_function(start_runs=True, patch_completed=True)
def insulation_scores_and_boundaries_start(connection, **kwargs):
    """Start insulation scores and boundaries caller runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'insulation_scores_and_boundaries_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    insu_and_boun_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = insu_and_boun_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = insu_and_boun_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True, runtype='insulation_scores_and_boundaries', pc_append=True)
    return action


@check_function(limit_to_uuids="", action="long_running_wfrs_fdn_start")
def long_running_wfrs_fdn_status(connection, **kwargs):
    """
    Find all runs with run status running/started. Action will cleanup their metadata, and this action might
    lead to new runs being started.
    arg:
     - limit_to_uuids: comma separated uuids to be returned to be deleted, to be used when a subset of runs needs cleanup
                       should also work if a list item is provided as input
    """
    check = CheckResult(connection, 'long_running_wfrs_fdn_status')
    my_auth = connection.ff_keys
    check.action = "long_running_wfrs_fdn_start"
    check.description = "Find runs running longer than specified, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = []
    check.status = 'PASS'
    check.allow_action = False
    # get workflow run limits
    workflow_details = wfr_utils.workflow_details
    # find all runs thats status is not complete or error
    q = '/search/?type=WorkflowRun&run_status!=complete&run_status!=error'
    running_wfrs = ff_utils.search_metadata(q, my_auth)

    # if a comma separated list of uuids is given, limit the result to them
    uuids = str(kwargs.get('limit_to_uuids'))
    if uuids:
        uuids = wfr_utils.string_to_list(uuids)
        running_wfrs = [i for i in running_wfrs if i['uuid'] in uuids]

    if not running_wfrs:
        check.summary = 'All Good!'
        return check

    print(len(running_wfrs))
    # times are UTC on the portal
    now = datetime.utcnow()
    long_running = 0

    for a_wfr in running_wfrs:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        # user submitted ones use run on insteand of run
        time_info = time_info.strip('on').strip()
        try:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S')
        run_time = (now - wfr_time).total_seconds() / 3600
        run_type = wfr_type_base.strip()
        # get run_limit, if wf not found set it to an hour, we should have an entry for all runs
        run_limit = workflow_details.get(run_type, {}).get('run_time', 10)
        if run_time > run_limit:
            long_running += 1
            # find all items to be deleted
            delete_list_uuid = wfr_utils.fetch_wfr_associated(a_wfr)
            check.full_output.append({'wfr_uuid': a_wfr['uuid'],
                                      'wfr_type': run_type,
                                      'wfr_run_time': str(int(run_time)) + 'h',
                                      'wfr_run_status': a_wfr['run_status'],
                                      'wfr_status': a_wfr['status'],
                                      'items_to_delete': delete_list_uuid})
    if long_running:
        check.allow_action = True
        check.status = 'WARN'
        check.summary = "Found {} run(s) running longer than expected".format(long_running)
    else:
        check.summary = 'All Good!'
    return check


PROTECTED_STATUSES = ["released", "released to project", "current", "archived", "archived to project"]


@action_function()
def long_running_wfrs_fdn_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'long_running_wfrs_fdn_start')
    my_auth = connection.ff_keys
    long_running_wfrs_fdn_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    deleted_wfrs = 0
    status_protected = 0
    for a_wfr in long_running_wfrs_fdn_check_result:
        # don't deleted if item is in protected statuses
        if a_wfr['wfr_status'] in PROTECTED_STATUSES:
            status_protected += 1
        else:
            deleted_wfrs += 1
            for an_item_to_delete in a_wfr['items_to_delete']:
                ff_utils.patch_metadata({'status': 'deleted'}, an_item_to_delete, my_auth)
    msg = '{} wfrs were removed'.format(str(deleted_wfrs))
    if status_protected:
        msg += ', {} wfrs were skipped due to protected item status.'.format(str(status_protected))
    action.output = msg
    action.status = 'DONE'
    return action


@check_function(delete_categories='Rerun', limit_to_uuids="", days_back='30', action="problematic_wfrs_fdn_start")
def problematic_wfrs_fdn_status(connection, **kwargs):
    """
    Find all runs with run status error. Action will cleanup their metadata, and this action might
    lead to new runs being started.
    arg:
     - delete_category: comma separated category list
                        which categories to delete with action, by default Rerun is deleted
     - limit_to_uuids: comma separated uuids to be returned to be deleted, to be used when a subset of runs needs cleanup
                       should also work if a list item is provided as input
     - days_back: (string) limit the search to recently created wfrs, up to n days ago. If 0, search all wfrs.
    """
    check = CheckResult(connection, 'problematic_wfrs_fdn_status')
    my_auth = connection.ff_keys
    check.action = "problematic_wfrs_fdn_start"
    check.description = "Find errored runs, action will delete the metadata for cleanup, which might lead to re-runs by pipeline checks"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'report_only': [], 'cleanup': []}
    check.status = 'PASS'
    check.allow_action = False
    # find all errored runs
    q = '/search/?type=WorkflowRun&run_status=error'
    if kwargs.get('days_back'):
        try:
            days_back = int(kwargs['days_back'])
            if days_back != 0:
                from_date = datetime.strftime(datetime.utcnow() - timedelta(days=days_back), "%Y-%m-%d")
                q += '&date_created.from=' + from_date
        except (ValueError, TypeError):
            # if any other value (e.g. a string) is provided, search all wfrs
            pass

    errored_wfrs = ff_utils.search_metadata(q, my_auth)
    # if a comma separated list of uuids is given, limit the result to them
    uuids = str(kwargs.get('limit_to_uuids'))
    if uuids:
        uuids = wfr_utils.string_to_list(uuids)
        errored_wfrs = [i for i in errored_wfrs if i['uuid'] in uuids]

    delete_categories = str(kwargs.get('delete_categories'))
    if delete_categories:
        delete_categories = wfr_utils.string_to_list(delete_categories)

    if not errored_wfrs:
        check.summary = 'All Good!'
        return check
    print(len(errored_wfrs))
    # report wfrs with error with warning
    check.status = 'WARN'
    # categorize errored runs based on the description keywords
    category_dictionary = {'NotEnoughSpace': 'Not enough space',
                           'Rerun': 'rerun',
                           'CheckLog': 'tibanna log --',
                           'EC2Idle': 'EC2 Idle',
                           'PatchError': 'Bad status code for PATCH',
                           'NotCategorized': ''  # to record all not categorized
                           }
    # counter for categories
    counter = {k: 0 for k in category_dictionary}
    # if a delete_category is not in category_dictionary, bail
    wrong_category = [i for i in delete_categories if i not in category_dictionary]
    if wrong_category:
        check.summary = 'Category was not found: {}'.format(wrong_category)
        return check

    for a_wfr in errored_wfrs:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        if len(wfr_type.strip().split(' ')) == 2:
            wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        else:
            wfr_type_base, wfr_version, tag = wfr_type.strip().split(' ')
        run_type = wfr_type_base.strip()
        # categorize
        desc = a_wfr.get('description', '')
        category = ''
        for a_key in category_dictionary:
            if category_dictionary[a_key] in desc:
                counter[a_key] += 1
                category = a_key
                break
        # all should be assigned to a category
        assert category
        # find all items to be deleted
        delete_list_uuid = wfr_utils.fetch_wfr_associated(a_wfr)

        info_pack = {'wfr_uuid': a_wfr['uuid'],
                     'wfr_type': run_type,
                     'wfr_run_status': a_wfr['run_status'],
                     'wfr_status': a_wfr['status'],
                     'wfr_description': a_wfr.get('description', '')[:50],
                     'category': category,
                     'items_to_delete': delete_list_uuid}
        action_category = ''
        # based on the category, place it in one of the lists in full output
        if category in delete_categories:
            action_category = 'To be deleted'
            check.full_output['cleanup'].append(info_pack)
        else:
            check.full_output['report_only'].append(info_pack)
            action_category = 'Only Reported'
        # add a short description for brief output
        check.brief_output.append("{}, {}, {}, {}".format(a_wfr['uuid'],
                                                          run_type,
                                                          category,
                                                          action_category
                                                          ))

    if check.full_output['cleanup']:
        check.allow_action = True

    report_catories = [i for i in category_dictionary if i not in delete_categories]
    check.summary = "{} wfrs ({}) will be deleted, and {} wfrs ({}) are reported".format(
        sum([counter[i] for i in delete_categories]),
        ",".join([i for i in delete_categories if counter[i]]),
        sum([counter[i] for i in report_catories]),
        ",".join([i for i in report_catories if counter[i]])
    )
    # add summary as the first item in brief output
    check.brief_output.insert(0, check.summary)
    return check


@action_function()
def problematic_wfrs_fdn_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'problematic_wfrs_fdn_start')
    my_auth = connection.ff_keys
    problematic_wfrs_fdn_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    deleted_wfrs = 0
    status_protected = 0
    for a_wfr in problematic_wfrs_fdn_check_result['cleanup']:
        # don't deleted if item is in protected statuses
        if a_wfr['wfr_status'] in PROTECTED_STATUSES:
            status_protected += 1
        else:
            deleted_wfrs += 1
            for an_item_to_delete in a_wfr['items_to_delete']:
                ff_utils.patch_metadata({'status': 'deleted'}, an_item_to_delete, my_auth)
    msg = '{} wfrs were removed'.format(str(deleted_wfrs))
    if status_protected:
        msg += ', {} wfrs were skipped due to protected item status.'.format(str(status_protected))
    action.output = msg
    action.status = 'DONE'
    return action


@check_function(action="compartments_caller_start")
def compartments_caller_status(connection, **kwargs):
    """Calls compartments on mcool files produced by the Hi-C pipeline"""

    check = CheckResult(connection, 'compartments_caller_status')
    my_auth = connection.ff_keys
    check.action = "compartments_caller_start"
    check.description = ""
    check.brief_output = []
    check.summary = ""
    check.full_output = {'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_types = ['in situ Hi-C', 'Dilution Hi-C', 'Micro-C', 'DNase Hi-C']
    feature = 'compartments'
    contact_type = 'cis'
    binsize = 250000
    # completion tag
    tag = wfr_utils.feature_calling_accepted_versions[feature][-1]
    # check indexing queue

    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check

    # Build the first query, experiments that have run the hic pipeline. add date and lab if available
    query = wfr_utils.build_feature_calling_query(exp_types, feature, kwargs)
    print(query)

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    print(len(res))

    if not res:
        check.summary = 'All Good!'
        return check

    for a_res in res:
        skip = False
        running = []
        completed = {'patch_opf': [], 'add_tag': []}
        missing_run = []
        problematic_run = []
        for pfile in a_res['processed_files']:
            if pfile['file_format']['display_title'] == 'mcool':
                file_meta = ff_utils.get_metadata(pfile['accession'], key=my_auth)
                qc_values = file_meta['quality_metric']['quality_metric_summary'][0]['value']
                problematic_resolutions = qc_values.split('; ')
                if str(binsize) in problematic_resolutions:
                    skip = True
                    continue

                # Skip problematic mcools files for now, until qc metrics for mcools are in place
                # if file_meta.get('tags'):
                #     if 'skip_domain_callers' in file_meta['tags']:
                #         skip = True
                #         continue
                workflow_status_report = wfr_utils.get_wfr_out(file_meta, "compartments-caller", key=my_auth)
                if skip:
                    continue
                elif workflow_status_report['status'] == 'running':
                    running.append(pfile['accession'])
                elif workflow_status_report['status'].startswith("no complete run, too many"):
                    problematic_run.append(['step1', a_res['accession'], pfile['accession']])
                elif workflow_status_report['status'] != 'complete':
                    organism = a_res['experiments_in_set'][0]['biosample']['biosource'][0]['organism']['name']
                    gc_content_file = wfr_utils.gc_content_ref.get(organism)
                    if not gc_content_file:
                        problematic_run.append(['step1', a_res['accession'], pfile['accession'], 'missing reference track'])
                    else:
                        overwrite = {'parameters': {"binsize": binsize, "contact_type": contact_type}}
                        inp_f = {'mcoolfile': pfile['accession'], "reference_track": gc_content_file}
                        missing_run.append(['step1', ['compartments-caller', organism, overwrite],
                                            inp_f, a_res['accession']])
                else:
                    patch_data = [workflow_status_report['bwfile']]
                    completed['patch_opf'].append([a_res['accession'], patch_data])
                    completed['add_tag'] = [a_res['accession'], tag]

        if running:
            check.full_output['running_runs'].append({a_res['accession']: running})
        if missing_run:
            check.full_output['needs_runs'].append({a_res['accession']: missing_run})
        if completed.get('add_tag'):
            assert not running
            assert not missing_run
            check.full_output['completed_runs'].append(completed)
        if problematic_run:
            check.full_output['problematic_runs'].append({a_res['accession']: problematic_run})

    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.allow_action = True
        check.status = 'WARN'
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.allow_action = True
        check.status = 'WARN'
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'

    return check


@action_function(start_runs=True, patch_completed=True)
def compartments_caller_start(connection, **kwargs):
    """Start compartments caller runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'compartments_caller_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    insu_and_boun_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = insu_and_boun_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = insu_and_boun_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=True, runtype='compartments', pc_append=True)
    return action


@check_function(lab_title=None, start_date=None, action="mcoolqc_start")
def mcoolqc_status(connection, **kwargs):
    """Searches for annotated bam files that do not have a qc object
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'mcoolqc_status')
    my_auth = connection.ff_keys
    check.action = "mcoolqc_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find mcool files)
    default_stati = 'released&status=uploaded&status=released+to+project'
    stati = 'status=' + (kwargs.get('status') or default_stati)
    query = 'search/?file_format.file_format=mcool&{}'.format(stati)
    query += '&type=FileProcessed'
    query += '&quality_metric.display_title=No+value'
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        query += '&lab.display_title=' + lab
    # The search
    print(query)
    res = ff_utils.search_metadata(query, key=my_auth)

    if not res:
        check.action_message = 'No action required at this moment'
        check.summary = 'All Good!'
        return check
    check.summary = '{} files need a mcoolqc'. format(len(res))
    check.status = 'WARN'
    check = wfr_utils.check_runs_without_output(res, check, 'mcoolQC', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def mcoolqc_start(connection, **kwargs):
    """Start mcoolqc runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'mcoolqc_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    mcoolqc_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(mcoolqc_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(mcoolqc_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)

        inp_f = {'mcoolfile': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('mcoolQC',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, connection.fs_env, mount=True
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


###################################
###################################
# TEMPLATES

# ##Template for qc type runs
@check_function(action="template_start")
def template_status(connection, **kwargs):
    """Searches for fastq files that don't have template"""
    start = datetime.utcnow()
    check = CheckResult(connection, 'template_status')
    my_auth = connection.ff_keys
    check.action = "template_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?type=File&my_own_field=my_value")
    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    if not res:
        check.summary = 'All Good!'
        return check
    check = wfr_utils.check_runs_without_output(res, check, 'template', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def template_start(connection, **kwargs):
    """Start template runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'template_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    template_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(template_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(template_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = wfr_utils.get_attribution(a_file)
        inp_f = {'input_fastq': a_file['@id']}
        wfr_setup = wfrset_utils.step_settings('template', 'no_organism', attributions)
        url = wfr_utils.run_missing_wfr(
            wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, connection.fs_env
        )
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action
