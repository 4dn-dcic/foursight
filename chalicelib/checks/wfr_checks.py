from datetime import datetime
from ..utils import (
    check_function,
    action_function,
)
from ..run_result import CheckResult, ActionResult
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
    check = CheckResult(connection, 'md5run_status_extra_file')
    my_auth = connection.ff_keys
    check.status = 'PASS'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query
    query = ('/search/?type=File&status!=uploading&status!=upload failed&status!=to be uploaded by workflow'
             '&extra_files.status!=uploaded&extra_files.href!=No value')
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
    # multiple failed runs
    problems = []
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

        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
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
    check = wfr_utils.check_runs_without_output(res, check, 'fastqc', my_auth, start)
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
                                        connection.ff_keys, connection.ff_env, mount=True)
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
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
             "&status!=uploading&status!=to be uploaded by workflow")
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
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
    accepted_types = ['LADs', 'boundaries', 'domain calls', 'peaks']

    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (find bg files without bw files)
    query = ("/search/?type=FileProcessed&file_format.file_format=bed"
             "&extra_files.file_format.display_title!=beddb"
             "&status!=uploading&status!=to be uploaded by workflow")
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
    query_f = ("/search/?type=FileProcessed&file_format.file_format=bed"
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
        check['full_output']['missing_assembly'] = missing
        msg = str(len(missing)) + ' files missing genome assembly'
        check['brief_output'].insert(0, msg)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bed2beddb_start(connection, **kwargs):
    """Start bed2beddb runs by sending compiled input_json to run_workflow endpoint"""
    # converter for workflow parameters
    genome = {"GRCh38": "hg38",
              "GRCm38": "mm10",
              "dm6": 'dm6',
              "galGal5": "galGal5"}
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
        inp_f = {'bedfile': a_file['@id']}
        override = {'parameters': {'assembly': genome[a_file['genome_assembly']]}}
        wfr_setup = wfrset_utils.step_settings('bedtobeddb',
                                               'no_organism',
                                               attributions,
                                               overwrite=override)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
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
    hic_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = hic_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = hic_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start,
                                   move_to_pc=True,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None)
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start,
                                   move_to_pc=True,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None)
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    print(check_result)
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start,
                                   move_to_pc=False,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None)
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start,
                                   move_to_pc=False,  runtype='repliseq')
    return action


@check_function(lab_title=None, start_date=None)
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
def chip_seq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'chip_seq_status')
    my_auth = connection.ff_keys
    check.action = "chip_seq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = ['All Good!']
    check.summary = "All Good!"
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'ChIP-seq'
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    return check


@action_function(start_runs=True, patch_completed=True)
def chip_seq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'chip_seq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False)
    return action


@check_function(lab_title=None, start_date=None)
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
    margi_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = margi_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = margi_check_result.get('completed_runs')

    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True, runtype='margi')
    return action


@check_function(lab_title=None, start_date=None)
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
             "&status!=uploading&status!=to be uploaded by workflow")
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
def rna_strandedness_status(connection, **kwargs):
    """Searches for fastq files of experiment seq type that don't have beta_actin_count fields
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
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (RNA-seq experiments)
    query = '/search/?experiment_type.display_title=RNA-seq&type=ExperimentSeq&status=pre-release&status=released&status=released to project'

    # The search
    res = ff_utils.search_metadata(query, key=my_auth)
    targets = []
    for re in res:
        for a_re_file in re['files']:
            if a_re_file['file_format']['display_title'] == 'fastq':
                file_meta = ff_utils.get_metadata(a_re_file['accession'], key=my_auth)
                file_meta_keys = file_meta.keys()
                if 'beta_actin_sense_count' not in file_meta_keys and 'beta_actin_antisense_count' not in file_meta_keys:
                    targets.append(file_meta)
    if not targets:
        check.summary = "All good!"
        return check

    running = []
    missing_run = []

    for a_file in targets:
        strandedness_report = wfr_utils.get_wfr_out(a_file, "rna-strandedness", key=my_auth, versions='v2', md_qc=True)
        if strandedness_report['status'] == 'running':
            running.append(a_file['accession'])
        elif strandedness_report['status'] != 'complete':
            missing_run.append(a_file['accession'])

    if running:
        check.summary = 'Some files are running rna_strandedness run'
        msg = str(len(running)) + ' files are still running rna_strandedness run.'
        check.brief_output.append(msg)
        check.full_output['files_running_rna_strandedness_run'] = running

    if missing_run:
        check.summary = 'Some files are missing rna_strandedness run'
        msg = str(len(missing_run)) + ' file(s) lack a successful rna_strandedness run'
        check.brief_output.append(msg)
        check.full_output['files_without_rna_strandedness_run'] = missing_run
        check.allow_action = True
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
        org = a_file['experiments'][0]['biosample']['biosource'][0]['individual']['organism']['name']
        kmer_file = wfr_utils.re_kmer[org]
        # Add function to calculate resolution automatically
        inp_f = {'fastq': a_file['@id'], 'ACTB_reference': kmer_file}
        wfr_setup = wfrset_utils.step_settings('rna-strandedness',
                                               'no_organism',
                                               attributions)
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None, query='')
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
    check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=True, runtype='rnaseq')
    return action


@check_function(lab_title=None, start_date=None)
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(lab_title=None, start_date=None)
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

    query = '/search/?status=uploaded&status=pre-release&status=released+to+project&status=released&type=FileFastq&file_first_line=No value&status=restricted'

    # The search
    print('About to query ES for files')
    res = ff_utils.search_metadata(query, key=my_auth)
    targets = []

    print('About to check metadata field for each result in the search')
    for re in res:
        if not re.get('file_first_line'):
            targets.append(re)

    if not targets:
        check.summary = "All good!"
        return check

    running = []
    missing_run = []

    print('About to check for workflow runs for each file')
    for a_file in targets:
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function()
def bam_re_status(connection, **kwargs):
    """Searches for fastq files that don't have bam_re"""
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
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query (skip to be uploaded by workflow)
    exp_types = ['in+situ+Hi-C',
                 'Dilution+Hi-C',
                 'Capture+Hi-C',
                 'TCC',
                 # 'in+situ+ChIA-PET',
                 'PLAC-seq']
    query = "/search/?file_format.file_format=bam&file_type=alignments&type=FileProcessed&status!=uploading"
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
                          "MspI", "NcoI_MspI_BspHI"  # added patterns in action
                          ]
    # make a new list of files to work on
    filtered_res = []
    # make a list of skipped files
    missing_nz_files = []
    # make a list of skipped enzymes
    missing_nz = []
    for a_file in res:
        # expecting all files to have an experiment
        nz = a_file.get('experiments')[0].get('digestion_enzyme', {}).get('name')
        if nz in acceptable_enzymes:
            filtered_res.append(a_file)
        else:
            missing_nz_files.append(a_file)
            if nz not in missing_nz:
                missing_nz.append(nz)

    check = wfr_utils.check_runs_without_output(filtered_res, check, 're_checker_workflow', my_auth, start)
    if missing_nz:
        skipped_files = str(len(res) - len(filtered_res))
        nzs = ', '.join(missing_nz)
        message = 'INFO: skipping files ({}) using {}'.format(skipped_files, nzs)
        check.summary += ', ' + message
        check.brief_output.insert(0, message)
        check.full_output['skipped'] = [i['accession'] for i in missing_nz_files]
        check.status = 'WARN'
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
    nz_patterns = {"MspI": {"motif": {"regex": "CCGCGG|GGCGCC"}},
                   "NcoI_MspI_BspHI": {"motif": {"regex": ("CCATGCATGG|CCATGCATGA|CCATGCGG|TCATGCATGG|TCATGCATGA|TCATGCGG|"
                                                           "CCGCATGG|CCGCATGA|CCGCGG|GGTACGTACC|AGTACGTACC|GGCGTACC|GGTACGTACT|"
                                                           "AGTACGTACT|GGCGTACT|GGTACGCC|AGTACGCC|GGCGCC")}}}
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
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
@check_function()
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
        url = wfr_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action
