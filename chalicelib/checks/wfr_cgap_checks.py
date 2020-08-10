from datetime import datetime
from ..utils import (
    check_function,
    action_function,
)
from ..run_result import CheckResult, ActionResult
from dcicutils import ff_utils, s3Utils
from .helpers import cgap_utils, wfrset_cgap_utils
lambda_limit = cgap_utils.lambda_limit

# list of acceptible version
cgap_partI_version = ['WGS_partI_V11', 'WGS_partI_V12', 'WGS_partI_V13']
cgap_partII_version = ['WGS_PartII_V11', 'WGS_PartII_V13', 'WGS_PartII_V15']
cgap_partIII_version = ['WGS_PartIII_V15']


@check_function(file_type='File', start_date=None)
def md5runCGAP_status(connection, **kwargs):
    """Searches for files that are uploaded to s3, but not went though md5 run.
    This check makes certain assumptions
    -all files that have a status<= uploaded, went through md5runCGAP
    -all files status uploading/upload failed, and no s3 file are pending,
    and skipped by this check.
    if you change status manually, it might fail to show up in this checkself.
    Keyword arguments:
    file_type -- limit search to a file type, i.e. FileFastq (default=File)
    start_date -- limit search to files generated since date  YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'md5runCGAP_status')
    my_auth = connection.ff_keys
    check.action = "md5runCGAP_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
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
        # elif 'FileVistrack' in a_file['@type']:
        #         my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                my_bucket = raw_bucket
        # check if file is in s3
        file_id = a_file['accession']
        head_info = my_s3_util.does_key_exist(a_file['upload_key'], my_bucket)
        if not head_info:
            no_s3_file.append(file_id)
            continue
        md5_report = cgap_utils.get_wfr_out(a_file, "md5", key=my_auth, md_qc=True)
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
        check.summary = 'Some files are running md5runCGAP'
        msg = str(len(running)) + ' files are still running md5runCGAP.'
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
def md5runCGAP_start(connection, **kwargs):
    """Start md5 runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'md5runCGAP_start')
    action_logs = {'runs_started': [], "runs_failed": []}
    my_auth = connection.ff_keys
    md5runCGAP_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    action_logs['check_output'] = md5runCGAP_check_result
    targets = []
    if kwargs.get('start_missing'):
        targets.extend(md5runCGAP_check_result.get('files_without_md5run', []))
    if kwargs.get('start_not_switched'):
        targets.extend(md5runCGAP_check_result.get('files_with_run_and_wrong_status', []))
    action_logs['targets'] = targets
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = cgap_utils.get_attribution(a_file)
        inp_f = {'input_file': a_file['@id']}
        wfr_setup = wfrset_cgap_utils.step_settings('md5', 'no_organism', attributions)

        url = cgap_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(start_date=None)
def fastqcCGAP_status(connection, **kwargs):
    """Searches for fastq files that don't have fastqcCGAP
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'fastqcCGAP_status')
    my_auth = connection.ff_keys
    check.action = "fastqcCGAP_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?type=File&file_format.file_format=fastq&&quality_metric.uuid=No+value"
             "&status=pre-release&status=released&status=released%20to%20project&status=uploaded")
    # fastqcCGAP not properly reporting for long reads
    skip_instruments = ['PromethION', 'GridION', 'MinION', 'PacBio RS II']
    skip_add = "".join(['&instrument!=' + i for i in skip_instruments])
    query += skip_add

    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # The search
    results = ff_utils.search_metadata(query, key=my_auth)
    res = []
    # check if the qc_metric is in the file
    for a_file in results:
        qc_metric = cgap_utils.is_there_my_qc_metric(a_file, 'QualityMetricFastqc', my_auth)
        if not qc_metric:
            res.append(a_file)

    if not res:
        check.summary = 'All Good!'
        return check
    check = cgap_utils.check_runs_without_output(res, check, 'fastqc', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def fastqcCGAP_start(connection, **kwargs):
    """Start fastqcCGAP runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'fastqcCGAP_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    fastqcCGAP_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(fastqcCGAP_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(fastqcCGAP_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = cgap_utils.get_attribution(a_file)
        inp_f = {'input_fastq': a_file['@id']}
        wfr_setup = wfrset_cgap_utils.step_settings('fastqc', 'no_organism', attributions)
        url = cgap_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env, mount=True)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(start_date=None)
def cgap_status(connection, **kwargs):
    """
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'cgap_status')
    my_auth = connection.ff_keys
    check.action = "cgap_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query_base = '/search/?type=Case&sample.files.display_title%21=No+value'
    version_filter = "".join(["&sample.completed_processes!=" + i for i in cgap_partI_version])
    q = query_base + version_filter

    all_cases = ff_utils.search_metadata(q, my_auth)
    print(len(all_cases))

    step1_name = 'workflow_bwa-mem_no_unzip-check'
    step2_name = 'workflow_add-readgroups-check'
    step3_name = 'workflow_merge-bam-check'
    step4_name = 'workflow_picard-MarkDuplicates-check'
    step5_name = 'workflow_sort-bam-check'
    step6_name = 'workflow_gatk-BaseRecalibrator'
    step7_name = 'workflow_gatk-ApplyBQSR-check'
    step8_name = 'workflow_granite-mpileupCounts'
    step9_name = 'workflow_gatk-HaplotypeCaller'
    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)
    for a_case in all_cases:
        all_items, all_uuids = ff_utils.expand_es_metadata([a_case['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version',
                                                                         'experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        a_sample = [i for i in all_items['sample'] if i['uuid'] == a_case['sample']['uuid']][0]
        now = datetime.utcnow()
        print(a_case['accession'], a_sample['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break
        # collect similar types of items under library
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}

        # check for workflow version problems
        all_collected_wfs = all_items.get('workflow')
        all_app_names = [i['app_name'] for i in all_collected_wfs]
        all_wfs = [i for i in all_system_wfs if i['app_name'] in all_app_names]
        wf_errs = cgap_utils.check_workflow_version(all_wfs)
        # if there are problems kill the loop, and report the error
        if wf_errs:
            final_status = a_case['accession'] + ' error, workflow versions'
            check.brief_output.extend(wf_errs)
            check.full_output['problematic_runs'].append({a_sample['accession']: wf_errs})
            break

        # are all files uploaded ?
        all_uploaded = True
        # get all fastq files (can be file_fastq or file_processed)
        fastq_files = [i for i in all_files if i.get('file_format', {}).get('file_format') == 'fastq']
        for a_file in fastq_files:
            if a_file['status'] in ['uploading', 'upload failed']:
                all_uploaded = False
        if not all_uploaded:
            final_status = a_sample['accession'] + ' skipped, waiting for file upload'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({a_sample['accession']: 'files status uploading'})
            continue

        sample_raw_files, refs = cgap_utils.find_fastq_info(a_sample, fastq_files)
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}
        s3_input_bams = []
        stop_level_2 = False

        # check if we need to run mpileupCounts
        # is this sample meant for part 3 (if sample_processing is set to WGS-Joint Calling, skip it)
        will_go_to_part_3 = False
        exclude_for_mpilup = ['WGS', 'WGS-Joint Calling']
        analysis_type = a_case.get('sample_processing', {}).get('analysis_type', '')
        if not analysis_type:
            pass
        elif analysis_type in exclude_for_mpilup:
            pass
        else:
            will_go_to_part_3 = True

        for pair in sample_raw_files:
            # RUN STEP 1
            s1_input_files = {'fastq_R1': pair[0], 'fastq_R2': pair[1], 'reference': refs['bwa_ref']}
            s1_tag = a_sample['accession'] + '_' + pair[0].split('/')[2] + '_' + pair[1].split('/')[2]
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  'step1', s1_tag, pair,
                                                                  s1_input_files,  step1_name, 'raw_bam')
            # RUN STEP 2
            if step1_status != 'complete':
                step2_status = ''
                stop_level_2 = True
            else:
                s2_input_files = {'input_bam': step1_output}
                s2_tag = a_sample['accession'] + '_' + step1_output.split('/')[2]
                add_par = {"parameters": {"sample_name": a_sample['aliases'][0].split(':')[1]}}
                keep, step2_status, step2_output = cgap_utils.stepper(library, keep,
                                                                      'step2', s2_tag, step1_output,
                                                                      s2_input_files,  step2_name, 'bam_w_readgroups',
                                                                      add_par)
            if step2_status != 'complete':
                stop_level_2 = True
            else:
                s3_input_bams.append(step2_output)
        # RUN STEP 3
        if stop_level_2:
            step3_status = ""
        else:
            # if there is a single bam, skip step 4
            if len(s3_input_bams) == 1:
                step3_status = 'complete'
                step3_output = s3_input_bams[0]
            else:
                s3_input_files = {'input_bams': s3_input_bams}
                keep, step3_status, step3_output = cgap_utils.stepper(library, keep,
                                                                      'step3', a_sample['accession'], s3_input_bams,
                                                                      s3_input_files,  step3_name, 'merged_bam')
        # RUN STEP 4
        if step3_status != 'complete':
            step4_status = ""
        else:
            s4_input_files = {'input_bam': step3_output}
            keep, step4_status, step4_output = cgap_utils.stepper(library, keep,
                                                                  'step4', a_sample['accession'], step3_output,
                                                                  s4_input_files,  step4_name, 'dupmarked_bam')
        # RUN STEP 5
        if step4_status != 'complete':
            step5_status = ""
        else:
            s5_input_files = {'input_bam': step4_output}
            keep, step5_status, step5_output = cgap_utils.stepper(library, keep,
                                                                  'step5', a_sample['accession'], step4_output,
                                                                  s5_input_files,  step5_name, 'sorted_bam')
        # RUN STEP 6
        if step5_status != 'complete':
            step6_status = ""
        else:
            s6_input_files = {'input_bam': step5_output, 'known-sites-snp': 'GAPFI4LJRN98',
                              'known-sites-indels': 'GAPFIAX2PPYB', 'reference': 'GAPFIXRDPDK5'}
            keep, step6_status, step6_output = cgap_utils.stepper(library, keep,
                                                                  'step6', a_sample['accession'], step5_output,
                                                                  s6_input_files,  step6_name, 'recalibration_report')
        # RUN STEP 7
        if step6_status != 'complete':
            step7_status = ""
        else:
            s7_input_files = {'input_bam': step5_output,
                              'reference': 'GAPFIXRDPDK5',
                              'recalibration_report': step6_output}
            keep, step7_status, step7_output = cgap_utils.stepper(library, keep,
                                                                  'step7', a_sample['accession'], step6_output,
                                                                  s7_input_files,  step7_name, 'recalibrated_bam')
        # RUN STEP 8 - only run if will_go_to_part_3 is True
        if will_go_to_part_3:
            if step7_status != 'complete':
                step8_status = ""
            else:
                s8_input_files = {'input_bam': step7_output,
                                  'regions': '1c07a3aa-e2a3-498c-b838-15991c4a2f28',
                                  'reference': '1936f246-22e1-45dc-bb5c-9cfd55537fe7'}
                keep, step8_status, step8_output = cgap_utils.stepper(library, keep,
                                                                      'step8', a_sample['accession'], step7_output,
                                                                      s8_input_files,  step8_name, 'rck')
        else:
            step8_status = 'complete'
            step8_output = ''

        # RUN STEP 9 # run together with step8
        if step7_status != 'complete':
            step9_status = ""
        else:
            s9_input_files = {'input_bam': step7_output,
                              'regions': '1c07a3aa-e2a3-498c-b838-15991c4a2f28',
                              'reference': '1936f246-22e1-45dc-bb5c-9cfd55537fe7'}
            keep, step9_status, step9_output = cgap_utils.stepper(library, keep,
                                                                  'step9', a_sample['accession'], step7_output,
                                                                  s9_input_files,  step9_name, 'gvcf')

        # are all runs done
        all_runs_completed = False
        if step8_status == 'complete' and step9_status == 'complete':
            all_runs_completed = True

        final_status = a_sample['accession']
        completed = []
        pipeline_tag = cgap_partI_version[-1]
        previous_tags = a_sample.get('completed_processes', [])

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if all_runs_completed:
            final_status += ' completed'
            completed = [
                a_sample['accession'],
                {'processed_files': [step7_output, step9_output],
                 'completed_processes': previous_tags + [pipeline_tag]}
                         ]
            if will_go_to_part_3:
                completed[1]['processed_files'].append(step8_output)
            print('COMPLETED', step9_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])
            if problematic_run:
                final_status += ' |Problem: ' + " ".join([i[0] for i in problematic_run])

        # add dictionaries to main ones
        set_acc = a_sample['accession']
        check.brief_output.append(final_status)
        print(final_status)
        if running:
            check.full_output['running_runs'].append({set_acc: running})
        if missing_run:
            check.full_output['needs_runs'].append({set_acc: missing_run})
        if problematic_run:
            check.full_output['problematic_runs'].append({set_acc: problematic_run})
        # if made it till the end
        if completed:
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(completed)

    # complete check values
    check.summary = ""
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['skipped']:
        check.summary += str(len(check.full_output['skipped'])) + ' skipped|'
        check.status = 'WARN'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'
    return check


@action_function(start_runs=True, patch_completed=True)
def cgap_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'cgap_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    cgap_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = cgap_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = cgap_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action


@check_function(start_date=None)
def cgapS2_status(connection, **kwargs):
    """
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'cgapS2_status')
    my_auth = connection.ff_keys
    check.action = "cgapS2_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query_base = '/search/?type=SampleProcessing&samples.uuid!=No value'
    version_filter = "".join(["&completed_processes!=" + i for i in cgap_partII_version])
    q = query_base + version_filter
    print(q)
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        return check
    # list step names
    step1_name = 'workflow_gatk-CombineGVCFs'
    step2_name = 'workflow_gatk-GenotypeGVCFs-check'
    # step3_name = 'workflow_gatk-VQSR-check'

    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)

    # iterate over msa
    print(len(res))
    for an_msa in res:
        all_items, all_uuids = ff_utils.expand_es_metadata([an_msa['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version',
                                                                         'experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        now = datetime.utcnow()
        print(an_msa['@id'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}

        # check for workflow version problems
        all_collected_wfs = all_items.get('workflow')
        all_app_names = [i['app_name'] for i in all_collected_wfs]
        all_wfs = [i for i in all_system_wfs if i['app_name'] in all_app_names]
        wf_errs = cgap_utils.check_workflow_version(all_wfs)
        # if there are problems kill the loop, and report the error
        if wf_errs:
            final_status = an_msa['@id'] + ' error, workflow versions'
            check.brief_output.extend(wf_errs)
            check.full_output['problematic_runs'].append({an_msa['@id']: wf_errs})
            break

        # if there are multiple samples merge them
        # if not skip step 1
        input_samples = an_msa['samples']
        input_vcfs = []
        # check all samples and collect input files
        samples_ready = True
        for a_sample in input_samples:
            sample_resp = [i for i in all_items['sample'] if i['uuid'] == a_sample['uuid']][0]
            comp_tags = sample_resp.get('completed_processes', [])
            # did sample complete upstream processing
            if not set(comp_tags) & set(cgap_partI_version):
                samples_ready = False
                break
            vcf = [i for i in sample_resp['processed_files'] if i['display_title'].endswith('gvcf.gz')][0]['@id']
            input_vcfs.append(vcf)

        if not samples_ready:
            final_status = an_msa['@id'] + ' waiting for upstream part'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({an_msa['@id']: 'missing upstream part'})
            continue

        # if multiple sample, merge vcfs, if not skip it
        if len(input_samples) > 1:
            s1_input_files = {'input_gvcfs': input_vcfs,
                              'chromosomes': 'a1d504ee-a313-4064-b6ae-65fed9738980',
                              'reference': '1936f246-22e1-45dc-bb5c-9cfd55537fe7'}
            # benchmarking
            if len(input_samples) < 4:
                ebs_size = '10x'
            else:
                ebs_size = str(10 + len(input_samples) - 3) + 'x'
            update_pars = {"config": {"ebs_size": ebs_size}}
            s1_tag = an_msa['@id'] + '_S2run1' + input_vcfs[0].split('/')[2]
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  'step1', s1_tag, input_vcfs,
                                                                  s1_input_files,  step1_name, 'combined_gvcf',
                                                                  additional_input=update_pars)
        else:
            step1_status = 'complete'
            step1_output = input_vcfs[0]

        if step1_status != 'complete':
            step2_status = ""
        else:
            # run step2
            s2_input_files = {'input_gvcf': step1_output,
                              "reference": "1936f246-22e1-45dc-bb5c-9cfd55537fe7",
                              "known-sites-snp": "8ed35691-0af4-467a-adbc-81eb088549f0",
                              'chromosomes': 'a1d504ee-a313-4064-b6ae-65fed9738980'}
            s2_tag = an_msa['@id'] + '_S2run2' + step1_output.split('/')[2]
            keep, step2_status, step2_output = cgap_utils.stepper(library, keep,
                                                                  'step2', s2_tag, step1_output,
                                                                  s2_input_files,  step2_name, 'vcf')

        final_status = an_msa['@id']
        completed = []
        pipeline_tag = cgap_partII_version[-1]
        previous_tags = an_msa.get('completed_processes', [])

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if step2_status == 'complete':
            final_status += ' completed'
            # existing_pf = [i['@id'] for i in an_msa['processed_files']]
            completed = [
                an_msa['@id'],
                {'processed_files': [step2_output],
                 'completed_processes': previous_tags + [pipeline_tag, ]}]
            print('COMPLETED', step2_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])

        # add dictionaries to main ones
        set_acc = an_msa['@id']
        check.brief_output.append(final_status)
        if running:
            check.full_output['running_runs'].append({set_acc: running})
        if missing_run:
            check.full_output['needs_runs'].append({set_acc: missing_run})
        if problematic_run:
            check.full_output['problematic_runs'].append({set_acc: problematic_run})
        # if made it till the end
        if completed:
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(completed)

    # complete check values
    check.summary = ""
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['skipped']:
        check.summary += str(len(check.full_output['skipped'])) + ' skipped|'
        check.status = 'WARN'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'
    return check


@action_function(start_runs=True, patch_completed=True)
def cgapS2_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'cgapS2_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    cgapS2_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = cgapS2_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = cgapS2_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action


@check_function(start_date=None)
def cgapS3_status(connection, **kwargs):
    """
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'cgapS3_status')
    my_auth = connection.ff_keys
    check.action = "cgapS3_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query_base = '/search/?type=SampleProcessing&samples.uuid!=No value&completed_processes!=No value&processed_files.uuid!=No value'
    version_filter = "".join(["&completed_processes!=" + i for i in cgap_partIII_version])
    q = query_base + version_filter
    print(q)
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        return check
    # list step names
    step1a_name = 'workflow_granite-rckTar'
    step1b_name = 'workflow_mutanno-micro-annot-check'
    step2_name = 'workflow_granite-filtering-check'
    step3_name = 'workflow_granite-novoCaller-rck-check'
    step4_name = 'workflow_granite-comHet-check'
    step5_name = 'workflow_mutanno-annot-check'
    # step3_name = 'workflow_gatk-VQSR-check'

    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)

    # iterate over msa
    print(len(res))
    for an_msa in res:
        all_items, all_uuids = ff_utils.expand_es_metadata([an_msa['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version',
                                                                         'experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        now = datetime.utcnow()
        print(an_msa['@id'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}

        # check for workflow version problems
        all_collected_wfs = all_items.get('workflow')
        all_app_names = [i['app_name'] for i in all_collected_wfs]
        all_wfs = [i for i in all_system_wfs if i['app_name'] in all_app_names]
        wf_errs = cgap_utils.check_workflow_version(all_wfs)
        # if there are problems kill the loop, and report the error
        if wf_errs:
            final_status = an_msa['@id'] + ' error, workflow versions'
            check.brief_output.extend(wf_errs)
            check.full_output['problematic_runs'].append({an_msa['@id']: wf_errs})
            break

        # only run for trios
        # # TODO: also look for analysis type
        input_samples = an_msa['samples']
        if len(input_samples) != 3:
            final_status = an_msa['@id'] + ' is not trio'
            check.brief_output.extend(wf_errs)
            check.full_output['problematic_runs'].append({an_msa['@id']: 'is not trio'})
            break

        # Setup for step 1a
        input_rcks = []
        # check all samples and collect input files
        for a_sample in input_samples:
            rck = ''
            sample_resp = [i for i in all_items['sample'] if i['uuid'] == a_sample['uuid']][0]
            try:
                rck = [i for i in sample_resp['processed_files'] if i['display_title'].endswith('rck.gz')][0]['@id']
            except:
                continue
            input_rcks.append(rck)

        if len(input_rcks) != len(input_samples):
            final_status = an_msa['@id'] + ' missing rck files on samples'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({an_msa['@id']: 'missing rck files on samples'})
            continue

        # if multiple sample, merge vcfs, if not skip it
        if len(input_samples) > 1:
            s1_input_files = {'input_gvcfs': input_vcfs,
                              'chromosomes': 'a1d504ee-a313-4064-b6ae-65fed9738980',
                              'reference': '1936f246-22e1-45dc-bb5c-9cfd55537fe7'}
            # benchmarking
            if len(input_samples) < 4:
                ebs_size = '10x'
            else:
                ebs_size = str(10 + len(input_samples) - 3) + 'x'
            update_pars = {"config": {"ebs_size": ebs_size}}
            s1_tag = an_msa['@id'] + '_S2run1' + input_vcfs[0].split('/')[2]
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  'step1', s1_tag, input_vcfs,
                                                                  s1_input_files,  step1_name, 'combined_gvcf',
                                                                  additional_input=update_pars)
        else:
            step1_status = 'complete'
            step1_output = input_vcfs[0]

        if step1_status != 'complete':
            step2_status = ""
        else:
            # run step2
            s2_input_files = {'input_gvcf': step1_output,
                              "reference": "1936f246-22e1-45dc-bb5c-9cfd55537fe7",
                              "known-sites-snp": "8ed35691-0af4-467a-adbc-81eb088549f0",
                              'chromosomes': 'a1d504ee-a313-4064-b6ae-65fed9738980'}
            s2_tag = an_msa['@id'] + '_S2run2' + step1_output.split('/')[2]
            keep, step2_status, step2_output = cgap_utils.stepper(library, keep,
                                                                  'step2', s2_tag, step1_output,
                                                                  s2_input_files,  step2_name, 'vcf')

        final_status = an_msa['@id']
        completed = []
        pipeline_tag = cgap_partII_version[-1]
        previous_tags = an_msa.get('completed_processes', [])

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if step2_status == 'complete':
            final_status += ' completed'
            # existing_pf = [i['@id'] for i in an_msa['processed_files']]
            completed = [
                an_msa['@id'],
                {'processed_files': [step2_output],
                 'completed_processes': previous_tags + [pipeline_tag, ]}]
            print('COMPLETED', step2_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])

        # add dictionaries to main ones
        set_acc = an_msa['@id']
        check.brief_output.append(final_status)
        if running:
            check.full_output['running_runs'].append({set_acc: running})
        if missing_run:
            check.full_output['needs_runs'].append({set_acc: missing_run})
        if problematic_run:
            check.full_output['problematic_runs'].append({set_acc: problematic_run})
        # if made it till the end
        if completed:
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(completed)

    # complete check values
    check.summary = ""
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['skipped']:
        check.summary += str(len(check.full_output['skipped'])) + ' skipped|'
        check.status = 'WARN'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'
    return check


@action_function(start_runs=True, patch_completed=True)
def cgapS3_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'cgapS3_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    cgapS3_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = cgapS3_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = cgapS3_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action


@check_function()
def bamqcCGAP_status(connection, **kwargs):
    """Searches for bam files that don't have bamqcCGAP
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead (default=24 hours)
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'bamqcCGAP_status')
    my_auth = connection.ff_keys
    check.action = "bamqcCGAP_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    # Build the query (skip to be uploaded by workflow)
    query = ("/search/?type=Sample&status%21=deleted&processed_files.uuid%21=No+value")
    results = ff_utils.search_metadata(query, key=my_auth)
    # List of bam files
    bam_list = []
    for a_sample in results:
        for a_file in a_sample['processed_files']:
            bam_uuid = a_file['uuid']
            bam_list.append(bam_uuid)

    res = []
    # check if the qc_metric is in the file
    for a_file in bam_list:
        results = ff_utils.get_metadata(a_file, key=my_auth)
        qc_metric = cgap_utils.is_there_my_qc_metric(results, 'QualityMetricWgsBamqc', my_auth)
        if not qc_metric:
            res.append(results)

    if not res:
        check.summary = 'All Good!'
        return check

    check = cgap_utils.check_runs_without_output(res, check, 'workflow_qcboard-bam', my_auth, start)
    return check


@action_function(start_missing_run=True, start_missing_meta=True)
def bamqcCGAP_start(connection, **kwargs):
    """Start bamqcCGAP runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'bamqcCGAP_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    bamqcCGAP_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = []
    if kwargs.get('start_missing_run'):
        targets.extend(bamqcCGAP_check_result.get('files_without_run', []))
    if kwargs.get('start_missing_meta'):
        targets.extend(bamqcCGAP_check_result.get('files_without_changes', []))
    for a_target in targets:
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            action.description = 'Did not complete action due to time limitations'
            break
        a_file = ff_utils.get_metadata(a_target, key=my_auth)
        attributions = cgap_utils.get_attribution(a_file)
        inp_f = {'input_files': a_file['@id']}
        wfr_setup = wfrset_cgap_utils.step_settings('workflow_qcboard-bam', 'no_organism', attributions)
        url = cgap_utils.run_missing_wfr(wfr_setup, inp_f, a_file['accession'], connection.ff_keys, connection.ff_env)
        # aws run url
        if url.startswith('http'):
            action_logs['runs_started'].append(url)
        else:
            action_logs['runs_failed'].append([a_target, url])
    action.output = action_logs
    action.status = 'DONE'
    return action


@check_function(start_date=None)
def cram_status(connection, **kwargs):
    """
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'cram_status')
    my_auth = connection.ff_keys
    check.action = "cram_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    q = '/search/?type=Sample&files.display_title=No+value&cram_files.display_title%21=No+value'
    all_samples = ff_utils.search_metadata(q, my_auth)
    print(len(all_samples))

    for a_sample in all_samples:
        all_items, all_uuids = ff_utils.expand_es_metadata([a_sample['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        now = datetime.utcnow()
        print(a_sample['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break
        # are all files uploaded ?
        all_uploaded = True
        cram_files = [i for i in all_items['file_processed'] if i['file_format']['file_format'] == 'CRAM']
        print(len(cram_files))
        for a_file in cram_files:
            if a_file['status'] in ['uploading', 'upload failed']:
                all_uploaded = False

        if not all_uploaded:
            final_status = a_sample['accession'] + ' skipped, waiting for file upload'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({a_sample['accession']: 'files status uploading'})
            continue

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}

        keep = {'missing_run': [], 'running': [], 'problematic_run': []}
        result_fastq_files = []
        all_done = True
        for a_cram in cram_files:
            # RUN STEP 1
            input_files = {'cram': a_cram['@id'],
                           'reference_fasta': '/files-reference/GAPFIXRDPDK5/',
                           'reference_md5_list': '/files-reference/GAPFIGWSGHNU/'}
            tag = a_sample['accession'] + '_' + a_cram['@id']
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  'cram2fastq', tag, a_cram['@id'],
                                                                  input_files,  'workflow_cram2fastq',
                                                                  ['fastq1', 'fastq2'])
            # RUN STEP 2
            if step1_status != 'complete':
                all_done = False
            else:
                result_fastq_files.extend(step1_output)

        final_status = a_sample['accession']  # start the reporting with acc
        completed = []
        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if all_done:
            final_status += ' completed'
            completed = [a_sample['accession'], {'files': result_fastq_files}]
            print('COMPLETED', result_fastq_files)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])
            if problematic_run:
                final_status += ' |Problem: ' + " ".join([i[0] for i in problematic_run])

        # add dictionaries to main ones
        set_acc = a_sample['accession']
        check.brief_output.append(final_status)
        print(final_status)
        if running:
            check.full_output['running_runs'].append({set_acc: running})
        if missing_run:
            check.full_output['needs_runs'].append({set_acc: missing_run})
        if problematic_run:
            check.full_output['problematic_runs'].append({set_acc: problematic_run})
        # if made it till the end
        if completed:
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(completed)

    # complete check values
    check.summary = ""
    if check.full_output['running_runs']:
        check.summary = str(len(check.full_output['running_runs'])) + ' running|'
    if check.full_output['skipped']:
        check.summary += str(len(check.full_output['skipped'])) + ' skipped|'
        check.status = 'WARN'
    if check.full_output['needs_runs']:
        check.summary += str(len(check.full_output['needs_runs'])) + ' missing|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['completed_runs']:
        check.summary += str(len(check.full_output['completed_runs'])) + ' completed|'
        check.status = 'WARN'
        check.allow_action = True
    if check.full_output['problematic_runs']:
        check.summary += str(len(check.full_output['problematic_runs'])) + ' problem|'
        check.status = 'WARN'
    return check


@action_function(start_runs=True, patch_completed=True)
def cram_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'cram_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    cram_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = cram_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = cram_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action
