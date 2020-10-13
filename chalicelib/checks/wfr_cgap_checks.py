import json
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
cgap_partI_version = ['WGS_partI_V11', 'WGS_partI_V12', 'WGS_partI_V13', 'WGS_partI_V15', 'WGS_partI_V16', 'WGS_partI_V17']
cgap_partII_version = ['WGS_partII_V11', 'WGS_partII_V13', 'WGS_partII_V15', 'WGS_partII_V16', 'WGS_partII_V17']
cgap_partIII_version = ['WGS_partIII_V15', 'WGS_partIII_V16', 'WGS_partIII_V17']


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
        inp_f = {'input_file': a_file['@id'],
                 'additional_file_parameters': {'input_file': {'mount': True}}}
        wfr_setup = wfrset_cgap_utils.step_settings('md5', 'no_organism', attributions)

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
        inp_f = {'input_fastq': a_file['@id'],
                 'additional_file_parameters': {'input_fastq': {'mount': True}}}
        wfr_setup = wfrset_cgap_utils.step_settings('fastqc', 'no_organism', attributions)
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

    query_base = '/search/?type=Case&sample.workup_type=WGS&sample.workup_type=WES'
    file_filter = '&sample.files.display_title%21=No+value&sample.files.status%21=uploading&sample.files.status%21=upload failed'
    version_filter = "".join(["&sample.completed_processes!=" + i for i in cgap_partI_version])
    q = query_base + version_filter + file_filter
    all_cases = ff_utils.search_metadata(q, my_auth)
    # sometimes the same sample is assigned to two cases, get unique ones
    # (ie when same family anayzed for two different probands, same samle gets 2 cases)
    all_unique_cases = []
    for a_case in all_cases:
        if a_case['sample']['uuid'] not in [i['sample']['uuid'] for i in all_unique_cases]:
            all_unique_cases.append(a_case)
    print(len(all_unique_cases))

    if not all_unique_cases:
        check.summary = 'All Good!'
        return check

    step1_name = 'workflow_bwa-mem_no_unzip-check'
    step2_name = 'workflow_add-readgroups-check'
    step3_name = 'workflow_merge-bam-check'
    step4_name = 'workflow_picard-MarkDuplicates-check'
    step5_name = 'workflow_sort-bam-check'
    step6_name = 'workflow_gatk-BaseRecalibrator'
    step7_name = 'workflow_gatk-ApplyBQSR-check'
    step8_name = 'workflow_granite-mpileupCounts'
    step9_name = 'workflow_gatk-HaplotypeCaller'
    step10_name = 'cgap-bamqc'

    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)
    wf_errs = cgap_utils.check_latest_workflow_version(all_system_wfs)
    if wf_errs:
        check.summary = 'Error, problem with latest workflow versions'
        check.brief_output.extend(wf_errs)
        return check
    cnt = 0
    for a_case in all_unique_cases:
        cnt += 1
        # get all items around case except old workflow versions, mother, father and sample_processing(we only want sample related items)
        all_items, all_uuids = ff_utils.expand_es_metadata([a_case['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version',
                                                                         'sample_processing',
                                                                         'family',
                                                                         'mother',
                                                                         'father'
                                                                         ])
        a_sample = [i for i in all_items['sample'] if i['uuid'] == a_case['sample']['uuid']][0]
        bam_sample_id = a_sample.get('bam_sample_id')
        if not bam_sample_id:
            final_status = a_case['accession'] + "-" + a_sample['accession'] + ' missing bam_sample_id'
            check.brief_output.append(final_status)
            check.full_output['problematic_runs'].append({a_sample['accession']: final_status})
            break

        now = datetime.utcnow()
        print(a_case['accession'], a_sample['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            check.summary = 'Timout - only {} samples were processed'.format(str(cnt))
            break
        # collect similar types of items under library
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}

        # are all files uploaded ?
        all_uploaded = True
        # get all fastq files (can be file_fastq or file_processed)
        fastq_file_ids = [i.get('@id') for i in a_sample.get('files', [])]
        if not fastq_file_ids:
            final_status = a_sample['accession'] + ' skipped, no files on sample'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({a_sample['accession']: 'no files on sample'})
            continue
        fastq_files = [i for i in all_files if i['@id'] in fastq_file_ids]
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
        # is this sample meant for part 3 as Trio?
        will_go_to_part_3 = False
        exclude_for_mpilup = ['WGS', 'WGS-Upstream only', 'WGS-Joint Calling'
                              'WES', 'WES-Upstream only', 'WES-Joint Calling']
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
            s1_tag = 'step1_' + a_sample['accession'] + '_' + pair[0].split('/')[2] + '_' + pair[1].split('/')[2]
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep, s1_tag, pair,
                                                                  s1_input_files,  step1_name, 'raw_bam')
            # RUN STEP 2
            if step1_status != 'complete':
                step2_status = ''
                stop_level_2 = True
            else:
                s2_input_files = {'input_bam': step1_output}
                s2_tag = 'step2_' + a_sample['accession'] + '_' + step1_output.split('/')[2]
                add_par = {"parameters": {"sample_name": bam_sample_id}}
                keep, step2_status, step2_output = cgap_utils.stepper(library, keep, s2_tag, step1_output,
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
                s3_tag = 'step3_' + a_sample['accession']
                keep, step3_status, step3_output = cgap_utils.stepper(library, keep, s3_tag, s3_input_bams,
                                                                      s3_input_files,  step3_name, 'merged_bam')
        # RUN STEP 4
        if step3_status != 'complete':
            step4_status = ""
        else:
            s4_input_files = {'input_bam': step3_output}
            s4_tag = 'step4_' + a_sample['accession']
            keep, step4_status, step4_output = cgap_utils.stepper(library, keep, s4_tag, step3_output,
                                                                  s4_input_files,  step4_name, 'dupmarked_bam')
        # RUN STEP 5
        if step4_status != 'complete':
            step5_status = ""
        else:
            s5_input_files = {'input_bam': step4_output}
            s5_tag = 'step5_' + a_sample['accession']
            keep, step5_status, step5_output = cgap_utils.stepper(library, keep, s5_tag, step4_output,
                                                                  s5_input_files,  step5_name, 'sorted_bam')
        # RUN STEP 6
        if step5_status != 'complete':
            step6_status = ""
        else:
            s6_input_files = {'input_bam': step5_output,
                              'known-sites-snp': '/files-reference/GAPFI4LJRN98/',
                              'known-sites-indels': '/files-reference/GAPFIAX2PPYB/',
                              'reference': '/files-reference/GAPFIXRDPDK5/'}
            s6_tag = 'step6_' + a_sample['accession']
            keep, step6_status, step6_output = cgap_utils.stepper(library, keep, s6_tag, step5_output,
                                                                  s6_input_files,  step6_name, 'recalibration_report')
        # RUN STEP 7
        if step6_status != 'complete':
            step7_status = ""
        else:
            s7_input_files = {'input_bam': step5_output,
                              'reference': '/files-reference/GAPFIXRDPDK5/',
                              'recalibration_report': step6_output}
            s7_tag = 'step7_' + a_sample['accession']
            keep, step7_status, step7_output = cgap_utils.stepper(library, keep, s7_tag, step6_output,
                                                                  s7_input_files,  step7_name, 'recalibrated_bam')
        # RUN STEP 8 - only run if will_go_to_part_3 is True
        if will_go_to_part_3:
            if step7_status != 'complete':
                step8_status = ""
            else:
                # mpileupCounts
                s8_input_files = {'input_bam': step7_output,
                                  'regions': '/files-reference/GAPFIBGEOI72/',
                                  'reference': '/files-reference/GAPFIXRDPDK5/',
                                  'additional_file_parameters': {'input_bam': {"mount": True}}}
                s8_tag = 'step8_' + a_sample['accession']
                keep, step8_status, step8_output = cgap_utils.stepper(library, keep, s8_tag, step7_output,
                                                                      s8_input_files,  step8_name, 'rck')
        else:
            step8_status = 'complete'
            step8_output = ''

        # RUN STEP 9 # run together with step8
        if step7_status != 'complete':
            step9_status = ""
        else:
            s9_input_files = {'input_bam': step7_output,
                              'regions': '/files-reference/GAPFIBGEOI72/',
                              'reference': '/files-reference/GAPFIXRDPDK5/'}
            s9_tag = 'step9_' + a_sample['accession']
            keep, step9_status, step9_output = cgap_utils.stepper(library, keep, s9_tag, step7_output,
                                                                  s9_input_files,  step9_name, 'gvcf')

        # step10 bamqc
        # RUN STEP 10 # run together with step9 and 8
        if step7_status != 'complete':
            step10_status = ""
        else:
            s10_input_files = {'input_bam': step7_output,
                               'additional_file_parameters': {'input_bam': {"mount": True}}
                               }
            update_pars = {"parameters": {'sample': bam_sample_id}}
            s10_tag = 'step10_' + a_sample['accession']
            keep, step10_status, step10_output = cgap_utils.stepper(library, keep, s10_tag, step7_output,
                                                                    s10_input_files,  step10_name, '',
                                                                    additional_input=update_pars, no_output=True)
        # are all runs done
        all_runs_completed = False
        if step8_status == 'complete' and step9_status == 'complete' and step10_status == 'complete':
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
    accepted_analysis_types = ['WGS-Trio', 'WGS', 'WGS-Group', 'WGS-Joint calling',
                               'WES-Trio', 'WES', 'WES-Group', 'WES-Joint calling']
    analysis_type_filter = "".join(["&analysis_type=" + i for i in accepted_analysis_types])
    version_filter = "".join(["&completed_processes!=" + i for i in cgap_partII_version])
    file_filter = '&samples.processed_files.uuid!=No value'
    q = query_base + version_filter + analysis_type_filter + file_filter
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        check.summary = 'All Good!'
        return check
    # list step names
    step1_name = 'workflow_gatk-CombineGVCFs'
    step2_name = 'workflow_gatk-GenotypeGVCFs-check'
    step3_name = 'workflow_vep-parallel'
    step4_name = 'workflow_mutanno-micro-annot-check'
    step5_name = 'workflow_granite-qcVCF'

    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)
    wf_errs = cgap_utils.check_latest_workflow_version(all_system_wfs)
    if wf_errs:
        final_status = 'Error, workflow versions'
        check.brief_output.extend(wf_errs)
        return check

    # iterate over msa
    print(len(res))
    cnt = 0
    for an_msa in res:
        cnt += 1
        all_items, all_uuids = ff_utils.expand_es_metadata([an_msa['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version'])
        now = datetime.utcnow()
        print(an_msa['@id'], an_msa.get('aliases', ['no-alias'])[0], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            check.summary = 'Timout - only {} sample_processings were processed'.format(str(cnt))
            break

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}

        all_samples = an_msa['samples']
        # if there are multiple families, this part will need changes
        families = an_msa['families']
        if len(families) > 1:
            final_status = an_msa['@id'] + ' error, multiple families'
            check.brief_output.extend(final_status)
            check.full_output['problematic_runs'].append({an_msa['@id']: final_status})
            continue
        # get variables used by vcfqc
        samples_pedigree = an_msa['samples_pedigree']
        vcfqc_input_samples, qc_pedigree, run_mode, error = cgap_utils.analyze_pedigree(samples_pedigree, all_samples)
        if error:
            error_msg = an_msa['@id'] + " " + error
            check.brief_output.extend(error_msg)
            check.full_output['problematic_runs'].append({an_msa['@id']: error_msg})
            continue
        # used by comHet, reversed one will be used by vcfqc
        sample_ids = []
        # check all samples and collect input files
        for a_sample in vcfqc_input_samples:
            sample_resp = [i for i in all_items['sample'] if i['accession'] == a_sample][0]
            sample_id = sample_resp.get('bam_sample_id')
            if sample_id:
                sample_ids.append(sample_id)
        # if there are multiple samples merge them
        # if not skip step 1
        input_samples = an_msa['samples']
        input_vcfs = []
        # check if all samples are done with PartI and collect input files
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
                              'chromosomes': '/files-reference/GAPFIGJVJDUY/',
                              'reference': '/files-reference/GAPFIXRDPDK5/'}
            # benchmarking
            if len(input_samples) < 4:
                ebs_size = '10x'
            else:
                ebs_size = str(10 + len(input_samples) - 3) + 'x'
            update_pars = {"config": {"ebs_size": ebs_size}}
            s1_tag = an_msa['@id'] + '_Part2step1' + input_vcfs[0].split('/')[2]
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  s1_tag, input_vcfs,
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
                              "reference": "/files-reference/GAPFIXRDPDK5/",
                              "known-sites-snp": "/files-reference/GAPFI4LJRN98/",
                              'chromosomes': '/files-reference/GAPFIGJVJDUY/'}
            s2_tag = an_msa['@id'] + '_Part2step2' + step1_output.split('/')[2]
            keep, step2_status, step2_output = cgap_utils.stepper(library, keep,
                                                                  s2_tag, step1_output,
                                                                  s2_input_files,  step2_name, 'vcf')

        if step2_status != 'complete':
            step3_status = ""
        else:
            # run step3 VEP
            s3_input_files = {'input_vcf': step2_output,
                              'mti': "/files-reference/GAPFIFJM2A8Z/",
                              'reference': "/files-reference/GAPFIXRDPDK5/",
                              'regions': "/files-reference/GAPFIBGEOI72/",
                              'vep_tar': "/files-reference/GAPFIFZB4NUO/",
                              'additional_file_parameters': {'mti': {"mount": True},
                                                             'reference': {"mount": True},
                                                             'vep_tar': {"mount": True}
                                                             }
                              }
            s3_tag = an_msa['@id'] + '_Part2step3' + step2_output.split('/')[2]
            # there are 2 files we need, one to use in the next step
            keep, step3_status, step3_outputs = cgap_utils.stepper(library, keep,
                                                                   s3_tag, step2_output,
                                                                   s3_input_files,  step3_name, ['microannot_mti', 'annot_mti'])

        if step3_status != 'complete':
            step4_status = ""
        else:
            # run step4 micro annotation
            # VEP has 2 outputs, unpack them
            step3_output_micro = step3_outputs[0]
            step3_output_full = step3_outputs[1]
            s4_input_files = {'input_vcf': step3_output_micro,
                              'mti': "/files-reference/GAPFIFJM2A8Z/",
                              'regions': "/files-reference/GAPFIBGEOI72/",
                              'additional_file_parameters': {'mti': {"mount": True}}
                              }
            s4_tag = an_msa['@id'] + '_Part2step4' + step3_output_micro.split('/')[2]
            # this step is tagged (with uuid of sample_processing, which means
            # that when finding the workflowruns, it will not only look with
            # workflow app name and input files, but also the tag on workflow run items
            # since we differentiate sample processings at this step, downsteam will be separated
            # no need for tagging them too
            keep, step4_status, step4_output = cgap_utils.stepper(library, keep,
                                                                  s4_tag, step3_output_micro,
                                                                  s4_input_files,  step4_name, 'annotated_vcf',
                                                                  tag=an_msa['uuid'])

        if step4_status != 'complete':
            step5_status = ""
        else:
            # step 5 vcfqc
            s5_input_files = {"input_vcf": step4_output,
                              'additional_file_parameters': {'input_vcf': {"unzip": "gz"}}
                              }
            str_qc_pedigree = str(json.dumps(qc_pedigree))
            proband_first_sample_list = list(reversed(sample_ids))  # proband first sample ids
            update_pars = {"parameters": {"samples": proband_first_sample_list,
                                          "pedigree": str_qc_pedigree,
                                          "trio_errors": True,
                                          "het_hom": True,
                                          "ti_tv": True}}
            s5_tag = an_msa['@id'] + '_Part3step5'
            keep, step3c_status, step3c_output = cgap_utils.stepper(library, keep,
                                                                    s5_tag, step4_output,
                                                                    s5_input_files,  step5_name, '',
                                                                    additional_input=update_pars, no_output=True)

        final_status = an_msa['@id']
        completed = []
        pipeline_tag = cgap_partII_version[-1]
        previous_tags = an_msa.get('completed_processes', [])

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if step5_status == 'complete':
            final_status += ' completed'
            # existing_pf = [i['@id'] for i in an_msa['processed_files']]
            completed = [
                an_msa['@id'],
                {'processed_files': [step3_output_full, step4_output],
                 'completed_processes': previous_tags + [pipeline_tag, ]}]
            print('COMPLETED', step4_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])
            if problematic_run:
                final_status += ' |Problem: ' + " ".join([i[0] for i in problematic_run])

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
        # in rare cases, the same run can be triggered by two different sample_processings
        # example is a quad analyzed for 2 different probands. In this case you want a single
        # combineGVCF step, but 2 sample_processings will be generated trying to run same job,
        # identify and remove duplicates
        check.full_output['needs_runs'] = cgap_utils.remove_duplicate_need_runs(check.full_output['needs_runs'])
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
    accepted_analysis_types = ['WGS-Trio', 'WGS', 'WGS-Group', 'WES-Trio', 'WES', 'WES-Group']
    analysis_type_filter = "".join(["&analysis_type=" + i for i in accepted_analysis_types])
    version_filter = "".join(["&completed_processes!=" + i for i in cgap_partIII_version])
    q = query_base + analysis_type_filter + version_filter
    # print(q)
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        check.summary = 'All Good!'
        return check
    # list step names
    step1_name = 'workflow_granite-rckTar'
    step2_name = 'workflow_granite-filtering-check'
    step3_name = 'workflow_granite-novoCaller-rck-check'
    step4_name = 'workflow_granite-comHet-check'
    step5_name = 'workflow_mutanno-annot-check'
    step5a_name = 'workflow_granite-qcVCF'
    step6_name = 'bamsnap'
    # collect all wf for wf version check
    all_system_wfs = ff_utils.search_metadata('/search/?type=Workflow&status=released', my_auth)
    wf_errs = cgap_utils.check_latest_workflow_version(all_system_wfs)
    if wf_errs:
        final_status = 'Error, workflow versions'
        check.brief_output.extend(wf_errs)
        return check
    # iterate over msa
    print(len(res))
    cnt = 0
    for an_msa in res:
        cnt += 1
        all_items, all_uuids = ff_utils.expand_es_metadata([an_msa['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['previous_version'])
        now = datetime.utcnow()

        alll = an_msa.get('aliases', ['none'])[0]

        print()
        print(an_msa['@id'], alll, (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            check.summary = 'Timout - only {} sample_processings were processed'.format(str(cnt))
            break

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        file_items = [typ for typ in all_items if typ.startswith('file_') and typ != 'file_format']
        all_files = [i for typ in all_items for i in all_items[typ] if typ in file_items]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}

        # only run for trios
        all_samples = an_msa['samples']
        # if there are multiple families, this part will need changes
        families = an_msa['families']
        if len(families) > 1:
            final_status = an_msa['@id'] + ' error, multiple families'
            check.brief_output.extend(final_status)
            check.full_output['problematic_runs'].append({an_msa['@id']: final_status})
            continue
        samples_pedigree = an_msa['samples_pedigree']

        # return input samples for trio/proband in sequence father,mother,proband
        input_samples, qc_pedigree, run_mode, error = cgap_utils.analyze_pedigree(samples_pedigree, all_samples)
        if error:
            error_msg = an_msa['@id'] + " " + error
            check.brief_output.extend(error_msg)
            check.full_output['problematic_runs'].append({an_msa['@id']: error_msg})
            continue

        # Setup for step 1a
        input_rcks = []
        sample_ids = []  # used by comHet
        input_bams = []  # used by bamsnap
        input_titles = []  # used by bamsnap
        # check all samples and collect input files
        for a_sample in input_samples:
            rck = ''
            sample_resp = [i for i in all_items['sample'] if i['accession'] == a_sample][0]
            sample_id = sample_resp.get('bam_sample_id')
            if sample_id:
                sample_ids.append(sample_id)
            rck = [i['@id'] for i in sample_resp['processed_files'] if i['display_title'].endswith('rck.gz')]
            if rck:
                input_rcks.append(rck[0])
            bam = [i['@id'] for i in sample_resp['processed_files'] if i['display_title'].endswith('bam')]
            if bam:
                input_bams.append(bam[0])
        # older processings might be missing rck files, a precaution
        if len(input_rcks) != len(input_samples) and run_mode == 'trio':
            final_status = an_msa['@id'] + ' missing rck files on samples'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({an_msa['@id']: final_status})
            continue
        # bail if sample id is missing
        if len(sample_ids) != len(input_samples):
            final_status = an_msa['@id'] + 'some samples missing bam_sample_id'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({an_msa['@id']: final_status})
            continue

        # add the input titles used by bamsnap
        if len(sample_ids) == 1:
            proband_title = '{} (Proband)'.format(sample_ids[0])
            input_titles = [proband_title]
        else:
            father_title = '{} (Father)'.format(sample_ids[0])
            mother_title = '{} (Mother)'.format(sample_ids[1])
            proband_title = '{} (Proband)'.format(sample_ids[2])
            input_titles = [father_title, mother_title, proband_title]

        # we need the vep and micro vcf in the processed_files field of sample_processing
        if len(an_msa.get('processed_files', [])) != 2:
            final_status = an_msa['@id'] + '2 items in processed_files of msa was expected'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({an_msa['@id']: final_status})
            continue

        # extract input files from msa_processed files
        mti_vep_full = an_msa['processed_files'][0]['@id']
        micro_annotated_vcf = an_msa['processed_files'][1]['@id']

        # if trio, run rck_tar
        if run_mode == 'trio':
            new_names = [i + '.rck.gz' for i in sample_ids]  # proband last
            s1_input_files = {'input_rcks': input_rcks,  # proband last
                              'additional_file_parameters': {'input_rcks': {"rename": new_names}}
                              }
            s1_tag = an_msa['@id'] + '_Part3step1'
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  s1_tag, input_rcks,
                                                                  s1_input_files,  step1_name, 'rck_tar')
        else:
            step1_status = 'complete'
            step1_output = ''

        if step1_status != 'complete':
            step2_status = ""
        else:
            # Run filtering
            input_vcf = micro_annotated_vcf
            s2_input_files = {"input_vcf": input_vcf,
                              # "bigfile": "20004873-b672-4d84-a7c1-7fd5c0407519",
                              'additional_file_parameters': {'input_vcf': {"unzip": "gz"}}
                              }
            s2_tag = an_msa['@id'] + '_Part3step2'
            keep, step2_status, step2_output = cgap_utils.stepper(library, keep,
                                                                  s2_tag, input_vcf,
                                                                  s2_input_files,  step2_name, 'merged_vcf')

        if step2_status != 'complete':
            step3_status = ""
        else:
            # Run novoCaller
            if run_mode == 'trio':
                s3_input_files = {'input_vcf': step2_output,
                                  'unrelated': '/files-processed/GAPFI344NFZE/',
                                  'trio': step1_output,
                                  'additional_file_parameters': {'input_vcf': {"unzip": "gz"},
                                                                 'unrelated': {"mount": True},
                                                                 'trio': {"mount": True},
                                                                 }
                                  }
                s3_tag = an_msa['@id'] + '_Part3step3'
                keep, step3_status, step3_output = cgap_utils.stepper(library, keep,
                                                                      s3_tag, step2_output,
                                                                      s3_input_files,  step3_name, 'novoCaller_vcf')
            else:
                # if proband only pass step2 output to next one
                step3_status = 'complete'
                step3_output = step2_output

        if step3_status != 'complete':
            step4_status = ""
        else:
            # Run ComHet
            s4_input_files = {"input_vcf": step3_output,
                              'additional_file_parameters': {'input_vcf': {"unzip": "gz"}}
                              }
            proband_first_sample_list = list(reversed(sample_ids))  # proband first sample ids
            update_pars = {"parameters": {"trio": proband_first_sample_list}}
            s4_tag = an_msa['@id'] + '_Part3step4'
            keep, step4_status, step4_output = cgap_utils.stepper(library, keep,
                                                                  s4_tag, step3_output,
                                                                  s4_input_files,  step4_name, 'comHet_vcf',
                                                                  additional_input=update_pars)

        if step4_status != 'complete':
            step5_status = ""
        else:
            # Run Full Annotation
            s5_input_files = {'input_vcf': step4_output,
                              'mti': '/files-reference/GAPFIL98NJ2K/',
                              'mti_vep': mti_vep_full,
                              'chainfile': '/files-reference/GAPFIYPTSAU8/',
                              'regions': '/files-reference/GAPFIBGEOI72/',
                              'additional_file_parameters': {'mti': {"mount": True},
                                                             'mti_vep': {"mount": True}
                                                             },
                              }
            s5_tag = an_msa['@id'] + '_Part3step5'
            keep, step5_status, step5_output = cgap_utils.stepper(library, keep,
                                                                  s5_tag, step4_output,
                                                                  s5_input_files,  step5_name, 'annotated_vcf')

        if step5_status != 'complete':
            step5a_status = ""
        else:
            # Run step 5a vcfqc
            s5a_input_files = {"input_vcf": step5_output,
                               'additional_file_parameters': {'input_vcf': {"unzip": "gz"}}
                               }
            str_qc_pedigree = str(json.dumps(qc_pedigree))
            proband_first_sample_list = list(reversed(sample_ids))  # proband first sample ids
            update_pars = {"parameters": {"samples": proband_first_sample_list,
                                          "pedigree": str_qc_pedigree,
                                          "trio_errors": True,
                                          "het_hom": False,
                                          "ti_tv": False},
                           "custom_qc_fields": {"filtering_condition": ("((Exonic and splice variants OR spliceAI>0.2) AND "
                                                                        "(gnomAD AF<0.01 AND not seen in 2 individuals among a set of 20 unrelated samples)) OR "
                                                                        "(Clinvar Pathogenic/Likely Pathogenic, Conflicting Interpretation or Risk Factor)")
                                                }
                           }
            s5a_tag = an_msa['@id'] + '_Part3step5a'
            keep, step5a_status, step5a_output = cgap_utils.stepper(library, keep,
                                                                    s5a_tag, step5_output,
                                                                    s5a_input_files,  step5a_name, '',
                                                                    additional_input=update_pars, no_output=True)

        if step5a_status != 'complete':
            step6_status = ""
        else:
            # BAMSNAP
            # TODO: add all samples, not just trio
            # change order of samples and input_titles to have proband first on bamsnap
            input_bams_rev = input_bams[::-1]
            input_titles_rev = input_titles[::-1]
            s6_input_files = {'input_bams': input_bams_rev,
                              'input_vcf': step5_output,
                              'ref': '/files-reference/GAPFIXRDPDK5/',
                              'additional_file_parameters': {'input_vcf': {"mount": True},
                                                             'input_bams': {"mount": True},
                                                             'ref': {"mount": True}
                                                             }
                              }
            s6_tag = an_msa['@id'] + '_Part3step6'
            update_pars = {"parameters": {"titles": input_titles_rev}}  # proband last
            keep, step6_status, step6_output = cgap_utils.stepper(library, keep,
                                                                  s6_tag, step5_output,
                                                                  s6_input_files,  step6_name, '',
                                                                  additional_input=update_pars, no_output=True)

        final_status = an_msa['@id']
        completed = []
        pipeline_tag = cgap_partIII_version[-1]
        previous_tags = an_msa.get('completed_processes', [])
        previous_files = [i['@id'] for i in an_msa['processed_files']]

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        if step6_status == 'complete':
            final_status += ' completed'
            # existing_pf = [i['@id'] for i in an_msa['processed_files']]
            completed = [
                an_msa['@id'],
                {'processed_files': previous_files + [step5_output, ],
                 'completed_processes': previous_tags + [pipeline_tag, ]}]
            print('COMPLETED', step5_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])
            if problematic_run:
                final_status += ' |Problem: ' + " ".join([i[0] for i in problematic_run])

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


@check_function(start_date=None, file_accessions="")
def ingest_vcf_status(connection, **kwargs):
    """Searches for fastq files that don't have ingest_vcf
    Keyword arguments:
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    file_accession -- run check with given files instead of the default query
                      expects comma/space separated accessions
    """
    check = CheckResult(connection, 'ingest_vcf_status')
    my_auth = connection.ff_keys
    check.action = "ingest_vcf_start"
    check.brief_output = []
    check.full_output = {}
    check.status = 'PASS'
    check.allow_action = False

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
    query = ("/search/?file_type=full+annotated+VCF&type=FileProcessed"
             "&file_ingestion_status=No value&file_ingestion_status=N/A"
             "status!=uploading&status!=to be uploaded by workflow&status!=upload failed")
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        query += '&date_created.from=' + s_date
    # add accessions
    file_accessions = kwargs.get('file_accessions')
    if s_date:
        file_accessions = file_accessions.replace(' ', ',')
        accessions = [i.strip() for i in file_accessions.split(',') if i]
        for an_acc in accessions:
            query += '&accession={}'.format(an_acc)
    # The search
    results = ff_utils.search_metadata(query, key=my_auth)
    if not results:
        check.summary = 'All Good!'
        return check
    msg = '{} files will be added to the ingestion_queue'.format(str(len(results)))
    files = [i['uuid'] for i in results]
    check.status = 'WARN'  # maybe use warn?
    check.brief_output = [msg, ]
    check.summary = msg
    check.full_output = {'files': files,
                         'accessions': [i['accession'] for i in results]}
    check.allow_action = True
    return check


@action_function()
def ingest_vcf_start(connection, **kwargs):
    """Start ingest_vcf runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'ingest_vcf_start')
    action_logs = {'runs_started': [], 'runs_failed': []}
    my_auth = connection.ff_keys
    ingest_vcf_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    targets = ingest_vcf_check_result['files']
    post_body = {"uuids": targets}
    action_logs = ff_utils.post_metadata(post_body, "/queue_ingestion", my_auth)
    action.output = action_logs
    action.status = 'DONE'
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

    if not all_samples:
        check.summary = 'All Good!'
        return check

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
            tag = 'cram2fastq_' + a_sample['accession'] + '_' + a_cram['@id']
            keep, step1_status, step1_output = cgap_utils.stepper(library, keep,
                                                                  tag, a_cram['@id'],
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
