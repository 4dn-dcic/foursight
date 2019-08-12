from datetime import datetime
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
from .helpers import cgap_utils

lambda_limit = cgap_utils.lambda_limit


@check_function(lab_title=None, start_date=None)
def cgap_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = init_check_res(connection, 'cgap_status')
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
    q = '/search/?type=ExperimentSeq&experiment_type=WGS&processed_files.uuid=No+value'
    all_exps = ff_utils.search_metadata(q, my_auth)
    print(len(all_exps))

    step1_name = 'workflow_bwa-mem_no_unzip-check'
    step2_name = 'workflow_readgroups-check'
    step3_name = 'workflow_merge_bam-check'
    step4_name = 'workflow_picard-markduplicates-check'
    step5_name = 'workflow_sort-bam-check'
    step6_name = 'workflow_gatk-BaseRecalibrator'
    step7_name = 'workflow_gatk-ApplyBQSR-check'
    # step8_name = 'workflow_index-sorted-bam'

    for an_exp in all_exps:
        print('===================')
        print(an_exp['accession'])
        all_items, all_uuids = ff_utils.expand_es_metadata([an_exp['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        now = datetime.utcnow()
        print(an_exp['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        all_files = [i for typ in all_items for i in all_items[typ] if typ.startswith('file_')]
        all_items.get('file_fast', []) + all_items.get('workflow_run_sbg', [])
        exp_files, refs = cgap_utils.find_fastq_info(an_exp, all_items['file_fastq'])
        missing_run = []
        running = []
        problematic_run = []
        s3_input_bams = []
        stop_level_2 = False
        for pair in exp_files:
            # RUN STEP 1
            s1_input_files = {'fastq_R1': pair[0], 'fastq_R2': pair[1], 'reference': refs['bwa_ref']}
            s1_tag = an_exp['accession'] + '_' + pair[0].split('/')[2] + '_' + pair[1].split('/')[2]
            running, problematic_run, missing_run, step1_status, step1_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                   'step1', s1_tag, pair,
                                                                                                   s1_input_files,  step1_name, 'raw_bam', {}, 'human')

            # RUN STEP 2
            if step1_status != 'complete':
                step2_status = ''
                stop_level_2 = True
            else:
                s2_input_files = {'input_bam': step1_output}
                s2_tag = an_exp['accession'] + '_' + step1_output.split('/')[2]
                add_par = {"parameters": {"sample_name": an_exp['aliases'][0].split(':')[1]}}
                running, problematic_run, missing_run, step2_status, step2_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                       'step2', s2_tag, step1_output,
                                                                                                       s2_input_files,  step2_name, 'bam_w_readgroups', add_par, 'human')
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
                running, problematic_run, missing_run, step3_status, step3_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                       'step3', an_exp['accession'], s3_input_bams,
                                                                                                       s3_input_files,  step3_name, 'merged_bam', {}, 'human')

        # RUN STEP 4
        if step3_status != 'complete':
            step4_status = ""
        else:
            s4_input_files = {'input_bam': step3_output}
            running, problematic_run, missing_run, step4_status, step4_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                   'step4', an_exp['accession'], step3_output,
                                                                                                   s4_input_files,  step4_name, 'dupmarked_bam', {}, 'human')

        # RUN STEP 5
        if step4_status != 'complete':
            step5_status = ""
        else:
            s5_input_files = {'input_bam': step4_output}
            running, problematic_run, missing_run, step5_status, step5_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                   'step5', an_exp['accession'], step4_output,
                                                                                                   s5_input_files,  step5_name, 'sorted_bam', {}, 'human')

        # RUN STEP 6
        if step5_status != 'complete':
            step6_status = ""
        else:
            s6_input_files = {'input_bam': step5_output, 'known-sites-snp': '4DNFI2RDOLUQ',
                              'known-sites-indels': '4DNFIBN3576F', 'reference': '4DNFIBH13WES'}
            running, problematic_run, missing_run, step6_status, step6_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                   'step6', an_exp['accession'], step5_output,
                                                                                                   s6_input_files,  step6_name, 'recalibration_report', {}, 'human')

        # RUN STEP 7
        if step6_status != 'complete':
            step7_status = ""
        else:
            s7_input_files = {'input_bam': step5_output, 'reference': '4DNFIBH13WES', 'recalibration_report': step6_output}
            running, problematic_run, missing_run, step7_status, step7_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
                                                                                                   'step7', an_exp['accession'], step6_output,
                                                                                                   s7_input_files,  step7_name, 'recalibrated_bam', {}, 'human')

        # # RUN STEP 8
        # if step7_status != 'complete':
        #     step8_status = ""
        # else:
        #     s8_input_files = {'bam': step7_output}
        #     running, problematic_run, missing_run, step8_status, step8_output = cgap_utils.stepper(all_files, all_wfrs, running, problematic_run, missing_run,
        #                                                                                            'step8', an_exp['accession'], step7_output,
        #                                                                                            s8_input_files,  step8_name, '', {}, 'human', no_output=True)

        final_status = an_exp['accession']
        completed = []
        if step7_status == 'complete':
            final_status += ' completed'
            completed = [an_exp['accession'], {'processed_files': [step7_output]}]
            print('COMPLETED', step7_output)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])

        # add dictionaries to main ones
        set_acc = an_exp['accession']
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
def cgap_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = init_action_res(connection, 'cgap_start')
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
