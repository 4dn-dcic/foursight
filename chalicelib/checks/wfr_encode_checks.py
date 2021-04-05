from datetime import datetime
from dcicutils import ff_utils
from dcicutils import s3Utils
from .helpers import wfr_utils
from .helpers import wfrset_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *

lambda_limit = wfr_utils.lambda_limit


@check_function(lab_title=None, start_date=None)
def chipseq_status(connection, **kwargs):
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
    exp_type = 'ChIP-seq'
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
    # run step 0 on all experiments with more than 2 sets of files
    # for control sets, run step1c on each experiment and finish
    # for non-control sets, run step1 on each experiment, check if control is ready, run step2 on set
    step0_name = 'merge-fastq'
    step1_name = 'encode-chipseq-aln-chip'
    step1c_name = 'encode-chipseq-aln-ctl'
    step2_name = 'encode-chipseq-postaln'

    for a_set in res:
        set_acc = a_set['accession']
        all_items, all_uuids = ff_utils.expand_es_metadata([a_set['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        now = datetime.utcnow()
        print(a_set['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break
        # are all files uploaded ?
        all_uploaded = True
        for a_file in all_items['file_fastq']:
            if a_file['status'] in ['uploading', 'upload failed']:
                all_uploaded = False

        if not all_uploaded:
            final_status = a_set['accession'] + ' skipped, waiting for file upload'
            print(final_status)
            check.brief_output.append(final_status)
            check.full_output['skipped'].append({a_set['accession']: 'files status uploading'})
            continue

        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        all_files = [i for typ in all_items for i in all_items[typ] if typ.startswith('file_')]
        all_qcs = [i for typ in all_items for i in all_items[typ] if typ.startswith('quality_metric')]
        library = {'wfrs': all_wfrs, 'files': all_files, 'qcs': all_qcs}
        keep = {'missing_run': [], 'running': [], 'problematic_run': []}
        # if all completed, patch this info
        complete = {'patch_opf': [],
                    'add_tag': []}
        set_acc = a_set['accession']
        # for i in all_items:
        #     print(i, len(all_items[i]))

        # some feature to extract from each set
        control = ""  # True or False (True if set is control)
        control_set = ""  # None if there are no control experiments or if the set is control
        target_type = ""  # Histone or TF (or None for control)
        paired = ""  # single or paired , checked for each experiment
        organism = ""
        replicate_exps = a_set['replicate_exps']
        replicate_exps = sorted(replicate_exps, key=lambda x: [x['bio_rep_no'], x['tec_rep_no']])
        # get organism, target and control from the first replicate
        f_exp = replicate_exps[0]['replicate_exp']['uuid']
        f_exp_resp = [i for i in all_items['experiment_seq'] if i['uuid'] == f_exp][0]
        # have to do another get for control experiments if there is one
        control, control_set, target_type, organism = wfr_utils.get_chip_info(f_exp_resp, my_auth)
        print('ORG:', organism, "CONT:", control, "TARGET:", target_type, "CONT_SET:", control_set)
        set_summary = " - ".join([set_acc, str(organism), str(target_type), str(control)])
        # sanity checks
        # if control and also has an AB with target
        if control and target_type:
            set_summary += "| error - has target and is control"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: set_summary})
            continue
        # can only process mouse and human at the moment
        if organism not in ['mouse', 'human']:
            set_summary += "| organism not ready for chip"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: set_summary})
            continue
        # if not control, we need a target
        if not control and not target_type:
            set_summary += "| missing target type"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: set_summary})
            continue
        # collect results from step1 runs for step2
        ta = []
        taxcor = []
        ta_cnt = []
        # track if all experiments completed step0 and step1
        ready_for_step2 = True
        for an_exp in replicate_exps:
            # track if all experiments completed step0
            ready_for_step1 = True
            # track if all control experiments are completed processing
            exp_id = an_exp['replicate_exp']['accession']
            exp_resp = [i for i in all_items['experiment_seq'] if i['accession'] == exp_id][0]
            exp_files, paired = wfr_utils.get_chip_files(exp_resp, all_files)
            # if there are more then 2 files, we need to merge:
            print(exp_id, len(exp_files), paired)
            # if too many input, merge them
            if len(exp_files) > 2:
                # exp_files format [[pair1,pair2], [pair1, pair2]]  @id
                input_list = []
                if paired == 'paired':
                    # first add paired end 1s
                    input_list.append([i[0] for i in exp_files])
                    input_list.append([i[1] for i in exp_files])
                elif paired == 'single':
                    input_list.append([i[0] for i in exp_files])
                # collect files for step1 and step1c
                merged_files = []
                step0_status = 'complete'
                merge_enum = 0
                # if paired, need to run merge twice for each end
                for merge_case in input_list:
                    merge_enum += 1
                    # RUN STEP 0
                    s0_input_files = {'input_fastqs': merge_case}
                    s0_tag = exp_id + '_p' + str(merge_enum)
                    keep, step0_status, step0_output = wfr_utils.stepper(library, keep,
                                                                         'step0', s0_tag, merge_case,
                                                                         s0_input_files, step0_name, 'merged_fastq')
                    if step0_status == 'complete':
                        merged_files.append(step0_output)
                    else:
                        ready_for_step1 = False

                if ready_for_step1:
                    # rewrite exp_files with merged ones
                    exp_files = [[]]
                    for a_merged in merged_files:
                        exp_files[0].append(a_merged)
            # if step0 was not complete, skip checks for step2
            if not ready_for_step1:
                ready_for_step2 = False
                continue

            # step1 references:
            input_files = {}
            if organism == 'human':
                org = 'hs'
                input_files['chip.bwa_idx_tar'] = '/files-reference/4DNFIZQB369V/'
                input_files['chip.blacklist'] = '/files-reference/4DNFIZ1TGJZR/'
                input_files['chip.chrsz'] = '/files-reference/4DNFIZJB62D1/'
                input_files['additional_file_parameters'] = {"chip.bwa_idx_tar": {"rename": "GRCh38_no_alt_analysis_set_GCA_000001405.15.fasta.tar"}}
            if organism == 'mouse':
                org = 'mm'
                input_files['chip.bwa_idx_tar'] = '/files-reference/4DNFIZ2PWCC2/'
                input_files['chip.blacklist'] = '/files-reference/4DNFIZ3FBPK8/'
                input_files['chip.chrsz'] = '/files-reference/4DNFIBP173GC/'
                input_files['additional_file_parameters'] = {"chip.bwa_idx_tar": {"rename": "mm10_no_alt_analysis_set_ENCODE.fasta.tar"}}
            # step1 Parameters
            parameters = {}
            parameters["chip.gensz"] = org
            if paired == 'single':
                frag_temp = [300]
                fraglist = frag_temp * len(exp_files)
                parameters['chip.fraglen'] = fraglist
                parameters['chip.paired_end'] = False
            elif paired == 'paired':
                parameters['chip.paired_end'] = True

            # run step1 for control
            if control:
                # control run on tf mode
                input_files = {'chip.ctl_fastqs': [exp_files]}
                control_parameters = {
                    "chip.pipeline_type": 'tf',
                    "chip.choose_ctl.always_use_pooled_ctl": True,
                    "chip.bam2ta_ctl.regex_grep_v_ta": "chr[MUE]|random|alt",
                    "chip.bwa_ctl.cpu": 8,
                    "chip.merge_fastq_ctl.cpu": 8,
                    "chip.filter_ctl.cpu": 8,
                    "chip.bam2ta_ctl.cpu": 8,
                    "chip.align_only": True
                }
                parameters.update(control_parameters)

                s1c_input_files = input_files
                s1c_tag = exp_id
                keep, step1c_status, step1c_output = wfr_utils.stepper(library, keep,
                                                                       'step1c', s1c_tag, exp_files,
                                                                       s1c_input_files, step1c_name, 'chip.first_ta_ctl',
                                                                       additional_input={'parameters': parameters})
                if step1c_status == 'complete':
                    # accumulate files to patch on experiment
                    patch_data = [step1c_output]
                    complete['patch_opf'].append([exp_id, patch_data])
                else:
                    # don't patch anything if at least one exp is still missing
                    ready_for_step2 = False
                print('step1c')
                print(step1c_status, step1c_output)

            # run step1
            else:
                input_files = {'chip.fastqs': [exp_files]}
                exp_parameters = {
                    "chip.pipeline_type": target_type,
                    "chip.choose_ctl.always_use_pooled_ctl": True,
                    "chip.bam2ta.regex_grep_v_ta": "chr[MUE]|random|alt",
                    "chip.bwa.cpu": 8,
                    "chip.merge_fastq.cpu": 8,
                    "chip.filter.cpu": 8,
                    "chip.bam2ta.cpu": 8,
                    "chip.xcor.cpu": 8,
                    "chip.align_only": True
                }
                parameters.update(exp_parameters)

                s1_input_files = input_files
                s1_tag = exp_id
                # if complete, step1_output will have a list of 2 files, first_ta, and fist_ta_xcor
                keep, step1_status, step1_output = wfr_utils.stepper(library, keep,
                                                                     'step1', s1_tag, exp_files,
                                                                     s1_input_files, step1_name, ['chip.first_ta', 'chip.first_ta_xcor'],
                                                                     additional_input={'parameters': parameters})
                if step1_status == 'complete':
                    exp_ta_file = step1_output[0]
                    exp_taxcor_file = step1_output[1]
                    # accumulate files to patch on experiment
                    patch_data = [exp_ta_file, ]
                    complete['patch_opf'].append([exp_id, patch_data])
                    ta.append(exp_ta_file)
                    taxcor.append(exp_taxcor_file)
                else:
                    # don't patch anything if at least one exp is still missing
                    ready_for_step2 = False
                print('step1')
                print(step1_status, step1_output)
        # back to set level
        all_completed = True
        # is step0 step1 complete
        if ready_for_step2:
            # for control, add tag to set, and files to experiments
            if control:
                complete['add_tag'] = [set_acc, tag]
            # for non controls we have to run find control files and runs step2
            else:


            break



        print()

        final_status = set_acc  # start the reporting with acc
        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']

        done = False

        if done:
            final_status += ' completed'
            # completed = [a_sample['accession'], {'files': result_fastq_files}]
            # print('COMPLETED', result_fastq_files)
        else:
            if missing_run:
                final_status += ' |Missing: ' + " ".join([i[0] for i in missing_run])
            if running:
                final_status += ' |Running: ' + " ".join([i[0] for i in running])
            if problematic_run:
                final_status += ' |Problem: ' + " ".join([i[0] for i in problematic_run])

        # add dictionaries to main ones
        check.brief_output.append(final_status)
        print(final_status)
        if running:
            check.full_output['running_runs'].append({set_acc: running})
        if missing_run:
            check.full_output['needs_runs'].append({set_acc: missing_run})
        if problematic_run:
            check.full_output['problematic_runs'].append({set_acc: problematic_run})
        # if made it till the end
        if done:
            assert not running
            assert not problematic_run
            assert not missing_run
            # check.full_output['completed_runs'].append(completed)
        break

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
def chipseq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'chipseq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    chipseq_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = chipseq_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = chipseq_check_result.get('completed_runs')
    action = cgap_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start)
    return action
