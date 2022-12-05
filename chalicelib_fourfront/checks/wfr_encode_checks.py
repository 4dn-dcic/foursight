from datetime import datetime
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


@check_function(lab_title=None, start_date=None, action="chipseq_start")
def chipseq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'chipseq_status')
    my_auth = connection.ff_keys
    check.action = "chipseq_start"
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
                                                           ignore_field=[  # 'experiment_relation',
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
        # have to do another get for control experiments if there is one
        f_exp_resp = [i for i in all_items['experiment_seq'] if i['uuid'] == f_exp][0]
        control, control_set, target_type, organism = wfr_utils.get_chip_info(f_exp_resp, all_items)
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
            control_ready = True
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
                                                                         s0_input_files, step0_name, 'merged_fastq', organism=organism)
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
                # input_files = {'chip.ctl_fastqs': [exp_files]}
                input_files['chip.ctl_fastqs'] = [exp_files]
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
                                                                       additional_input={'parameters': parameters}, organism=organism)
                if step1c_status == 'complete':
                    # accumulate files to patch on experiment
                    patch_data = [step1c_output, ]
                    complete['patch_opf'].append([exp_id, patch_data])
                else:
                    # don't patch anything if at least one exp is still missing
                    ready_for_step2 = False
                print('step1c')
                print(step1c_status, step1c_output)

            # run step1
            else:
                # input_files = {'chip.fastqs': [exp_files]}
                input_files['chip.fastqs'] = [exp_files]
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
                                                                     additional_input={'parameters': parameters}, organism=organism)
                if step1_status == 'complete':
                    exp_ta_file = step1_output[0]
                    exp_taxcor_file = step1_output[1]
                    # accumulate files to patch on experiment
                    patch_data = [exp_ta_file, ]
                    complete['patch_opf'].append([exp_id, patch_data])
                    ta.append(exp_ta_file)
                    taxcor.append(exp_taxcor_file)

                    # find the control file if there is a control set found
                    if control_set:
                        try:
                            exp_cnt_ids = [i['experiment'] for i in exp_resp['experiment_relation'] if i['relationship_type'] == 'controlled by']
                            exp_cnt_ids = [i['@id'] for i in exp_cnt_ids]
                        except:
                            control_ready = False
                            print('Control Relation has problems for this exp', exp_id)
                            continue
                        if len(exp_cnt_ids) != 1:
                            control_ready = False
                            print('Multiple controls for this exp', exp_id)
                            continue
                        exp_cnt_id = exp_cnt_ids[0]
                        print('controled by set', exp_cnt_id)
                        # have to do a get for the control experiment
                        exp_cnt_resp = [i for i in all_items['experiment_seq'] if i['@id'] == exp_cnt_id][0]
                        cont_file = ''
                        # check opf for control file
                        for opf_case in exp_cnt_resp.get('other_processed_files', []):
                            if opf_case['title'] == 'ENCODE ChIP-Seq Pipeline - Preliminary Files':
                                opf_files = opf_case['files']
                                assert len(opf_files) == 1
                                cont_file = opf_files[0]['@id']
                        # if not in opf, check processed files
                        if not cont_file:
                            pf_list = exp_cnt_resp.get('processed_files', [])
                            if pf_list:
                                if pf_list:
                                    assert len(pf_list) == 1
                                    cont_file = pf_list[0]['@id']
                        # did we find it, if so, add it to ta_cnt
                        if cont_file:
                            ta_cnt.append(cont_file)
                        else:
                            control_ready = False

                else:
                    # don't patch anything if at least one exp is still missing
                    ready_for_step2 = False
                print('step1')
                print(step1_status, step1_output, control_ready)
        # back to set level
        final_status = set_acc  # start the reporting with acc
        all_completed = False
        # is step0 step1 complete
        if ready_for_step2 and not control_ready:
            final_status += ' waiting for control experiments to finish processing'
        elif ready_for_step2:
            # for control, add tag to set, and files to experiments
            if control:
                complete['add_tag'] = [set_acc, tag]
            # for non controls check for step2
            else:
                # this only works with 2 experiments, if 3, pick best 2, if more, skip for now
                if len(ta) > 3:
                    set_summary += "| skipped - more then 3 experiments in set, can not process at the moment"
                    check.brief_output.append(set_summary)
                    check.full_output['skipped'].append({set_acc: set_summary})
                    continue
                if len(ta) > 2:
                    ta_2 = []
                    taxcor_2 = []
                    print('ExperimentSet has 3 experiments, selecting best 2')
                    ta_2 = wfr_utils.select_best_2(ta, all_files, all_qcs)
                    # xcor does not have qc, use ta indexes to find the correct files
                    for ta_f in ta_2:
                        taxcor_2.append(taxcor[ta.index(ta_f)])
                    ta = ta_2
                    taxcor = taxcor_2
                    # for control files ,also select best2
                    ta_cnt = wfr_utils.select_best_2(ta_cnt, all_files, all_qcs)

                # collect step2 input files
                s2_input_files = {}
                if organism == 'human':
                    org = 'hs'
                    s2_input_files['chip.blacklist'] = '/files-reference/4DNFIZ1TGJZR/'
                    s2_input_files['chip.chrsz'] = '/files-reference/4DNFIZJB62D1/'
                if organism == 'mouse':
                    org = 'mm'
                    s2_input_files['chip.blacklist'] = '/files-reference/4DNFIZ3FBPK8/'
                    s2_input_files['chip.chrsz'] = '/files-reference/4DNFIBP173GC/'

                def rename_chip(input_at_id_list):
                    # rename bed.gz to tagAlign.gz
                    renamed = []
                    for a_file in input_at_id_list:
                        acc = a_file.split('/')[2]
                        renamed.append(acc + '.tagAlign.gz')
                    return renamed

                s2_input_files['additional_file_parameters'] = {}
                s2_input_files['chip.tas'] = ta
                s2_input_files['additional_file_parameters']['chip.tas'] = {"rename": rename_chip(ta)}
                s2_input_files['chip.bam2ta_no_filt_R1.ta'] = taxcor
                s2_input_files['additional_file_parameters']['chip.bam2ta_no_filt_R1.ta'] = {"rename": rename_chip(taxcor)}
                if ta_cnt:
                    s2_input_files['chip.ctl_tas'] = ta_cnt
                    s2_input_files['additional_file_parameters']['chip.ctl_tas'] = {"rename": rename_chip(ta_cnt)}

                # collect parameters
                parameters = {}
                if paired == 'single':
                    chip_p = False
                elif paired == 'paired':
                    chip_p = True
                if not control_set:
                    if target_type == 'histone':
                        set_summary += "| skipped - histone without control needs attention, ie change to tf"
                        check.brief_output.append(set_summary)
                        check.full_output['skipped'].append({set_acc: set_summary})
                        continue
                run_ids = {'desc': set_acc + a_set.get('description', '')}
                parameters = {
                    "chip.pipeline_type": target_type,
                    "chip.paired_end": chip_p,
                    "chip.choose_ctl.always_use_pooled_ctl": True,
                    "chip.qc_report.desc": run_ids['desc'],
                    "chip.gensz": org,
                    "chip.xcor.cpu": 4,
                }
                if paired == 'single':
                    frag_temp = [300]
                    fraglist = frag_temp * len(ta)
                    parameters['chip.fraglen'] = fraglist

                # if the target is a tf and there is no control, use macs2
                if not control_set:
                    if target_type == 'tf':
                        parameters['chip.peak_caller'] = "macs2"

                s2_tag = set_acc
                # if complete, step1_output will have a list of 2 files, first_ta, and fist_ta_xcor
                keep, step2_status, step2_output = wfr_utils.stepper(library, keep,
                                                                     'step2', s2_tag, ta,
                                                                     s2_input_files, step2_name,
                                                                     ['chip.optimal_peak', 'chip.conservative_peak', 'chip.sig_fc'],
                                                                     additional_input={'parameters': parameters}, organism=organism)
                if step2_status == 'complete':
                    set_opt_peak = step2_output[0]
                    set_cons_peak = step2_output[1]
                    set_sig_fc = step2_output[2]
                    # accumulate files to patch on experiment
                    patch_data = [set_opt_peak, set_cons_peak, set_sig_fc]
                    complete['patch_opf'].append([set_acc, patch_data])
                    complete['add_tag'] = [set_acc, tag]
                    all_completed = True

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']
        if all_completed:
            final_status += ' completed'
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
        if complete.get('add_tag'):
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(complete)

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
    fs_env = connection.fs_env
    chipseq_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = chipseq_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = chipseq_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,  move_to_pc=True, runtype='chip')
    return action


@check_function(lab_title=None, start_date=None, pick_best_2=False, action="atacseq_start")
def atacseq_status(connection, **kwargs):
    """
    Keyword arguments:
    lab_title -- limit search with a lab i.e. Bing+Ren, UCSD
    start_date -- limit search to files generated since a date formatted YYYY-MM-DD
    run_time -- assume runs beyond run_time are dead
    pick_best_2 -- False by default. If set the True, for sets more than 2 experiments,
                   2 best will be used instead of running mergebed
    """
    start = datetime.utcnow()
    check = CheckResult(connection, 'atacseq_status')
    my_auth = connection.ff_keys
    check.action = "atacseq_start"
    check.description = "run missing steps and add processing results to processed files, match set status"
    check.brief_output = []
    check.summary = ""
    check.full_output = {'skipped': [], 'running_runs': [], 'needs_runs': [],
                         'completed_runs': [], 'problematic_runs': []}
    check.status = 'PASS'
    exp_type = 'ATAC-seq'
    # completion tag
    tag = wfr_utils.accepted_versions[exp_type][-1]
    pick_best_2 = kwargs.get('pick_best_2', False)
    # check indexing queue
    check, skip = wfr_utils.check_indexing(check, connection)
    if skip:
        return check
    # Build the query, add date and lab if available
    query = wfr_utils.build_exp_type_query(exp_type, kwargs)
    res = ff_utils.search_metadata(query, key=my_auth)
    print(len(res))

    if not res:
        check.summary = 'All Good!'
        return check
    # run step 0 on all experiments with more than 2 sets of files
    # step1 on each experiment,if multiple exps, merge beds, run step3 on set
    step0_name = 'merge-fastq'
    step1_name = 'encode-atacseq-aln'
    step2_name = 'mergebed'
    step3_name = 'encode-atacseq-postaln'

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

        # some feature to extract from each set
        paired = ""  # single or paired , checked for each experiment
        organism = ""
        replicate_exps = a_set['replicate_exps']
        replicate_exps = sorted(replicate_exps, key=lambda x: [x['bio_rep_no'], x['tec_rep_no']])
        # get organism
        f_exp = replicate_exps[0]['replicate_exp']['uuid']
        # have to do another get for control experiments if there is one
        f_exp_resp = [i for i in all_items['experiment_atacseq'] if i['uuid'] == f_exp][0]
        biosample = f_exp_resp['biosample']
        organism = list(set([bs['organism']['name'] for bs in biosample['biosource']]))[0]
        set_summary = " - ".join([set_acc, str(organism)])
        print(set_summary)
        # sanity checks
        # can only process mouse and human at the moment
        if organism not in ['mouse', 'human']:
            set_summary += "| organism not ready for atac"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: set_summary})
            continue

        # collect results from step1 runs for step2
        ta = []
        # track if all experiments completed step0 and step1
        ready_for_step2 = True
        for an_exp in replicate_exps:
            # track if all experiments completed step0
            ready_for_step1 = True
            exp_id = an_exp['replicate_exp']['accession']
            exp_resp = [i for i in all_items['experiment_atacseq'] if i['accession'] == exp_id][0]
            # exp_files [[pair1,pair2], [pair1, pair2]]
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

            # step1 files
            # references
            input_files = {}
            if organism == 'human':
                org = 'hs'
                input_files['atac.bowtie2_idx_tar'] = '/files-reference/4DNFIMQPTYDY/'
                input_files['atac.blacklist'] = '/files-reference/4DNFIZ1TGJZR/'
                input_files['atac.chrsz'] = '/files-reference/4DNFIZJB62D1/'
                input_files['additional_file_parameters'] = {"atac.bowtie2_idx_tar": {"rename": "GRCh38_no_alt_analysis_set_GCA_000001405.15.fasta.tar"}}
            if organism == 'mouse':
                org = 'mm'
                input_files['atac.bowtie2_idx_tar'] = '/files-reference/4DNFI2493SDN/'
                input_files['atac.blacklist'] = '/files-reference/4DNFIZ3FBPK8/'
                input_files['atac.chrsz'] = '/files-reference/4DNFIBP173GC/'
                input_files['additional_file_parameters'] = {"atac.bowtie2_idx_tar": {"rename": "mm10_no_alt_analysis_set_ENCODE.fasta.tar"}}
            # add input files
            input_files['atac.fastqs'] = [exp_files]
            # step1 Parameters
            parameters = {
                "atac.pipeline_type": 'atac',
                "atac.gensz": org,
                "atac.bam2ta.regex_grep_v_ta": "chr[MUE]|random|alt",
                "atac.disable_ataqc": True,
                "atac.enable_xcor": False,
                "atac.trim_adapter.auto_detect_adapter": True,
                "atac.bowtie2.cpu": 4,
                "atac.filter.cpu": 4,
                "atac.bam2ta.cpu": 4,
                "atac.trim_adapter.cpu": 4,
                "atac.align_only": True
            }
            if paired == 'single':
                frag_temp = [300]
                fraglist = frag_temp * len(exp_files)
                parameters['atac.fraglen'] = fraglist
                parameters['atac.paired_end'] = False
            elif paired == 'paired':
                parameters['atac.paired_end'] = True

            s1_input_files = input_files
            s1_tag = exp_id
            # if complete, step1_output will have a list of 2 files, first_ta, and fist_ta_xcor
            keep, step1_status, step1_output = wfr_utils.stepper(library, keep,
                                                                 'step1', s1_tag, exp_files,
                                                                 s1_input_files, step1_name, 'atac.first_ta',
                                                                 additional_input={'parameters': parameters})
            if step1_status == 'complete':
                # accumulate files to patch on experiment
                patch_data = [step1_output, ]
                complete['patch_opf'].append([exp_id, patch_data])
                ta.append(step1_output)
            else:
                # don't patch anything if at least one exp is still missing
                ready_for_step2 = False
            print('step1', step1_status, step1_output)

        # back to set level
        final_status = set_acc  # start the reporting with acc
        all_completed = False
        # is step0 step1 complete
        if ready_for_step2:
            # Following was the proposed logic, but it is not implemented
            # Currently, for sets with more than 2 experiments, there are 2 options
            # 1) pick best 2,   2) run mergebed (default)

            # Proposed logic
            # if there are more then 2 experiments, check the number of biological replicates
            # if there is 1 Biological Replicate
            # -pick best 2 exp
            # if there are 2 Biological replicates
            #  - run mergebed on bioreps with more then 1 technical replicate
            # if there are 3 Biological replicates
            # - if there are 3 total experiments (1 in each biological rep), pick best 2
            # - else, run mergebed on bioreps with more then 1 technical replicate, and pick best 2 biorep
            # if there are 4 or more Biolofical replicates
            # - run mergebed on bioreps with more then 1 technical replicate, and pick best 2 biorep
            # this only works with 2 experiments, if 3, pick best 2, if more, skip for now
            ready_for_step3 = True
            if len(ta) > 2:
                if pick_best_2:
                    # pick best 2 - False by default
                    print('ExperimentSet has 3 experiments, selecting best 2')
                    ta = wfr_utils.select_best_2(ta, all_files, all_qcs)
                else:
                    # run mergebed - default option
                    s2_input_files = {'input_bed': ta}
                    s2_tag = set_acc
                    # if complete, step1_output will have a list of 2 files, first_ta, and fist_ta_xcor
                    keep, step2_status, step2_output = wfr_utils.stepper(library, keep,
                                                                         'step2', s2_tag, ta,
                                                                         s2_input_files, step2_name, 'merged_bed')
                    if step2_status == 'complete':
                        ta = [step2_output, ]
                    else:
                        ready_for_step3 = False
            if ready_for_step3:
                # collect step3 input files
                s3_input_files = {}
                if organism == 'human':
                    org = 'hs'
                    s3_input_files['atac.blacklist'] = '/files-reference/4DNFIZ1TGJZR/'
                    s3_input_files['atac.chrsz'] = '/files-reference/4DNFIZJB62D1/'
                if organism == 'mouse':
                    org = 'mm'
                    s3_input_files['atac.blacklist'] = '/files-reference/4DNFIZ3FBPK8/'
                    s3_input_files['atac.chrsz'] = '/files-reference/4DNFIBP173GC/'

                def rename_chip(input_at_id_list):
                    # rename bed.gz to tagAlign.gz
                    renamed = []
                    for a_file in input_at_id_list:
                        acc = a_file.split('/')[2]
                        renamed.append(acc + '.tagAlign.gz')
                    return renamed

                s3_input_files['additional_file_parameters'] = {}
                s3_input_files['atac.tas'] = ta
                s3_input_files['additional_file_parameters']['chip.tas'] = {"rename": rename_chip(ta)}
                # collect parameters
                if paired == 'single':
                    chip_p = False
                elif paired == 'paired':
                    chip_p = True
                parameters = {
                    "atac.pipeline_type": 'atac',
                    "atac.paired_end": chip_p,
                    "atac.gensz": org,
                    "atac.disable_ataqc": True,
                    "atac.enable_xcor": False,
                }
                if paired == 'single':
                    frag_temp = [300]
                    fraglist = frag_temp * len(ta)
                    parameters['atac.fraglen'] = fraglist

                s3_tag = set_acc
                # if complete, step1_output will have a list of 2 files, first_ta, and fist_ta_xcor
                keep, step3_status, step3_output = wfr_utils.stepper(library, keep,
                                                                     'step3', s3_tag, ta,
                                                                     s3_input_files, step3_name,
                                                                     ['atac.optimal_peak', 'atac.conservative_peak', 'atac.sig_fc'],
                                                                     additional_input={'parameters': parameters})
                if step3_status == 'complete':
                    set_opt_peak = step3_output[0]
                    set_cons_peak = step3_output[1]
                    set_sig_fc = step3_output[2]
                    # accumulate files to patch on experiment
                    patch_data = [set_opt_peak, set_cons_peak, set_sig_fc]
                    complete['patch_opf'].append([set_acc, patch_data])
                    complete['add_tag'] = [set_acc, tag]
                    all_completed = True

        # unpack results
        missing_run = keep['missing_run']
        running = keep['running']
        problematic_run = keep['problematic_run']
        if all_completed:
            final_status += ' completed'
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
        if complete.get('add_tag'):
            assert not running
            assert not problematic_run
            assert not missing_run
            check.full_output['completed_runs'].append(complete)

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
def atacseq_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    start = datetime.utcnow()
    action = ActionResult(connection, 'atacseq_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    fs_env = connection.fs_env
    atacseq_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    missing_runs = []
    patch_meta = []
    if kwargs.get('start_runs'):
        missing_runs = atacseq_check_result.get('needs_runs')
    if kwargs.get('patch_completed'):
        patch_meta = atacseq_check_result.get('completed_runs')
    action = wfr_utils.start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start,  move_to_pc=True, runtype='atac')
    return action
