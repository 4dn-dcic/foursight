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
    if not res:
        check.summary = 'All Good!'
        return check

    step0_name = 'merge-fastq'
    step1_name = 'encode-chipseq-aln-chip'
    step1c_name = 'encode-chipseq-aln-ctl'
    step2_name = 'encode-chipseq-postaln'

    for a_set in res:
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
        set_acc = a_set['accession']

        # some feature to extract from each set
        control = ""  # True or False (True if set is control)
        control_set = ""  # None if there are no control experiments or if the set is control
        target_type = ""  # Histone or TF (or None for control)
        paired = ""  # single or paired
        organism = ""
        replicate_exps = a_set['replicate_exps']
        replicate_exps = sorted(replicate_exps, key=lambda x: [x['bio_rep_no'], x['tec_rep_no']])
        # get organism, target and control from the first replicate
        f_exp = replicate_exps[0]['replicate_exp']['uuid']
        f_exp_resp = [i for i in all_items['experiment_seq'] if i['uuid'] == f_exp][0]
        control, control_set, target_type, organism = wfr_utils.get_chip_info(f_exp_resp, all_items)

        print('ORG:', organism, "CONT:", control, "TARGET:", target_type, "CONT_SET:", control_set)

        # sanity checks
        set_summary = " - ".join([set_acc, organism, target_type, control])
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
