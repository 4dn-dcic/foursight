from dcicutils import ff_utils, s3Utils
from datetime import datetime
from operator import itemgetter
from . import wfrset_cgap_utils
import json
lambda_limit = wfrset_cgap_utils.lambda_limit

# check at the end
# check extract_file_info has 4 arguments

# wfr_name, accepted versions, expected run time
workflow_details = {
    "md5": {
        "run_time": 12,
        "accepted_versions": ["0.0.4", "0.2.6"]
    },
    "fastqc-0-11-4-1": {
        "run_time": 50,
        "accepted_versions": ["0.2.0"]
    },
    "workflow_bwa-mem_no_unzip-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_add-readgroups-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_merge-bam-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_picard-MarkDuplicates-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_sort-bam-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_gatk-BaseRecalibrator": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_gatk-ApplyBQSR-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11"]
    },
    "workflow_index-sorted-bam": {
        "run_time": 12,
        "accepted_versions": ["v9"]
    },
    'workflow_gatk-HaplotypeCaller': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11"]
    },
    'workflow_gatk-GenotypeGVCFs-check': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11"]
    },
    "workflow_qcboard-bam": {
        "run_time": 12,
        "accepted_versions": ["v9"]
    },
}


# accepted versions for completed pipelines
accepted_versions = {
    'WGS':  ["WGS_Pipeline_V8"]
    }

# Reference Files
bwa_index = {'human': 'GAPFI4U1HXIY'}

# chr_size = {"human": "4DNFI823LSII",
#             "mouse": "4DNFI3UBJ3HZ",
#             "fruit-fly": '4DNFIBEEN92C',
#             "chicken": "4DNFIQFZW4DX"}


def stepper(all_files, all_wfrs, running, problematic_run, missing_run,
            step_tag, sample_tag, new_step_input_file,
            input_file_dict,  new_step_name, new_step_output_arg,
            additional_input, organism, no_output=False):
    step_output = ''
    # Lets get the repoinse from one of the input files that will be used in this step
    # if it is a list take the first item, if not use it as is
    # new_step_input_file must be the @id
    if isinstance(new_step_input_file, list) or isinstance(new_step_input_file, tuple):
        input_resp = [i for i in all_files if i['@id'] == new_step_input_file[0]][0]
        name_tag = '_'.join([i.split('/')[2] for i in new_step_input_file])
    else:
        input_resp = [i for i in all_files if i['@id'] == new_step_input_file][0]
        name_tag = new_step_input_file.split('/')[2]
    if no_output:
        step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=all_wfrs, md_qc=True)
    else:
        step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=all_wfrs)
    step_status = step_result['status']
    # if successful
    if step_status == 'complete':
        if new_step_output_arg:
            step_output = step_result[new_step_output_arg]
        pass
    # if still running
    elif step_status == 'running':
        running.append([step_tag, sample_tag])
    # if run is not successful
    elif step_status.startswith("no complete run, too many"):
        problematic_run.append([step_tag, sample_tag])
    else:
        # add step 4
        missing_run.append([step_tag, [new_step_name, organism, additional_input], input_file_dict, name_tag])

    return running, problematic_run, missing_run, step_status, step_output


def get_wfr_out(emb_file, wfr_name, key=None, all_wfrs=None, versions=None, md_qc=False, run=None):
    """For a given file, fetches the status of last wfr (of wfr_name type)
    If there is a successful run, it will return the output files as a dictionary of
    argument_name:file_id, else, will return the status. Some runs, like qc and md5,
    does not have any file_format output, so they will simply return 'complete'
    args:
     emb_file: embedded frame file info
     wfr_name: base name without version
     key: authorization
     all_wfrs : all releated wfrs in embedded frame
     versions: acceptable versions for wfr
     md_qc: if no output file is excepted, set to True
     run: if run is still running beyond this hour limit, assume problem
    """
    error_at_failed_runs = 2
    # you should provide key or all_wfrs
    # assert key or all_wfrs
    assert wfr_name in workflow_details
    # get default accepted versions if not provided
    if not versions:
        versions = workflow_details[wfr_name]['accepted_versions']
    # get default run out time
    if not run:
        run = workflow_details[wfr_name]['run_time']
    workflows = emb_file.get('workflow_run_inputs')
    wfr = {}
    run_status = 'did not run'

    my_workflows = [i for i in workflows if i['display_title'].startswith(wfr_name)]
    if not my_workflows:
        return {'status': "no workflow on file"}

    for a_wfr in my_workflows:
        wfr_type, time_info = a_wfr['display_title'].split(' run ')
        wfr_type_base, wfr_version = wfr_type.strip().split(' ')
        # user submitted ones use run on insteand of run
        time_info = time_info.strip('on').strip()
        try:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S')
        a_wfr['run_hours'] = (datetime.utcnow() - wfr_time).total_seconds() / 3600
        a_wfr['run_type'] = wfr_type_base.strip()
        a_wfr['run_version'] = wfr_version.strip()
    my_workflows = [i for i in my_workflows if i['run_version'] in versions]
    if not my_workflows:
        return {'status': "no workflow in file with accepted version"}
    my_workflows = sorted(my_workflows, key=lambda k: k['run_hours'])
    same_type_wfrs = [i for i in my_workflows if i['run_type'] == wfr_name]
    last_wfr = same_type_wfrs[0]
    # get metadata for the last wfr
    if all_wfrs:
        wfr = [i for i in all_wfrs if i['uuid'] == last_wfr['uuid']][0]
    else:
        wfr = ff_utils.get_metadata(last_wfr['uuid'], key)
    run_duration = last_wfr['run_hours']
    run_status = wfr['run_status']

    if run_status == 'complete':
        outputs = wfr.get('output_files')
        # some runs, like qc, don't have a real file output
        if md_qc:
            return {'status': 'complete'}
        # if expected output files, return a dictionary of argname:file_id
        else:
            out_files = {}
            for output in outputs:
                if output.get('format'):
                    # get the arg name
                    arg_name = output['workflow_argument_name']
                    out_files[arg_name] = output['value']['@id']
            if out_files:
                out_files['status'] = 'complete'
                return out_files
            else:
                return {'status': "no file found"}
    # if status is error
    elif run_status == 'error':
        # are there too many failed runs
        if len(same_type_wfrs) >= error_at_failed_runs:
            return {'status': "no complete run, too many errors"}

        return {'status': "no complete run, errrored"}
    # if other statuses, started running
    elif run_duration < run:
        return {'status': "running"}
    # this should be the timeout case
    else:
        if len(same_type_wfrs) >= error_at_failed_runs:
            return {'status': "no complete run, too many time-outs"}
        else:
            return {'status': "no completed run, time-out"}


def get_attribution(file_json):
    """give file response in embedded frame and extract attribution info"""
    attributions = {
        'project': file_json['project']['@id'],
        'institution': file_json['institution']['@id']
    }
    return attributions


def extract_file_info(obj_id, arg_name, auth, env, rename=[]):
    """Takes file id, and creates info dict for tibanna"""
    my_s3_util = s3Utils(env=env)
    raw_bucket = my_s3_util.raw_file_bucket
    out_bucket = my_s3_util.outfile_bucket
    """Creates the formatted dictionary for files.
    """
    # start a dictionary
    template = {"workflow_argument_name": arg_name}
    if rename:
        change_from = rename[0]
        change_to = rename[1]
    # if it is list of items, change the structure
    if isinstance(obj_id, list):
        object_key = []
        uuid = []
        buckets = []
        for obj in obj_id:
            metadata = ff_utils.get_metadata(obj, key=auth)
            object_key.append(metadata['display_title'])
            uuid.append(metadata['uuid'])
            # get the bucket
            if 'FileProcessed' in metadata['@type']:
                my_bucket = out_bucket
            else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                my_bucket = raw_bucket
            buckets.append(my_bucket)
        # check bucket consistency
        assert len(list(set(buckets))) == 1
        template['object_key'] = object_key
        template['uuid'] = uuid
        template['bucket_name'] = buckets[0]
        if rename:
            template['rename'] = [i.replace(change_from, change_to) for i in template['object_key']]

    # if obj_id is a string
    else:
        metadata = ff_utils.get_metadata(obj_id, key=auth)
        template['object_key'] = metadata['display_title']
        template['uuid'] = metadata['uuid']
        # get the bucket
        if 'FileProcessed' in metadata['@type']:
            my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
            my_bucket = raw_bucket
        template['bucket_name'] = my_bucket
        if rename:
            template['rename'] = template['object_key'].replace(change_from, change_to)
    return template


def start_missing_run(run_info, auth, env):
    attr_keys = ['fastq1', 'fastq', 'input_pairs', 'input_bams', 'fastq_R1', 'input_bam', 'input_gvcf']
    run_settings = run_info[1]
    inputs = run_info[2]
    name_tag = run_info[3]
    # find file to use for attribution
    # if there is a single input, use that one
    if len(inputs) == 1:
        for i in inputs:
            attr_file = inputs[i]
            if isinstance(attr_file, list):
                attr_file = attr_file[0]
            break
    else:
        for attr_key in attr_keys:
            if attr_key in inputs:
                attr_file = inputs[attr_key]
                if isinstance(attr_file, list):
                    attr_file = attr_file[0]
                break
    # use pony_dev

    attributions = get_attribution(ff_utils.get_metadata(attr_file, auth))
    settings = wfrset_cgap_utils.step_settings(run_settings[0], run_settings[1], attributions, run_settings[2])
    url = run_missing_wfr(settings, inputs, name_tag, auth, env)
    return url


def run_missing_wfr(input_json, input_files, run_name, auth, env):
    all_inputs = []
    for arg, files in input_files.items():
        inp = extract_file_info(files, arg, auth, env)
        all_inputs.append(inp)
    # tweak to get bg2bw working
    all_inputs = sorted(all_inputs, key=itemgetter('workflow_argument_name'))
    my_s3_util = s3Utils(env=env)
    out_bucket = my_s3_util.outfile_bucket
    """Creates the trigger json that is used by foufront endpoint.
    """
    input_json['input_files'] = all_inputs
    input_json['output_bucket'] = out_bucket
    input_json["_tibanna"] = {
        "env": env,
        "run_type": input_json['app_name'],
        "run_id": run_name}
    # input_json['env_name'] = 'fourfront-cgap'
    input_json['step_function_name'] = 'tibanna_zebra'
    input_json['public_postrun_json'] = True
    try:
        e = ff_utils.post_metadata(input_json, 'WorkflowRun/run', key=auth)
        url = json.loads(e['input'])['_tibanna']['url']
        return url
    except Exception as e:
        return str(e)


def check_runs_without_output(res, check, run_name, my_auth, start):
    """Common processing for checks that are running on files and not producing output files
    like qcs ones producing extra files"""
    # no successful run
    missing_run = []
    # successful run but no expected metadata change (qc or extra file)
    missing_meta_changes = []
    # still running
    running = []
    # multiple failed runs
    problems = []

    for a_file in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
            break
        file_id = a_file['accession']
        report = get_wfr_out(a_file, run_name,  key=my_auth, md_qc=True)
        if report['status'] == 'running':
            running.append(file_id)
        elif report['status'].startswith("no complete run, too many"):
            problems.append(file_id)
        elif report['status'] != 'complete':
            missing_run.append(file_id)
        # There is a successful run, but no extra_file
        elif report['status'] == 'complete':
            missing_meta_changes.append(file_id)
    if running:
        check.summary = 'Some files are running'
        check.brief_output.append(str(len(running)) + ' files are still running.')
        check.full_output['running'] = running
    if problems:
        check.summary = 'Some files have problems'
        check.brief_output.append(str(len(problems)) + ' files have multiple failed runs')
        check.full_output['problems'] = problems
        check.status = 'WARN'
    if missing_run:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_run)) + ' files lack a successful run')
        check.full_output['files_without_run'] = missing_run
        check.status = 'WARN'
    if missing_meta_changes:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_meta_changes)) + ' files have successful run but no qc/extra file')
        check.full_output['files_without_changes'] = missing_meta_changes
        check.status = 'WARN'
    check.summary = check.summary.strip()
    if not check.brief_output:
        check.brief_output = ['All Good!']
    return check


def find_fastq_info(exp, fastq_files):
    """Find fastq files from experiment set, exclude miseq by default
    expects my_rep_set to be set response in frame object (search result)
    will check if files are paired or not, and if paired will give list of lists for each exp
    if not paired, with just give list of files per experiment.

    result is 2 dictionaries
    - file dict  { exp1 : [file1, file2, file3, file4]}  # unpaired
      file dict  { exp1 : [ [file1, file2], [file3, file4]]} # paired
    - refs keys  {pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size, lab}
    """
    files = []
    refs = {}
    # check pairing for the first file, and assume all same
    paired = ""
    total_f_size = 0
    organism = 'human'
    exp_files = exp['files']

    for fastq_file in exp_files:
        file_resp = [i for i in fastq_files if i['uuid'] == fastq_file['uuid']][0]
        if file_resp.get('file_size'):
            total_f_size += file_resp['file_size']
        # skip pair no 2
        if file_resp.get('paired_end') == '2':
            continue
        # check that file has a pair
        f1 = file_resp['@id']
        f2 = ""
        # assign pairing info by the first file
        if not paired:
            try:
                relations = file_resp['related_files']
                paired_files = [relation['file']['@id'] for relation in relations
                                if relation['relationship_type'] == 'paired with']
                assert len(paired_files) == 1
                paired = "Yes"
            except:
                paired = "No"

        if paired == 'No':
            files.append(f1)
        elif paired == 'Yes':
            relations = file_resp['related_files']
            paired_files = [relation['file']['@id'] for relation in relations
                            if relation['relationship_type'] == 'paired with']
            assert len(paired_files) == 1
            f2 = paired_files[0]
            files.append((f1, f2))
    bwa = bwa_index.get(organism)
    # chrsize = chr_size.get(organism)

    f_size = int(total_f_size / (1024 * 1024 * 1024))
    refs = {'pairing': paired,
            'organism': organism,
            'bwa_ref': bwa,
            # 'chrsize_ref': chrsize,
            'f_size': str(f_size)+'GB'}
    return files, refs


def start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False, runtype='hic'):
    started_runs = 0
    patched_md = 0
    action.description = ""
    action_log = {'started_runs': [], 'failed_runs': [], 'patched_meta': [], 'failed_meta': []}
    if missing_runs:
        for a_case in missing_runs:
            now = datetime.utcnow()
            acc = list(a_case.keys())[0]
            print((now-start).seconds, acc)
            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break

            for a_run in a_case[acc]:
                started_runs += 1
                url = start_missing_run(a_run, my_auth, my_env)
                log_message = acc + ' started running ' + a_run[0] + ' with ' + a_run[3]
                if url.startswith('http'):
                    action_log['started_runs'].append([log_message, url])
                else:
                    action_log['failed_runs'].append([log_message, url])
    if patch_meta:
        action_log['patched_meta'] = []
        for a_completed_info in patch_meta:
            exp_acc = a_completed_info[0]
            patch_body = a_completed_info[1]
            now = datetime.utcnow()
            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break
            patched_md += 1
            ff_utils.patch_metadata(patch_body, exp_acc, my_auth)
            action_log['patched_meta'].append(exp_acc)

    # did we complete without running into time limit
    for k in action_log:
        if action_log[k]:
            add_desc = "| {}: {} ".format(k, str(len(action_log[k])))
            action.description += add_desc

    action.output = action_log
    action.status = 'DONE'
    return action


def is_there_my_qc_metric(file_meta, qc_metric_name, my_auth):
    if not file_meta.get('quality_metric'):
        return False

    qc_results = ff_utils.get_metadata(file_meta['quality_metric']['uuid'], key=my_auth)

    if qc_results['display_title'].startswith('QualityMetricQclist'):
        if not qc_results.get('qc_list'):
            return False

        for qc in qc_results['qc_list']:
            if qc_metric_name not in qc['value']['display_title']:
                return False
    else:
        if qc_metric_name not in qc_results['display_title']:
            return False
    return True
