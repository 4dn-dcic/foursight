from dcicutils import ff_utils
from dcicutils import s3Utils
from datetime import datetime
from operator import itemgetter
import json

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
    "bwa-mem": {
        "run_time": 50,
        "accepted_versions": ["0.2.6"]
    },
    "pairsqc-single": {
        "run_time": 100,
        "accepted_versions": ["0.2.5", "0.2.6"]
    },
    "hi-c-processing-bam": {
        "run_time": 50,
        "accepted_versions": ["0.2.6"]
    },
    "hi-c-processing-pairs": {
        "run_time": 200,
        "accepted_versions": ["0.2.6", "0.2.7"]
    },
    "hi-c-processing-pairs-nore": {
        "run_time": 200,
        "accepted_versions": ["0.2.6"]
    },
    "hi-c-processing-pairs-nonorm": {
        "run_time": 200,
        "accepted_versions": ["0.2.6"]
    },
    "hi-c-processing-pairs-nore-nonorm": {
        "run_time": 200,
        "accepted_versions": ["0.2.6"]
    },
    "repliseq-parta": {
        "run_time": 200,
        "accepted_versions": ["v13.1", "v14", "v16"]
    },
    "bedGraphToBigWig": {
        "run_time": 24,
        "accepted_versions": ["v4"]
    },
    "bedtobeddb": {
        "run_time": 24,
        "accepted_versions": ["v2"]
    },
    "encode-chipseq-aln-chip": {
        "run_time": 200,
        "accepted_versions": ["1.1.1"]
    },
    "encode-chipseq-aln-ctl": {
        "run_time": 200,
        "accepted_versions": ["1.1.1"]
    },
    "encode-chipseq-postaln": {
        "run_time": 200,
        "accepted_versions": ["1.1.1"]
    },
    "encode-atacseq-aln": {
        "run_time": 200,
        "accepted_versions": ["1.1.1"]
    },
    "encode-atacseq-postaln": {
        "run_time": 200,
        "accepted_versions": ["1.1.1"]
    },
    "mergebed": {
        "run_time": 200,
        "accepted_versions": ["v1"]
    }
}

# accepted versions for completed pipelines
accepted_versions = {
    'in situ Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'dilution Hi-C': ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'micro-C':       ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'DNase Hi-C':    ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'capture Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'ChIA-PET':      ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    'TSA-seq':       ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    'Repli-seq':     ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    'NAD-seq':       ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    'ATAC-seq':      ['ENCODE_ATAC_Pipeline_1.1.1'],


    'ChIP-seq': ['ENCODE_CHIP_Pipeline_1.1.1'],
    'single cell Repli-seq': [''],
    'cryomilling TCC': [''],
    'single cell Hi-C': [''],
    'sci-Hi-C': [''],
    'MC-3C': [''],
    'MC-Hi-C': [''],
    'ChIA-PET': [''],
    'PLAC-seq': [''],
    'Hi-ChIP': [''],
    'DAM-ID seq': [''],

    'RNA-seq': [''],
    'DNA SPRITE': [''],
    'RNA-DNA SPRITE': [''],
    'MARGI': [''],
    'GAM': [''],
    'CUT&RUN': [''],
    'TrAC-loop': [''],
    'TRIP': [''],
    'TCC': [''],
    }


def get_wfr_out(file_id, wfr_name, auth, versions=[], md_qc=False, run=None):
    """For a given file, fetches the status of last wfr (of wfr_name type)
    If there is a successful run, it will return the output files as a dictionary of
    argument_name:file_id, else, will return the status. Some runs, like qc and md5,
    does not have any file_format output, so they will simply return 'complete'
    args:
     file_id: accession/uuid/alias of file
     wfr_name: base name without version
     auth: connection ff_keys
     versions: acceptable versions for wfr
     md_qc: if no output file is excepted, set to True
     run: if run is still running beyond this hour limit, assume problem
    """
    if wfr_name not in workflow_details:
        assert wfr_name in workflow_details
    # get default accepted versions if not provided
    if not versions:
        versions = workflow_details[wfr_name]['accepted_versions']
    # get default run out time
    if not run:
        run = workflow_details[wfr_name]['run_time']
    emb_file = ff_utils.get_metadata(file_id, key=auth)
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
    last_wfr = [i for i in my_workflows if i['run_type'] == wfr_name][0]
    # get metadata for the last wfr
    wfr = ff_utils.get_metadata(last_wfr['uuid'], key=auth)
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
                print('no output file was found, maybe this run is a qc?')
                return {'status': "no file found"}
    # if status is error
    elif run_status == 'error':
        return {'status': "no complete run, errrored"}
    # if other statuses, started running
    elif run_duration < run:
        return {'status': "running"}
    # this should be the timeout case
    else:
        return {'status': "no completed run, timout"}


def get_attribution(file_json):
    attributions = {
        'lab': file_json['lab']['@id'],
        'award': file_json['award']['@id']
    }
    cont_labs = []
    if file_json.get('contributing_labs'):
        cont_labs = [i['@id'] for i in file_json['contributing_labs']]
    appendFDN = True
    if attributions['lab'] == '/labs/4dn-dcic-lab/':
        appendFDN = False
    if cont_labs:
        if appendFDN:
            cont_labs.append('/labs/4dn-dcic-lab/')
            cont_labs = list(set(cont_labs))
        attributions['contributing_labs'] = cont_labs
    else:
        if appendFDN:
            cont_labs = ['/labs/4dn-dcic-lab/']
            attributions['contributing_labs'] = cont_labs
        else:
            pass
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
        try:
            assert len(list(set(buckets))) == 1
        except AssertionError:
            print('Files from different buckets', obj_id)
            return
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


def run_missing_wfr(input_json, input_files, run_name, auth, env):
    all_inputs = []
    for arg, files in input_files.items():
        print(arg, files)
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
    e = ff_utils.post_metadata(input_json, 'WorkflowRun/run', key=auth)
    url = json.loads(e['input'])['_tibanna']['url']
    return url
