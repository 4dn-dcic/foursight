import json
import time
import random
from dcicutils import ff_utils
from dcicutils.s3_utils import s3Utils
from dcicutils.env_utils_legacy import FF_PRODUCTION_IDENTIFIER, FF_STAGING_IDENTIFIER
from datetime import datetime, timezone, timedelta
from operator import itemgetter
from tibanna_4dn.core import API
from . import wfrset_utils

lambda_limit = wfrset_utils.lambda_limit
random_wait = wfrset_utils.random_wait
load_wait = wfrset_utils.load_wait


# wfr_name, accepted versions, expected run time # wfr_name, accepted versions,
workflow_details = {
    "md5": {
        "run_time": 12,
        "accepted_versions": ["0.0.4", "0.2.6"]
    },
    # old workflow run naming, updated on workflows for old ones
    "fastqc-0-11-4-1": {
        "run_time": 50,
        "accepted_versions": ["0.2.0"]
    },
    "fastqc": {
        "run_time": 50,
        "accepted_versions": ["v1", "v2"]
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
        "run_time": 200,
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
        "accepted_versions": ["v4", "v5", "v6"]
    },
    "bedtomultivec": {
        "run_time": 24,
        "accepted_versions": ["v4"]
    },
    "bedtobeddb": {
        "run_time": 24,
        "accepted_versions": ["v2", "v3"]
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
    },
    'imargi-processing-fastq': {
        "run_time": 50,
        "accepted_versions": ["1.1.1_dcic_4"]
    },
    'imargi-processing-bam': {
        "run_time": 50,
        "accepted_versions": ["1.1.1_dcic_4"]
    },
    'imargi-processing-pairs': {
        "run_time": 200,
        "accepted_versions": ["1.1.1_dcic_4"]
    },
    'encode-rnaseq-stranded': {
        "run_time": 200,
        "accepted_versions": ["1.1"]
    },
    'encode-rnaseq-unstranded': {
        "run_time": 200,
        "accepted_versions": ["1.1"]
    },
    'rna-strandedness': {
        "run_time": 200,
        "accepted_versions": ["v2"]
    },
    'bamqc': {
        "run_time": 200,
        "accepted_versions": ["v2", "v3"]
    },
    'fastq-first-line': {
        "run_time": 200,
        "accepted_versions": ["v2"]
    },
    're_checker_workflow': {
        "run_time": 200,
        "accepted_versions": ['v1.1', 'v1.2']
    },
    'mad_qc_workflow': {
        "run_time": 200,
        "accepted_versions": ['1.1_dcic_2']
    },
    'merge-fastq': {
        "run_time": 200,
        "accepted_versions": ['v1']
    },
    'insulation-scores-and-boundaries-caller': {
            "run_time": 200,
            "accepted_versions": ['v1']
    },
    'compartments-caller': {
                "run_time": 200,
                "accepted_versions": ['v1.2']
    },
    'mcoolQC': {
                "run_time": 200,
                "accepted_versions": ['v1']
    }
}


# accepted versions for completed pipelines
accepted_versions = {
    # OFFICIAL
    'in situ Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # OFFICIAL
    'Dilution Hi-C': ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # OFFICIAL
    'TCC':           ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # OFFICIAL  # NO-RE
    'DNase Hi-C':    ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # OFFICIAL  # NO-NORM
    'Capture Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # OFFICIAL  # NO-RE
    'Micro-C':       ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE NO-NORM
    'ChIA-PET':      ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE NO-NORM
    'in situ ChIA-PET': ["HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE NO-NORM
    'TrAC-loop':     ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-NORM
    'PLAC-seq':      ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-NORM
    'HiChIP': ["HiC_Pipeline_0.2.7"],
    # bwa mem # handled manually for now
    'MARGI':         ['MARGI_Pipeline_1.1.1_dcic_4'],
    # Preliminary -  Don't release - (Released to network is pending approval from Belmont lab)
    'TSA-seq':       ['RepliSeq_Pipeline_v13.1_step1',
                      'RepliSeq_Pipeline_v14_step1',
                      'RepliSeq_Pipeline_v16_step1'],
    # OFFICIAL - 1 STEP
    '2-stage Repli-seq': ['RepliSeq_Pipeline_v13.1_step1',
                          'RepliSeq_Pipeline_v14_step1',
                          'RepliSeq_Pipeline_v16_step1'],
    # OFFICIAL - 1 STEP
    'Multi-stage Repli-seq': ['RepliSeq_Pipeline_v13.1_step1',
                              'RepliSeq_Pipeline_v14_step1',
                              'RepliSeq_Pipeline_v16_step1'],
    # Preliminary - Released to network
    'NAD-seq':       ['RepliSeq_Pipeline_v13.1_step1', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    # OFFICIAL
    'ATAC-seq':      ['ENCODE_ATAC_Pipeline_1.1.1'],
    # OFFICIAL
    'ChIP-seq':      ['ENCODE_ChIP_Pipeline_1.1.1'],
    # OFFICIAL
    'RNA-seq': ['ENCODE_RNAseq_Pipeline_1.1'],
    'single cell Repli-seq': [''],
    'cryomilling TCC': [''],
    'single cell Hi-C': [''],
    'sci-Hi-C': [''],
    'MC-3C': [''],
    'MC-Hi-C': [''],
    'DamID-seq': [''],
    'DNA SPRITE': [''],
    'RNA-DNA SPRITE': [''],
    'GAM': [''],
    'CUT&RUN': [''],
    'TRIP': ['']
    }

# Accepted versions for feature calling pipelines
feature_calling_accepted_versions = {
    'insulation_scores_and_boundaries': ["insulation_scores_and_boundaries_v1"],
    'compartments': ["compartments_v1.2"]
}
# Reference Files
bwa_index = {"human": "4DNFIZQZ39L9",
             "mouse": "4DNFI823LSI8",
             "fruit-fly": '4DNFIO5MGY32',
             "chicken": "4DNFIVGRYVQF",
             "zebrafish": "4DNFIUH46PG1"}

chr_size = {"human": "4DNFI823LSII",
            "mouse": "4DNFI3UBJ3HZ",
            "fruit-fly": '4DNFIBEEN92C',
            "chicken": "4DNFIQFZW4DX",
            "zebrafish": "4DNFI5W8CN1M"}

# star index for rna Seq
rna_star_index = {"human": "4DNFI3FCGSW2",
                  "mouse": "4DNFINJIU765"}

# star index for rna Seq
rna_rsem_index = {"human": "4DNFIB4HV398",
                  "mouse": "4DNFI2GFI8KN"}

# chromosome sizes for rna Seq
rna_chr_size = {"human": "4DNFIZJB62D1",
                "mouse": "4DNFIBP173GC"}

# id to gene type for rna Seq
rna_t2g = {"human": "4DNFIHBJI984",
           "mouse": "4DNFINBJ25DT"}

re_nz = {"human": {'MboI': '/files-reference/4DNFI823L812/',
                   'DpnII': '/files-reference/4DNFIBNAPW3O/',
                   'HindIII': '/files-reference/4DNFI823MBKE/',
                   'NcoI': '/files-reference/4DNFI3HVU2OD/',
                   'MspI': '/files-reference/4DNFI2JHR3OI/',
                   'NcoI_MspI_BspHI': '/files-reference/4DNFI6HA6EH9/',
                   'AluI': '/files-reference/4DNFIN4DB5O8/',
                   'DdeI': '/files-reference/4DNFI4YGL4RE/',
                   'DdeI and DpnII': '/files-reference/4DNFIS1FCRRK/',
                   'MseI': '/files-reference/4DNFIMD6BNQ8/',
                   "Arima - A1, A2": '/files-reference/4DNFIU1RS39P/'
                   },
         "mouse": {'MboI': '/files-reference/4DNFIONK4G14/',
                   'DpnII': '/files-reference/4DNFI3HVC1SE/',
                   "HindIII": '/files-reference/4DNFI6V32T9J/',
                   "Arima - A1, A2": '/files-reference/4DNFIE78H3K7/'
                   },
         "fruit-fly": {'MboI': '/files-reference/4DNFIS1ZVUWO/'
                       },
         "chicken": {"HindIII": '/files-reference/4DNFITPCJFWJ/'
                     },
         "zebrafish": {'MboI': '/files-reference/4DNFI6OUDFWL/'
                       }
         }


# re bed files for MARGI pipeline
re_fragment = {"human": {'AluI': '/files-reference/4DNFIL1I5TSP/'}}

# reference beta-actin 21kmer for rna_strandness pipeline
re_kmer = {"human": '/files-reference/4DNFIDMVPFSO/',
           "mouse": '/files-reference/4DNFIAQ4BI8Y'}

# max_distance for species (used for pairsqc)
max_size = {"human": None,
            "mouse": 8.2,
            "fruit-fly": 7.5,
            "chicken": 8.2,
            "zebrafish": 7.9}

# Restriction enzyme recognition site length`
re_nz_sizes = {"HindIII": "6",
               "DpnII": "4",
               "MboI": "4",
               "NcoI": "6",
               "MspI": "4",
               "BspHI": "6",
               "DdeI and DpnII": "4",
               "DdeI": "4",
               "NcoI_MspI_BspHI": "4",  # this is an NZ mix, no of cut sites should be similar to 4 cutter mspI
               "MseI": "4",
               "Arima - A1, A2": "4"  # this is an NZ mix, no of cut sites should be similar to 4 cutter mspI
               }

mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5',
          'zebrafish': 'GRCz11'}

# color map states bed file
states_file_type = {
    'SPIN_states_v1':
        {
            'color_mapper': '/files-reference/4DNFI27WSLAG/',
            'num_states': 9
            }
        }


# GC% content reference files (compartments pipeline)
gc_content_ref = {"human": "/files-reference/4DNFI7MCA4R6/",
                  "mouse": "/files-reference/4DNFIOFXJOUA",
                  'fruit fly': "/files-reference/4DNFID6KQ941",
                  "chicken": "/files-reference/4DNFI19V162N",
                  "zebrafish": "/files-reference/4DNFIHEHIZ3P"}


def check_indexing(check, connection):
    """Checks the indexing queue, if there are items in the queue,
    Modifies the check, and returns it along with a flag that is set to True,
    if no items, returns original checks, and flag False"""
    # wait for random time
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # # TEMPORARILY DISABLE ALL PIPELINE RUNS
    # check.status = 'PASS'  # maybe use warn?
    # check.brief_output = ['Check Temporarily Disabled']
    # check.summary = 'Check Temporarily Disabled'
    # check.full_output = {}
    # return check, True
    # check indexing queue
    env = connection.ff_env
    if env in [FF_PRODUCTION_IDENTIFIER, FF_STAGING_IDENTIFIER]:
        health = ff_utils.get_health_page(ff_env=env)
        env = health['beanstalk_env']  # this is ENV_NAME and needs to match to get the correct queue
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check, True
    else:
        return check, False


# check for a specific tag in a states file
def isthere_states_tag(a_file):
    if a_file.get('tags'):
        for tag in a_file['tags']:
            if tag not in states_file_type:
                return (False, 'unregistered_tag')
            else:
                return (True, '')
    else:
        return (False, 'missing_tag')


def extract_nz_chr(acc, auth):
    """Get RE nz recognition site length and chrsize file accession
    used for pairsqc."""
    exp_resp = ff_utils.get_metadata(acc, key=auth)
    exp_type = exp_resp['experiment_type']['display_title']
    # get enzyme
    nz_num = ""
    nz = exp_resp.get('digestion_enzyme')
    if nz:
        nz_num = re_nz_sizes.get(nz['display_title'])
    if nz_num:
        pass
    # Use 6 for Chiapet and all without nz (Soo & Burak)
    elif exp_type in ['in situ ChIA-PET', 'ChIA-PET', 'Micro-C', 'DNase Hi-C', 'TrAC-loop']:
        nz_num = '6'
    else:
        return (None, None, 'No enzyme or accepted exp type')
    # get organism
    biosample = exp_resp['biosample']
    organisms = list(set([bs['organism']['name'] for bs in biosample['biosource']]))
    chrsize = ''
    if len(organisms) == 1:
        chrsize = chr_size.get(organisms[0])
    else:
        # multiple organism biosample
        return (None, None, 'Biosample contains multiple organism')
    # if organism is not available return empty
    if not chrsize:
        msg = organisms[0] + ' does not have chrsize file'
        return (None, None, msg)
    # organism should be in max size dict
    assert organisms[0] in max_size
    max_distance = max_size.get(organisms[0])
    # return result if both exist
    return nz_num, chrsize, max_distance


def check_qcs_on_files(file_meta, all_qcs):
    """Go over qc related fields, and check for overall quality score."""
    def check_qc(file_accession, resp, failed_qcs_list):
        """format errors and return a errors list."""
        quality_score = resp.get('overall_quality_status', '')
        if quality_score.upper() != 'PASS':
            failed_qcs_list.append([file_accession, resp['display_title'], resp['uuid']])
        return failed_qcs_list

    failed_qcs = []
    if not file_meta.get('quality_metric'):
        return
    qc_result = [i for i in all_qcs if i['@id'] == file_meta['quality_metric']['@id']][0]
    if qc_result['display_title'].startswith('QualityMetricQclist'):
        if not qc_result.get('qc_list'):
            return
        for qc in qc_result['qc_list']:
            qc_resp = [i for i in all_qcs if i['@id'] == qc['@id']][0]
            failed_qcs = check_qc(file_meta['accession'], qc_resp, failed_qcs)
    else:
        failed_qcs = check_qc(file_meta['accession'], qc_result, failed_qcs)
    return failed_qcs


def stepper(library, keep,
            step_tag, sample_tag, new_step_input_file,
            input_file_dict,  new_step_name, new_step_output_arg,
            additional_input={}, organism='human', no_output=False):
    """This functions packs the core of wfr check, for a given workflow and set of
    input files, it will return the status of process on these files.
    It will also check for failed qcs on input files.
    new_step_output_arg= can be str or list, will return str or list of @id for output files with given argument(s)"""
    step_output = ''
    # unpack library
    all_files = library['files']
    all_wfrs = library['wfrs']
    all_qcs = library['qcs']
    # unpack keep
    running = keep['running']
    problematic_run = keep['problematic_run']
    missing_run = keep['missing_run']

    # Lets get the repoinse from one of the input files that will be used in this step
    # if it is a list take the first item, if not use it as is
    # new_step_input_file must be the @id
    # also check for qc status
    qc_errors = []
    if isinstance(new_step_input_file, list) or isinstance(new_step_input_file, tuple):
        file_accs = []
        for an_input in new_step_input_file:
            # for chip seq we need another level of unwrapping
            if isinstance(an_input, list) or isinstance(an_input, tuple):
                for a_nested_input in an_input:
                    file_accs.append(a_nested_input.split('/')[2])
                    input_resp = [i for i in all_files if i['@id'] == a_nested_input][0]
                    errors = check_qcs_on_files(input_resp, all_qcs)
                    if errors:
                        qc_errors.extend(errors)
            else:
                file_accs.append(an_input.split('/')[2])
                input_resp = [i for i in all_files if i['@id'] == an_input][0]
                errors = check_qcs_on_files(input_resp, all_qcs)
                if errors:
                    qc_errors.extend(errors)
        name_tag = '_'.join(file_accs)
    else:
        input_resp = [i for i in all_files if i['@id'] == new_step_input_file][0]
        errors = check_qcs_on_files(input_resp, all_qcs)
        if errors:
            qc_errors.extend(errors)
        name_tag = new_step_input_file.split('/')[2]
    # if there are qc errors, return with qc qc_errors
    if qc_errors:
        problematic_run.append([step_tag + ' input file qc error', qc_errors])
        step_status = "no complete run, qc error"
    # if no qc problem, go on with the run check
    else:
        if no_output:
            step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=all_wfrs, md_qc=True)
        else:
            step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=all_wfrs)
        step_status = step_result['status']
        # if successful
        input_file_accession = input_resp['accession']
        if step_status == 'complete':
            if new_step_output_arg:
                if isinstance(new_step_output_arg, list):
                    output_list = []
                    for an_output_arg in new_step_output_arg:
                        output_list.append(step_result[an_output_arg])
                    step_output = output_list
                else:
                    step_output = step_result[new_step_output_arg]
            pass
        # if still running
        elif step_status == 'running':
            running.append([step_tag, sample_tag, input_file_accession])
        # if run is not successful
        elif step_status.startswith("no complete run, too many"):
            problematic_run.append([step_tag, sample_tag, input_file_accession])
        else:
            # add step 4
            missing_run.append([step_tag, [new_step_name, organism, additional_input], input_file_dict, name_tag])

    keep['running'] = running
    keep['problematic_run'] = problematic_run
    keep['missing_run'] = missing_run
    return keep, step_status, step_output


def get_wfr_out(emb_file, wfr_name, key=None, all_wfrs=None, versions=None,
                md_qc=False, run=None, error_threshold=2):
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
     error_threshold = if there are this many failed runs, don't proceed
    """
    # tag as problematic if problematic runs are this many
    # if there are n failed runs, don't proceed

    error_at_failed_runs = error_threshold
    # you should provide key or all_wfrs
    assert key or all_wfrs
    if wfr_name not in workflow_details:
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
    if not workflows:
        return {'status': "no workflow on file"}
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

    if not same_type_wfrs:
        return {'status': "no workflow on file"}
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
                    try:
                        out_files[arg_name] = output['value']['@id']
                    except KeyError:
                        out_files[arg_name] = None
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
    dciclab = '/labs/4dn-dcic-lab/'
    attributions = {
        'lab': dciclab,
        'award': '/awards/2U01CA200059-06/'
    }
    assert file_json.get('lab').get('@id')  # assume this is not really necessary
    file_lab = file_json['lab']['@id']
    cont_labs = []
    if file_json.get('contributing_labs'):
        cont_labs = [i['@id'] for i in file_json['contributing_labs']]
    if file_lab != dciclab and file_lab not in cont_labs:
        cont_labs.append(file_lab)
    if cont_labs:
        attributions['contributing_labs'] = cont_labs
    return attributions


def extract_file_info(obj_id, arg_name, additional_parameters, auth, env, rename=[]):
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
    # with chip seq, files might be wrapped in 3 levels of nesting
    # ToDo: an iterative unwrapper would simplify the code here
    if isinstance(obj_id, list):
        object_key = []
        uuid = []
        buckets = []
        print('1', obj_id)
        for obj in obj_id:
            print('2', obj)
            if isinstance(obj, (list, tuple)):
                nested_object_key = []
                nested_uuid = []
                for nested_obj in obj:
                    print('3', nested_obj)
                    if isinstance(nested_obj, (list, tuple)):
                        nested_nested_object_key = []
                        nested_nested_uuid = []
                        for nested_nested_obj in nested_obj:
                            print('4', nested_nested_obj)
                            metadata = ff_utils.get_metadata(nested_nested_obj, key=auth)
                            nested_nested_object_key.append(metadata['display_title'])
                            nested_nested_uuid.append(metadata['uuid'])
                            # get the bucket
                            if 'FileProcessed' in metadata['@type']:
                                my_bucket = out_bucket
                            else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                                my_bucket = raw_bucket
                            buckets.append(my_bucket)
                        nested_object_key.append(nested_nested_object_key)
                        nested_uuid.append(nested_nested_uuid)
                    else:
                        metadata = ff_utils.get_metadata(nested_obj, key=auth)
                        nested_object_key.append(metadata['display_title'])
                        nested_uuid.append(metadata['uuid'])
                        # get the bucket
                        if 'FileProcessed' in metadata['@type']:
                            my_bucket = out_bucket
                        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
                            my_bucket = raw_bucket
                        buckets.append(my_bucket)
                object_key.append(nested_object_key)
                uuid.append(nested_uuid)
            else:
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
        template['uuid'] = uuid
        if rename:
            template['rename'] = [i.replace(change_from, change_to) for i in object_key]
        if additional_parameters:
            template.update(additional_parameters)

    # if obj_id is a string
    else:
        metadata = ff_utils.get_metadata(obj_id, key=auth)
        template['uuid'] = metadata['uuid']
        # get the bucket
        if 'FileProcessed' in metadata['@type']:
            my_bucket = out_bucket
        else:  # covers cases of FileFastq, FileReference, FileMicroscopy
            my_bucket = raw_bucket
        if rename:
            template['rename'] = object_key.replace(change_from, change_to)
        if additional_parameters:
            template.update(additional_parameters)
    return template


def build_exp_type_query(exp_type, kwargs):
    assert exp_type in accepted_versions
    statuses = ['pre-release', 'released', 'released to project']
    versions = accepted_versions[exp_type]
    # Build the query
    pre_query = "/search/?experimentset_type=replicate&type=ExperimentSetReplicate"
    pre_query += "&experiments_in_set.experiment_type={}".format(exp_type)
    pre_query += "".join(["&status=" + i for i in statuses])
    # for some cases we don't have a defined complete processing tag
    if versions:
        pre_query += "".join(["&completed_processes!=" + i for i in versions])

    # skip non processable experiment sets
    pre_query += "&tags!=skip_processing"
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        pre_query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        pre_query += '&lab.display_title=' + lab
    return pre_query


def build_feature_calling_query(exp_types, feature, kwargs):
    assert feature in feature_calling_accepted_versions

    for exp_type in exp_types:
        assert exp_type in accepted_versions

    statuses = ['pre-release', 'released', 'released to project', 'uploaded']
    versions = [i for i in accepted_versions[exp_type]]
    feature_calling_versions = feature_calling_accepted_versions[feature]
    # Build the query
    pre_query = "/search/?experimentset_type=replicate&type=ExperimentSetReplicate"
    pre_query += "".join(["&experiments_in_set.experiment_type=" + i for i in exp_types])
    pre_query += "".join(["&status=" + i for i in statuses])
    # for some cases we don't have a defined complete processing tag
    if versions:
        pre_query += "".join(["&completed_processes=" + i for i in versions])

    if feature_calling_versions:
        pre_query += "".join(["&completed_processes!=" + i for i in feature_calling_versions])
    pre_query += "&processed_files.quality_metric.@type=QualityMetricMcool"
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        pre_query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        pre_query += '&lab.display_title=' + lab
    return pre_query


def find_fastq_info(my_rep_set, fastq_files, type=None):
    """Find fastq files from experiment set
    expects my_rep_set to be set response in frame object (search result)
    will check if files are paired or not, and if paired will give list of lists for each exp
    if not paired, with just give list of files per experiment.

    result is 2 dictionaries
    - file dict  { exp1 : [file1, file2, file3, file4]}  # unpaired
      file dict  { exp1 : [ [file1, file2], [file3, file4]]} # paired
    - refs keys  {pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size, lab}
    """
    # remove non fastq.gz files from the file list
    fastq_files = [i for i in fastq_files if i['file_format']['file_format'] == 'fastq']
    file_dict = {}
    refs = {}

    # check pairing for the first file, and assume all same

    rep_resp = my_rep_set['experiments_in_set']
    enzymes = []
    organisms = []
    total_f_size = 0
    pairing = []  # collect pairing for each experiment and report if they are consistent or not
    for exp in rep_resp:
        paired = ""
        exp_resp = exp
        file_dict[exp['accession']] = []
        if not organisms:
            biosample = exp['biosample']
            organisms = list(set([bs.get('organism', {}).get('name') for bs in biosample['biosource']]))
        exp_files = exp['files']
        enzyme = exp.get('digestion_enzyme')
        if enzyme:
            enzymes.append(enzyme['display_title'])
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
                file_dict[exp_resp['accession']].append(f1)
            elif paired == 'Yes':
                #
                relations = file_resp['related_files']
                paired_files = [relation['file']['@id'] for relation in relations
                                if relation['relationship_type'] == 'paired with']
                assert len(paired_files) == 1
                f2 = paired_files[0]
                file_dict[exp_resp['accession']].append((f1, f2))
        pairing.append(paired)
    # get the organism
    if len(list(set(organisms))) == 1:
        organism = organisms[0]
    elif len(list(set(organisms))) > 1:
        organism = "multiple organisms"
    else:
        organism = None

    # get the enzyme
    if len(list(set(enzymes))) == 1:
        enz = enzymes[0]
    else:
        enz = ''

    bwa = bwa_index.get(organism)
    chrsize = chr_size.get(organism)

    # if margi, enzyme files are predifined in a separate dict
    if type == 'MARGI':
        if re_fragment.get(organism):
            enz_file = re_fragment[organism].get(enz)
        else:
            enz_file = None
    else:
        # get the enzyme file for organism and enzyme type
        if re_nz.get(organism):
            enz_file = re_nz[organism].get(enz)
        else:
            enz_file = None

    f_size = int(total_f_size / (1024 * 1024 * 1024))
    # check pairing consistency
    if len(set(pairing)) == 1:
        set_pair_status = pairing[0]
    else:
        set_pair_status = 'Inconsistent'
    refs = {'pairing': set_pair_status,
            'organism': organism,
            'enzyme': enz,
            'bwa_ref': bwa,
            'chrsize_ref': chrsize,
            'enz_ref': enz_file,
            'f_size': str(f_size)+'GB'}
    return file_dict, refs


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
        # There is a successful run, but not the expected change (should be part of query)
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


def check_hic(res, my_auth, tag, check, start, lambda_limit, nore=False, nonorm=False):
    """Check run status for each set in res, and report missing runs and completed process"""
    for a_set in res:
        # get all related items
        all_items, all_uuids = ff_utils.expand_es_metadata([a_set['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        now = datetime.utcnow()
        print(a_set['accession'], (now-start).seconds)
        if (now-start).seconds > lambda_limit:
            break
        # missing run
        missing_run = []
        # still running
        running = []
        # problematic cases
        problematic_run = []
        # if all runs are complete, add the patch info for processed files and tag
        complete = {'patch_opf': [],
                    'add_tag': []}
        set_summary = ""
        set_acc = a_set['accession']
        part3 = 'ready'
        # references dict content
        # pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size
        exp_files, refs = find_fastq_info(a_set, all_items['file_fastq'])
        set_summary = " - ".join([set_acc, str(refs['organism']), str(refs['enzyme']), str(refs['f_size'])])
        # if no files were found
        if all(not value for value in exp_files.values()):
            set_summary += "| skipped - no usable file"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no usable file'})
            continue

        # Skip is organism is missing
        if not refs['organism']:
            set_summary += "| skipped - no organism"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no organism'})
            continue
        # skip if more than one organism
        if refs['organism'] == "multiple organisms":
            set_summary += "| skipped - multiple organisms"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - multiple organisms'})
            continue

        # skip if missing reference
        if not refs['bwa_ref'] or not refs['chrsize_ref']:
            set_summary += "| skipped - no chrsize/bwa"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no chrsize/bwa'})
            continue
        # if enzyme is used in the pipeline, check required parameters
        if not nore:
            if not refs['enzyme']:
                set_summary += "| skipped - missing enzyme metadata"
                check.brief_output.append(set_summary)
                check.full_output['skipped'].append({set_acc: 'skipped - missing enzyme metadata'})
                continue
            if not refs['enz_ref']:
                set_summary += "| skipped - no reference NZ file"
                check.brief_output.append(set_summary)
                check.full_output['skipped'].append({set_acc: 'skipped - no reference NZ file'})
                continue
        set_pairs = []
        # cycle through the experiments, skip the ones without usable files
        for exp in exp_files.keys():
            if not exp_files.get(exp):
                continue
            # Check Part 1 and See if all are okay
            exp_bams = []
            part2 = 'ready'
            for pair in exp_files[exp]:
                pair_resp = [i for i in all_items['file_fastq'] if i['@id'] == pair[0]][0]
                step1_result = get_wfr_out(pair_resp, 'bwa-mem', all_wfrs=all_wfrs)
                # if successful
                if step1_result['status'] == 'complete':
                    exp_bams.append(step1_result['out_bam'])
                # if still running
                elif step1_result['status'] == 'running':
                    part2 = 'not ready'
                    running.append(['step1', exp, pair])
                # if run is not successful
                elif step1_result['status'].startswith("no complete run, too many"):
                    part2 = 'not ready'
                    problematic_run.append(['step1', exp, pair])
                else:
                    part2 = 'not ready'
                    # add part 1
                    inp_f = {'fastq1': pair[0], 'fastq2': pair[1], 'bwa_index': refs['bwa_ref']}
                    name_tag = pair[0].split('/')[2]+'_'+pair[1].split('/')[2]
                    missing_run.append(['step1', ['bwa-mem', refs['organism'], {}], inp_f, name_tag])
            # stop progress to part2 and 3
            if part2 is not 'ready':
                part3 = 'not ready'
                # skip part 2 checks
                continue
            # make sure all input bams went through same last step2
            all_step2s = []
            for bam in exp_bams:
                bam_resp = [i for i in all_items['file_processed'] if i['@id'] == bam][0]
                step2_result = get_wfr_out(bam_resp, 'hi-c-processing-bam', all_wfrs=all_wfrs)
                all_step2s.append((step2_result['status'], step2_result.get('annotated_bam')))
            # all bams should have same wfr
            assert len(list(set(all_step2s))) == 1
            # check if part 2 run already
            if step2_result['status'] == 'complete':
                # accumulate pairs files for step3
                set_pairs.append(step2_result['filtered_pairs'])
                # add files for experiment opf
                patch_data = [step2_result['annotated_bam'], step2_result['filtered_pairs']]
                complete['patch_opf'].append([exp, patch_data])
                continue
            # if still running
            elif step2_result['status'] == 'running':
                part3 = 'not ready'
                running.append(['step2', exp])
                continue
            # problematic runs with repeated fails
            elif step2_result['status'].startswith("no complete run, too many"):
                part3 = 'not ready'
                problematic_run.append(['step2', exp])
                continue
            # if run is not successful
            else:
                part3 = 'not ready'
                # Add part2
                inp_f = {'input_bams': exp_bams, 'chromsize': refs['chrsize_ref']}
                missing_run.append(['step2', ['hi-c-processing-bam', refs['organism'], {}], inp_f, exp])
        if part3 is not 'ready':
            if running:
                set_summary += "| running step 1/2"
            elif missing_run:
                set_summary += "| missing step 1/2"
            elif problematic_run:
                set_summary += "| problem in step 1/2"

        if part3 is 'ready':
            # if we made it to this step, there should be files in set_pairs
            assert set_pairs
            # make sure all input bams went through same last step3
            all_step3s = []
            for a_pair in set_pairs:
                a_pair_resp = [i for i in all_items['file_processed'] if i['@id'] == a_pair][0]
                step3_result = get_wfr_out(a_pair_resp, 'hi-c-processing-pairs', all_wfrs=all_wfrs)
                all_step3s.append((step3_result['status'], step3_result.get('mcool')))
            # make sure existing step3s are matching
            if len(list(set(all_step3s))) == 1:
                # if successful
                if step3_result['status'] == 'complete':
                    set_summary += '| completed runs'
                    patch_data = [step3_result['merged_pairs'], step3_result['hic'], step3_result['mcool']]
                    complete['patch_opf'].append([set_acc, patch_data])
                    complete['add_tag'] = [set_acc, tag]
                # if still running
                elif step3_result['status'] == 'running':
                    running.append(['step3', set_acc])
                    set_summary += "| running step3"
                # problematic runs with repeated fails
                elif step3_result['status'].startswith("no complete run, too many"):
                    set_summary += "| problems in step3"
                    problematic_run.append(['step3', set_acc])
                # if run is not successful
                else:
                    set_summary += "| missing step3"
                    inp_f = {'input_pairs': set_pairs,
                             'chromsizes': refs['chrsize_ref']}
                    if not nore:
                        inp_f['restriction_file'] = refs['enz_ref']
                    overwrite = {}
                    if nonorm:
                        overwrite = {'parameters': {"no_balance": True}}
                    missing_run.append(['step3', ['hi-c-processing-pairs', refs['organism'], overwrite],
                                        inp_f, set_acc])
            else:
                problematic_run.append(['step3-not_unique', set_acc, all_step3s])
                set_summary += "| problem in step 3- not unique"
        check.brief_output.append(set_summary)
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


def check_margi(res, my_auth, tag, check, start, lambda_limit, nore=False, nonorm=False):
    """Check run status for each set in res, and report missing runs and completed process"""
    for a_set in res:
        # get all related items
        all_items, all_uuids = ff_utils.expand_es_metadata([a_set['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        now = datetime.utcnow()
        print(a_set['accession'], (now-start).seconds, len(all_uuids))
        if (now-start).seconds > lambda_limit:
            break
        # missing run
        missing_run = []
        # still running
        running = []
        # problematic cases
        problematic_run = []
        # if all runs are complete, add the patch info for processed files and tag
        complete = {'patch_opf': [],
                    'add_tag': []}
        set_summary = ""
        set_acc = a_set['accession']
        part3 = 'ready'
        # references dict content
        # pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size
        exp_files, refs = find_fastq_info(a_set, all_items['file_fastq'], type='MARGI')
        set_summary = " - ".join([set_acc, str(refs['organism']), str(refs['enzyme']), str(refs['f_size'])])
        # if no files were found
        if all(not value for value in exp_files.values()):
            set_summary += "| skipped - no usable file"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no usable file'})
            continue
        # Skip is organism is missing
        if not refs['organism']:
            set_summary += "| skipped - no organism"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no organism'})
            continue
        # skip if more than one organism
        if refs['organism'] == "multiple organisms":
            set_summary += "| skipped - multiple organisms"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - multiple organisms'})
            continue
        # skip if missing reference
        if not refs['bwa_ref'] or not refs['chrsize_ref']:
            set_summary += "| skipped - no chrsize/bwa"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no chrsize/bwa'})
            continue
        if not refs['enz_ref'] and not nore:
            set_summary += "| skipped - no enz"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no enz'})
            continue
        if refs['pairing'] != 'Yes':
            set_summary += "| skipped - unpaired files"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - not paired'})
            continue

        set_pairs = []
        # cycle through the experiments, skip the ones without usable files
        for exp in exp_files.keys():
            # we don't have exp level runs, but we have a patch, hence part 2.5
            part2_5 = 'ready'
            if not exp_files.get(exp):
                continue
            # Check Part 1 and See if all are okay
            exp_pairs = []
            exp_margi_files = []
            for pair in exp_files[exp]:
                part2 = 'ready'
                input_bam = ""
                pair_resp = [i for i in all_items['file_fastq'] if i['@id'] == pair[0]][0]
                step1_result = get_wfr_out(pair_resp, 'imargi-processing-fastq', all_wfrs=all_wfrs)
                # if successful
                if step1_result['status'] == 'complete':
                    input_bam = step1_result['out_bam']
                    exp_margi_files.append(step1_result['out_bam'])
                # if still running
                elif step1_result['status'] == 'running':
                    part2 = 'not ready'
                    running.append(['step1', exp, pair])
                # if run is not successful
                elif step1_result['status'].startswith("no complete run, too many"):
                    part2 = 'not ready'
                    problematic_run.append(['step1', exp, pair])
                else:
                    part2 = 'not ready'
                    # add part 1
                    inp_f = {'fastq_R1': pair[0], 'fastq_R2': pair[1], 'bwa_index': refs['bwa_ref']}
                    name_tag = pair[0].split('/')[2]+'_'+pair[1].split('/')[2]
                    missing_run.append(['step1', ['imargi-processing-fastq', refs['organism'], {}], inp_f, name_tag])

                # going into step2
                if part2 != 'ready':
                    part2_5 = 'not ready'
                    continue

                bam_resp = [i for i in all_items['file_processed'] if i['@id'] == input_bam][0]
                step2_result = get_wfr_out(bam_resp, 'imargi-processing-bam', all_wfrs=all_wfrs)
                # if successful
                if step2_result['status'] == 'complete':
                    exp_margi_files.append(step2_result['out_pairs'])
                    exp_pairs.append(step2_result['out_pairs'])
                # if still running
                elif step2_result['status'] == 'running':
                    part2_5 = 'not ready'
                    running.append(['step2', exp, input_bam])
                # if run is not successful
                elif step2_result['status'].startswith("no complete run, too many"):
                    part2_5 = 'not ready'
                    problematic_run.append(['step2', exp, input_bam])
                else:
                    part2_5 = 'not ready'
                    # add part 1
                    inp_f = {'input_bam': input_bam, 'chromsize': refs['chrsize_ref'],
                             'restrict_frags': refs['enz_ref']}
                    name_tag = input_bam.split('/')[2]
                    missing_run.append(['step2', ['imargi-processing-bam', refs['organism'], {}], inp_f, name_tag])

            if part2_5 != 'ready':
                part3 = 'not ready'
            else:
                # if exps runs were fine, lets patch exp with all pairs produced
                patch_data = exp_margi_files
                complete['patch_opf'].append([exp, patch_data])
                set_pairs.extend(exp_pairs)

                # patch the experiment with exp_pairs
        if part3 is not 'ready':
            if running:
                set_summary += "| running step 1/2"
            elif missing_run:
                set_summary += "| missing step 1/2"
            elif problematic_run:
                set_summary += "| problem in step 1/2"

        if part3 is 'ready':
            # if we made it to this step, there should be files in set_pairs
            assert set_pairs
            # make sure all input bams went through same last step3
            all_step3s = []
            for a_pair in set_pairs:
                a_pair_resp = [i for i in all_items['file_processed'] if i['@id'] == a_pair][0]
                step3_result = get_wfr_out(a_pair_resp, 'imargi-processing-pairs', all_wfrs=all_wfrs)
                all_step3s.append((step3_result['status'], step3_result.get('out_mcool')))
            # make sure existing step3s are matching
            if len(list(set(all_step3s))) == 1:
                # if successful
                if step3_result['status'] == 'complete':
                    set_summary += '| completed runs'
                    patch_data = [step3_result['merged_pairs'], step3_result['out_mcool']]
                    complete['patch_opf'].append([set_acc, patch_data])
                    complete['add_tag'] = [set_acc, tag]
                # if still running
                elif step3_result['status'] == 'running':
                    running.append(['step3', set_acc])
                    set_summary += "| running step3"
                # problematic runs with repeated fails
                elif step3_result['status'].startswith("no complete run, too many"):
                    set_summary += "| problems in step3"
                    problematic_run.append(['step3', set_acc])
                # if run is not successful
                else:
                    set_summary += "| missing step3"
                    inp_f = {'input_pairs': set_pairs}
                    missing_run.append(['step3', ['imargi-processing-pairs', refs['organism'], {}],
                                        inp_f, set_acc])
            else:
                problematic_run.append(['step3-not_unique', set_acc])
                set_summary += "| problem in step 3- not unique"
        check.brief_output.append(set_summary)
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


def patch_complete_data(patch_data, pipeline_type, auth, move_to_pc=False, pc_append=False):
    """Function to update experiment and experiment set metadata for pipeline completions
    and output files.
    Parameters
    ----------
    patch_data: (dict) example format:
                {
                'patch_opf': [
                    ['set_acc', ['file1', 'file2']],
                    ['exp_acc', ['file3', 'file4']]
                    ]
                'add_tag': [['exp_acc', 'completed_pipeline_tag']]
                }
    pipeline_type: (str) key for titles dictionary for setting the opf title
    move_to_pc: (bool) If False, processing results go to other_processed_files field
                If True:
                   If set/exp is released/to project processing results go to other_processed_files field
                   If set/exp is in other status, processing results go to processed_files field
    pc_append: (bool) If True and move_to_pc is True - append outfiles to existing processed files
            This is relevant for pipelines that produce files to be added to datasets upon which a pipeline (eg. Hi-C)
            has already been run - eg. Compartment Caller or Insulation Score/Boundaries
    """
    titles = {"hic": "HiC Processing Pipeline - Preliminary Files",
              "repliseq": "Repli-Seq Pipeline - Preliminary Files",
              'chip': "ENCODE ChIP-Seq Pipeline - Preliminary Files",
              'atac': "ENCODE ATAC-Seq Pipeline - Preliminary Files",
              'margi': "iMARGI Processing Pipeline - Preliminary Files",
              'rnaseq': "ENCODE RNA-Seq Pipeline - Preliminary Files",
              'insulation_scores_and_boundaries': "Insulation scores and boundaries calls - Preliminary Files",
              'compartments': "Compartments Signals - Preliminary Files"}
    """move files to other processed_files field."""
    if not patch_data.get('patch_opf'):
        return ['no content in patch_opf, skipping']
    if not patch_data.get('add_tag'):
        return ['no tag info, skipping']
    pc_set_title = titles[pipeline_type]
    log = []
    for a_case in patch_data['patch_opf']:
        # exp/set acc, and list of files to add
        acc, list_pc = a_case[0], a_case[1]
        resp = ff_utils.get_metadata(acc, auth)
        # check if these items are in existing processed files field
        ex_pc = resp.get('processed_files')
        if ex_pc:
            ex_pc_ids = [i['@id'] for i in ex_pc]
            common = list(set(ex_pc_ids) & set(list_pc))
            if common:
                log.append('some files ({}) are already in processed_files filed for {}'.format(common, acc))
                continue
        # check if these items are in other processed files field
        ex_opc = resp.get('other_processed_files')
        if ex_opc:
            # make sure the title is not already There
            all_existing_titles = [a['title'] for a in ex_opc]
            if pc_set_title in all_existing_titles:
                log.append('opc using same title already exists for {}'.format(acc))
                continue
            ex_opc_ids = [i['@id'] for a in ex_opc for i in a['files']]
            common = list(set(ex_opc_ids) & set(list_pc))
            if common:
                log.append('some files ({}) are already in other_processed_files filed for {}'.format(common, acc))
                continue
        source_status = resp['status']
        # if move_to_pc is set to true, but the source status is released/to project
        # set it back to finalize_user_pending_labs
        if source_status in ['released', 'released to project']:
            move_to_pc = False
        # if move_to_pc is true, add them to processed_files
        if move_to_pc:
            # at this step we expect processed_files field to be empty
            # unless pc_append is True
            if ex_pc:
                if pc_append:
                    ex_pc_cnt = len(ex_pc_ids)
                    ex_pc_ids.extend(list_pc)
                    list_pc = ex_pc_ids
                    if ex_pc_cnt == len(list_pc):
                        # warn if it looks like no existing pfs were added
                        log.append('expected additions to existing processed files: {}'.format(acc))
                        continue
                else:
                    log.append('expected processed_files to be empty: {}'.format(acc))
                    continue
            # patch the processed files field
            ff_utils.patch_metadata({'processed_files': list_pc}, obj_id=acc, key=auth)
        # if not move_to_pc, add files to opf with proper title
        else:
            # we need raw to get the existing piece, to patch back with the new ones
            if ex_opc:
                patch_val = ff_utils.get_metadata(acc, key=auth, add_on='frame=raw').get('other_processed_files', [])
            else:
                patch_val = []

            new_data = {'title': pc_set_title,
                        'type': 'preliminary',
                        'files': list_pc}
            patch_val.append(new_data)
            patch_body = {'other_processed_files': patch_val}
            ff_utils.patch_metadata(patch_body, obj_id=acc, key=auth)
    # add the tag
    set_acc = patch_data['add_tag'][0]
    new_tag = patch_data['add_tag'][1]
    existing_tags = ff_utils.get_metadata(set_acc, auth).get('completed_processes', [])
    new_tags = list(set(existing_tags + [new_tag]))
    ff_utils.patch_metadata({'completed_processes': new_tags}, set_acc, auth)
    return log


def run_missing_wfr(input_json, input_files_and_params, run_name, auth, env, fs_env, mount=False):
    if fs_env == 'staging':
        raise ValueError("'staging' not an expected value for fs_env - pipelines do not run on staging."
                         "please run on data instead.")
    all_inputs = []
    # input_files container
    input_files = {k: v for k, v in input_files_and_params.items() if k != 'additional_file_parameters'}
    # additional input file parameters
    input_file_parameters = input_files_and_params.get('additional_file_parameters', {})
    for arg, files in input_files.items():
        additional_params = input_file_parameters.get(arg, {})
        inp = extract_file_info(files, arg, additional_params, auth, env)
        all_inputs.append(inp)
    # tweak to get bg2bw working
    all_inputs = sorted(all_inputs, key=itemgetter('workflow_argument_name'))
    my_s3_util = s3Utils(env=env)
    out_bucket = my_s3_util.outfile_bucket
    sfn = 'tibanna_pony_' + fs_env
    # shorten long name_tags
    # they get combined with workflow name, and total should be less then 80
    # (even less since repeats need unique names)
    if len(run_name) > 30:
        run_name = run_name[:30] + '...'
    """Creates the trigger json that is used by foufront endpoint.
    """
    input_json['input_files'] = all_inputs
    input_json['output_bucket'] = out_bucket
    input_json["_tibanna"] = {
        "env": env,
        "run_type": input_json['app_name'],
        "run_id": run_name}
    input_json['public_postrun_json'] = True
    input_json['step_function_name'] = sfn
    input_json['env_name'] = env
    if mount:
        for a_file in input_json['input_files']:
            a_file['mount'] = True

    # # testing
    # json_object = json.dumps(input_json, indent=4)
    # print(json_object)
    # return

    # env should be either data, webdev or fourfront-webdev

    try:
        res = API().run_workflow(input_json, sfn=sfn, verbose=False)
        url = res['_tibanna']['url']
        return url
    except Exception as e:
        return str(e)


def start_missing_run(run_info, auth, env, fs_env):
    attr_keys = ['fastq1', 'fastq', 'input_pairs', 'input_bams', 'input_fastqs',
                 'fastq_R1', 'input_bam', 'rna.fastqs_R1', 'mad_qc.quantfiles', 'mcoolfile',
                 'chip.ctl_fastqs', 'chip.fastqs', 'chip.tas', 'atac.fastqs', 'atac.tas']
    run_settings = run_info[1]
    inputs = run_info[2]
    name_tag = run_info[3]
    attr_file = ''
    # find file to use for attribution
    for attr_key in attr_keys:
        if attr_key in inputs:
            attr_file = inputs[attr_key]
            if isinstance(attr_file, list):
                attr_file = attr_file[0]
                if isinstance(attr_file, list):
                    attr_file = attr_file[0]
                    if isinstance(attr_file, list):
                        attr_file = attr_file[0]
                        break
                    else:
                        break
                else:
                    break
            else:
                break
    if not attr_file:
        possible_keys = [i for i in inputs.keys() if i != 'additional_file_parameters']
        error_message = ('one of these argument names {} which carry the input file -not the references-'
                         ' should be added to att_keys dictionary on foursight cgap_utils.py function start_missing_run').format(possible_keys)
        raise ValueError(error_message)
    attributions = get_attribution(ff_utils.get_metadata(attr_file, auth))
    settings = wfrset_utils.step_settings(run_settings[0], run_settings[1], attributions, run_settings[2])
    url = run_missing_wfr(settings, inputs, name_tag, auth, env, fs_env, mount=False)
    return url


def start_tasks(missing_runs, patch_meta, action, my_auth, my_env, fs_env, start, move_to_pc=False, runtype='hic', pc_append=False):
    started_runs = 0
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
                url = start_missing_run(a_run, my_auth, my_env, fs_env)
                log_message = acc + ' started running ' + a_run[0] + ' with ' + a_run[3]
                if url.startswith('http'):
                    action_log['started_runs'].append([log_message, url])
                else:
                    action_log['failed_runs'].append([log_message, url])
    if patch_meta:
        action_log['patched_meta'] = []
        for a_completed_info in patch_meta:
            acc = a_completed_info['add_tag'][0]
            now = datetime.utcnow()
            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break
            error = patch_complete_data(a_completed_info, runtype, my_auth, move_to_pc=move_to_pc, pc_append=pc_append)
            if not error:
                log_message = acc + ' completed processing'
                action_log['patched_meta'].append(log_message)
            else:
                action_log['failed_meta'].append([acc, error])

    # did we complete without running into time limit
    for k in action_log:
        if action_log[k]:
            add_desc = "| {}: {} ".format(k, str(len(action_log[k])))
            action.description += add_desc

    action.output = action_log
    action.status = 'DONE'
    return action


def check_repli(res, my_auth, tag, check, start, lambda_limit, winsize=None):
    """Check run status for each set in res, and report missing runs and completed process"""
    for a_set in res:
        # get all related items
        all_items, all_uuids = ff_utils.expand_es_metadata([a_set['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        now = datetime.utcnow()
        print(a_set['accession'], (now-start).seconds)
        if (now-start).seconds > lambda_limit:
            break
        # missing run
        missing_run = []
        # still running
        running = []
        # problematic cases
        problematic_run = []
        # if all runs are complete, add the patch info for processed files and tag
        complete = {'patch_opf': [],
                    'add_tag': []}
        set_summary = ""
        set_acc = a_set['accession']
        # references dict content
        # pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size
        exp_files, refs = find_fastq_info(a_set, all_items['file_fastq'])
        paired = refs['pairing']
        set_summary = " - ".join([set_acc, str(refs['organism']), str(refs['f_size'])])
        # if no files were found
        if all(not value for value in exp_files.values()):
            set_summary += "| skipped - no usable file"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no usable file'})
            continue
        # Skip is organism is missing
        if not refs['organism']:
            set_summary += "| skipped - no organism"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no organism'})
            continue
        # skip if more than one organism
        if refs['organism'] == "multiple organisms":
            set_summary += "| skipped - multiple organisms"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - multiple organisms'})
            continue
        # skip if missing reference
        if not refs['bwa_ref'] or not refs['chrsize_ref']:
            set_summary += "| skipped - no chrsize/bwa"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no chrsize/bwa'})
            continue
        # cycle through the experiments, skip the ones without usable files
        part3 = 'ready'  # switch for watching the set
        for exp in exp_files.keys():
            if not exp_files.get(exp):
                continue
            # Check Part 1 and See if all are okay
            all_files = []
            part2 = 'ready'  # switch for watching the exp
            for pair in exp_files[exp]:
                if paired == 'Yes':
                    pair_resp = [i for i in all_items['file_fastq'] if i['@id'] == pair[0]][0]
                elif paired == 'No':
                    pair_resp = [i for i in all_items['file_fastq'] if i['@id'] == pair][0]
                step1_result = get_wfr_out(pair_resp, 'repliseq-parta', all_wfrs=all_wfrs)
                # if successful
                if step1_result['status'] == 'complete':
                    all_files.extend([step1_result['filtered_sorted_deduped_bam'],
                                      step1_result['count_bg']])
                # if still running
                elif step1_result['status'] == 'running':
                    part2 = 'not ready'
                    running.append(['step1', exp, pair])
                # if run is not successful
                elif step1_result['status'].startswith("no complete run, too many"):
                    part2 = 'not ready'
                    problematic_run.append(['step1', exp, pair])
                else:
                    part2 = 'not ready'
                    # add part 1
                    if paired == 'Yes':
                        inp_f = {'fastq': pair[0], 'fastq2': pair[1],
                                 'bwaIndex': refs['bwa_ref'],
                                 'chromsizes': refs['chrsize_ref']}
                        name_tag = pair[0].split('/')[2]+'_'+pair[1].split('/')[2]
                    elif paired == 'No':
                        inp_f = {'fastq': pair,
                                 'bwaIndex': refs['bwa_ref'],
                                 'chromsizes': refs['chrsize_ref']}
                        name_tag = pair.split('/')[2]
                    overwrite = {}
                    if winsize:
                        overwrite = {'parameters': {'winsize': winsize}}
                    missing_run.append(['step1', ['repliseq-parta', refs['organism'], overwrite], inp_f, name_tag])
            # are all step1s complete
            if part2 == 'ready':
                # add files for experiment opf
                complete['patch_opf'].append([exp, all_files])
            else:
                part3 = 'not ready'
        if part3 == 'ready':
            # add the tag
            set_summary += "| completed runs"
            complete['add_tag'] = [set_acc, tag]
        else:
            if running:
                set_summary += "| running step 1"
            elif missing_run:
                set_summary += "| missing step 1"
            elif problematic_run:
                set_summary += "| problem in step 1"

        check.brief_output.append(set_summary)
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


def check_rna(res, my_auth, tag, check, start, lambda_limit):
    """Check run status for each set in res, and report missing runs and completed process"""
    for a_set in res:
        # get all related items
        all_items, all_uuids = ff_utils.expand_es_metadata([a_set['uuid']], my_auth,
                                                           store_frame='embedded',
                                                           add_pc_wfr=True,
                                                           ignore_field=['experiment_relation',
                                                                         'biosample_relation',
                                                                         'references',
                                                                         'reference_pubs'])
        all_wfrs = all_items.get('workflow_run_awsem', []) + all_items.get('workflow_run_sbg', [])
        now = datetime.utcnow()
        # print(a_set['accession'], (now-start).seconds)
        if (now-start).seconds > lambda_limit:
            break
        # missing run
        missing_run = []
        # still running
        running = []
        # problematic cases
        problematic_run = []
        # if all runs are complete, add the patch info for processed files and tag
        complete = {'patch_opf': [],
                    'add_tag': []}
        set_summary = ""
        set_acc = a_set['accession']
        final_status = 'ready'
        # references dict content
        # pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size
        exp_files, refs = find_fastq_info(a_set, all_items['file_fastq'])

        print(a_set['accession'], 'paired=', refs['pairing'], refs['organism'], refs['f_size'])
        paired = refs['pairing']
        organism = refs['organism']
        set_summary = " - ".join([set_acc, str(organism), str(refs['f_size'])])

        # if no files were found
        if all(not value for value in exp_files.values()):
            set_summary += "| skipped - no usable file"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no usable file'})
            continue
        # Skip is organism is missing
        if not refs['organism']:
            set_summary += "| skipped - no organism"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no organism'})
            continue
        # skip if more than one organism
        if refs['organism'] == "multiple organisms":
            set_summary += "| skipped - multiple organisms"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - multiple organisms'})
            continue
        if organism not in ['mouse', 'human']:
            msg = 'No reference file for ' + organism
            set_summary += "| " + msg
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: msg})
            continue

        # check  strandedness_verified
        not_verified = []
        for an_exp in exp_files:
            an_exp_resp = [i for i in all_items['experiment_seq'] if i['accession'] == an_exp][0]
            tags = an_exp_resp.get('tags', [])
            if 'strandedness_verified' not in tags:
                not_verified.append(an_exp)
            elif not an_exp_resp.get('strandedness'):
                not_verified.append(an_exp)
        if not_verified:
            msg = ', '.join(not_verified) + ' Not verified for strandedness'
            set_summary += "| " + msg
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: msg})
            continue

        # cycle through the experiments, skip the ones without usable files
        # accumulate files for madqc
        step2_files = []
        step2_status = 'ready'
        for exp in exp_files.keys():
            step1_status = 'ready'
            if not exp_files.get(exp):
                continue

            strand_info = ''
            exp_resp = [i for i in all_items['experiment_seq'] if i['accession'] == exp][0]
            tags = exp_resp.get('tags', [])
            strand_info = exp_resp.get('strandedness')

            # run unstranded pipeline
            app_name = ''
            # set parameters
            pars = {
                'rna.strandedness': '',
                'rna.strandedness_direction': '',
                'rna.endedness': ''
            }
            if strand_info == 'unstranded':
                pars['rna.strandedness'] = 'unstranded'
                pars['rna.strandedness_direction'] = 'unstranded'
                app_name = 'encode-rnaseq-unstranded'
            elif strand_info in ['reverse', 'forward']:
                pars['rna.strandedness'] = 'stranded'
                pars['rna.strandedness_direction'] = strand_info
                app_name = 'encode-rnaseq-stranded'

            # add more parameters and get status report
            input_files = exp_files[exp]
            if paired == 'Yes':
                pars['rna.endedness'] = 'paired'
                input_resp = [i for i in all_items['file_fastq'] if i['@id'] == input_files[0][0]][0]
            elif paired == 'No':
                pars['rna.endedness'] = 'single'
                input_resp = [i for i in all_items['file_fastq'] if i['@id'] == input_files[0]][0]
            step1_result = get_wfr_out(input_resp, app_name, all_wfrs=all_wfrs)

            # if successful
            if step1_result['status'] == 'complete':
                # add  madqc file
                step2_files.append(step1_result['rna.gene_expression'])
                # create processed files list for experiment
                exp_results = []
                for a_type in ['rna.outbam',
                               'rna.plusbw',
                               'rna.minusbw',
                               'rna.outbw',
                               'rna.gene_expression',
                               'rna.isoform_expression']:
                    if a_type in step1_result:
                        exp_results.append(step1_result[a_type])
                complete['patch_opf'].append([exp, exp_results])
            # if still running
            elif step1_result['status'] == 'running':
                step1_status = 'not ready'
                running.append(['step1', exp])
            # if run is not successful
            elif step1_result['status'].startswith("no complete run, too many"):
                step1_status = 'not ready'
                problematic_run.append(['step1', exp])
            # if it is missing
            else:
                step1_status = 'not ready'
                # add part
                name_tag = exp
                inp_f = {'rna.align_index': rna_star_index[organism],
                         'rna.rsem_index': rna_rsem_index[organism],
                         'rna.chrom_sizes': rna_chr_size[organism],
                         'rna.rna_qc_tr_id_to_gene_type_tsv': rna_t2g[organism]}
                if paired == 'Yes':
                    inp_f['rna.fastqs_R1'] = [[i[0] for i in input_files]]
                    inp_f['rna.fastqs_R2'] = [[i[1] for i in input_files]]
                elif paired == 'No':
                    inp_f['rna.fastqs_R1'] = [input_files]
                overwrite = {'parameters': pars}
                missing_run.append(['step1', [app_name, organism, overwrite], inp_f, name_tag])
            if step1_status != 'ready':
                step2_status = 'not ready'

        if step2_status != 'ready':
            if running:
                set_summary += "| running step 1"
            elif missing_run:
                set_summary += "| missing step 1"
            elif problematic_run:
                set_summary += "| problem in step 1"
        # if there is a single replicate, skip madqc
        elif len(step2_files) == 1:
            step2_status = 'ready'
        # run step2 if step1 s are complete
        else:
            step2_input = [i for i in all_items['file_processed'] if i['@id'] == step2_files[0]][0]
            step2_result = get_wfr_out(step2_input, 'mad_qc_workflow', all_wfrs=all_wfrs, md_qc=True)

            # if successful
            if step2_result['status'] == 'complete':
                pass
            # if still running
            elif step2_result['status'] == 'running':
                step2_status = 'not ready'
                running.append(['step2', set_acc])
            # if run is not successful
            elif step2_result['status'].startswith("no complete run, too many"):
                step2_status = 'not ready'
                problematic_run.append(['step2', set_acc])
            # if it is missing
            else:
                step2_status = 'not ready'
                # add part
                name_tag = set_acc
                inp_f = {'mad_qc.quantfiles': step2_files}
                missing_run.append(['step2', ['mad_qc_workflow', organism, {}], inp_f, name_tag])
        if step2_status != 'ready':
            final_status = 'not ready'

        if final_status == 'ready':
            # add the tag
            set_summary += "| completed runs"
            complete['add_tag'] = [set_acc, tag]
        else:
            if running:
                set_summary += "| running step 2"
            elif missing_run:
                set_summary += "| missing step 2"
            elif problematic_run:
                set_summary += "| problem in step 2"

        check.brief_output.append(set_summary)
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


def string_to_list(string):
    "Given a string that is either comma separated values, or a python list, parse to list"
    for a_sep in "'\":[] ":
        values = string.replace(a_sep, ",")
    values = [i.strip() for i in values.split(',') if i]
    return values


def fetch_wfr_associated(wfr_info):
    """Given wfr embedded frame, find associated output files and qcs"""
    wfr_as_list = []
    wfr_as_list.append(wfr_info['uuid'])
    if wfr_info.get('output_files'):
        for o in wfr_info['output_files']:
            if o.get('value'):
                wfr_as_list.append(o['value']['uuid'])
            elif o.get('value_qc'):
                wfr_as_list.append(o['value_qc']['uuid'])
    if wfr_info.get('output_quality_metrics'):
        for qc in wfr_info['output_quality_metrics']:
            if qc.get('value'):
                wfr_as_list.append(qc['value']['uuid'])
    if wfr_info.get('quality_metric'):
        wfr_as_list.append(wfr_info['quality_metric']['uuid'])
    return list(set(wfr_as_list))


def get_chip_info(f_exp_resp, all_items):
    """Gether the following information from the first experiment in the chip set"""
    control = ""  # True or False (True if set in scope is control)
    control_set = ""  # None (if no control exp is set), or the control experiment for the one in scope
    target_type = ""  # Histone or TF (or None for control)
    # get target
    targets = f_exp_resp.get('targeted_factor', [])
    # TODO: tag all control antibodies as control and make use of it here
    if targets:
        # use the tag from the first target, this assumes the rest follows the first one
        target = targets[0]
        target_info = [i for i in all_items['bio_feature'] if i['uuid'] == target['uuid']][0]
        # set to tf default and switch to histone if tagged so
        target_tags = target_info.get('tags', [])
        if not target_tags:
            target_type = None
        elif 'histone' in target_tags:
            target_type = 'histone'
        else:
            target_type = 'tf'
    else:
        target_type = None

    # get organism
    biosample = f_exp_resp['biosample']
    organism = list(set([bs['organism']['name'] for bs in biosample['biosource']]))[0]

    # get control information
    exp_relation = f_exp_resp.get('experiment_relation')
    if exp_relation:
        rel_type = [i['relationship_type'] for i in exp_relation]
        if 'control for' in rel_type:
            control = True
        if 'controlled by' in rel_type:
            control = False
            controls = [i['experiment'] for i in exp_relation if i['relationship_type'] == 'controlled by']
            if len(controls) != 1:
                print('multiple control experiments')
            else:
                cont_exp_resp = [i for i in all_items['experiment_seq'] if i['uuid'] == controls[0]['uuid']][0]
                cont_exp_info = cont_exp_resp['experiment_sets']
                control_set = [i['accession'] for i in cont_exp_info if i['@id'].startswith('/experiment-set-replicates/')][0]
    else:
        # if no relation is present
        # set it as if control when the target is None
        if not target_type:
            control = True
        # if there is target, but no relation, treat it as an experiment without control
        else:
            control = False
            control_set = None
    return control, control_set, target_type, organism


def get_chip_files(exp_resp, all_files):
    files = []
    paired = ""
    exp_files = exp_resp['files']
    for a_file in exp_files:
        f_t = []
        file_resp = [i for i in all_files if i['uuid'] == a_file['uuid']][0]
        # get pair end no
        pair_end = file_resp.get('paired_end')
        if pair_end == '2':
            paired = 'paired'
            continue
        # get paired file
        paired_with = ""
        relations = file_resp.get('related_files')
        if not relations:
            pass
        else:
            for relation in relations:
                if relation['relationship_type'] == 'paired with':
                    paired = 'paired'
                    paired_with = relation['file']['@id']
        # decide if data is not paired end reads
        if not paired_with:
            if not paired:
                paired = 'single'
            else:
                if paired != 'single':
                    print('inconsistent fastq pair info')
                    continue
            f_t.append(file_resp['@id'])
        else:
            f2 = [i for i in all_files if i['@id'] == paired_with][0]
            f_t.append(file_resp['@id'])
            f_t.append(f2['@id'])
        files.append(f_t)
    return files, paired


def select_best_2(file_list, all_files, all_qcs):
    scores = []
    # run it for list with at least 3 elements
    if len(file_list) < 3:
        return(file_list)

    for f in file_list:
        f_resp = [i for i in all_files if i['@id'] == f][0]
        qc = f_resp['quality_metric']
        qc_resp = [i for i in all_qcs if i['uuid'] == qc['uuid']][0]
        try:
            score = qc_resp['nodup_flagstat_qc'][0]['mapped']
        except Exception:
            score = qc_resp['ctl_nodup_flagstat_qc'][0]['mapped']
        scores.append((score, f))
    scores = sorted(scores, key=lambda x: -x[0])
    return [scores[0][1], scores[1][1]]


def limit_number_of_runs(check, my_auth):
    """Checks the number of workflow runs started in the past 6h. Return the
    number of remaining runs before hitting the rate limit of pulls from Docker
    Hub. This is currently 200 every 6 hours."""
    n_runs_max = 180  # this is set below the actual limit, since only a few workflows check this before running
    statuses = ['in review by lab', 'deleted', 'pre-release', 'released to project', 'released']
    query = '/search/?type=WorkflowRunAwsem' + ''.join(['&status=' + s for s in statuses])
    six_h_ago = datetime.now(timezone.utc) - timedelta(hours=6)
    query += '&date_created.from=' + datetime.strftime(six_h_ago, "%Y-%m-%d %H:%M")
    recent_runs = ff_utils.search_metadata(query, key=my_auth)
    n_runs_available = max(n_runs_max - len(recent_runs), 0)  # no negative results
    if n_runs_available == 0:
        check.status = 'PASS'
        check.brief_output = ['Waiting (max 6h) due to Docker Hub rate limit']
        check.summary = f'Limiting the number of workflow runs to {n_runs_max} every 6h'
        check.full_output = {}
    return check, n_runs_available
