import json
from dcicutils import ff_utils, s3Utils
from datetime import datetime
from operator import itemgetter
from . import wfrset_utils

lambda_limit = 260


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
    # OFFICIAL
    'in situ Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # OFFICIAL
    'dilution Hi-C': ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # OFFICIAL
    'TCC':           ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # OFFICIAL  # NO-RE
    'DNase Hi-C':    ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # OFFICIAL  # NO-NORM
    'capture Hi-C':  ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE
    'micro-C':       ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE NO-NORM
    'ChIA-PET':      ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-RE NO-NORM
    'TrAC-loop':     ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # Preliminary - Released to network  # NO-NORM
    'PLAC-seq':      ["HiC_Pipeline_0.2.6", "HiC_Pipeline_0.2.6_skipped-small-set", "HiC_Pipeline_0.2.7"],
    # bwa mem # handled manually for now
    'MARGI': [''],
    # Preliminary - Released to network
    'TSA-seq':       ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    # OFFICIAL - 1 STEP
    'Repli-seq':     ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    # Preliminary - Released to network
    'NAD-seq':       ['RepliSeq_Pipeline_v13.1_step1 ', 'RepliSeq_Pipeline_v14_step1', 'RepliSeq_Pipeline_v16_step1'],
    # OFFICIAL
    'ATAC-seq':      ['ENCODE_ATAC_Pipeline_1.1.1'],
    # OFFICIAL
    'ChIP-seq':      ['ENCODE_CHIP_Pipeline_1.1.1'],
    'single cell Repli-seq': [''],
    'cryomilling TCC': [''],
    'single cell Hi-C': [''],
    'sci-Hi-C': [''],
    'MC-3C': [''],
    'MC-Hi-C': [''],
    'Hi-ChIP': [''],
    'DAM-ID seq': [''],
    'RNA-seq': [''],
    'DNA SPRITE': [''],
    'RNA-DNA SPRITE': [''],
    'GAM': [''],
    'CUT&RUN': [''],
    'TRIP': [''],
    }

# Reference Files
bwa_index = {"human": "4DNFIZQZ39L9",
             "mouse": "4DNFI823LSI8",
             "fruit-fly": '4DNFIO5MGY32',
             "chicken": "4DNFIVGRYVQF"}

chr_size = {"human": "4DNFI823LSII",
            "mouse": "4DNFI3UBJ3HZ",
            "fruit-fly": '4DNFIBEEN92C',
            "chicken": "4DNFIQFZW4DX"}

re_nz = {"human": {'MboI': '/files-reference/4DNFI823L812/',
                   'DpnII': '/files-reference/4DNFIBNAPW3O/',
                   'HindIII': '/files-reference/4DNFI823MBKE/',
                   'NcoI': '/files-reference/4DNFI3HVU2OD/'
                   },
         "mouse": {'MboI': '/files-reference/4DNFIONK4G14/',
                   'DpnII': '/files-reference/4DNFI3HVC1SE/',
                   "HindIII": '/files-reference/4DNFI6V32T9J/'
                   },
         "fruit-fly": {'MboI': '/files-reference/4DNFIS1ZVUWO/'
                       },
         "chicken": {"HindIII": '/files-reference/4DNFITPCJFWJ/'
                     }
         }

# max_distance for species (used for pairsqc)
max_size = {"human": None,
            "mouse": 8.2,
            "fruit-fly": 7.5,
            "chicken": 8.2}

# Restriction enzyme recognition site length`
re_nz_sizes = {"HindIII": "6",
               "DpnII": "4",
               "MboI": "4",
               "NcoI": "6"}

mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5'}

def extract_nz_chr(acc, auth):
    """Get RE nz recognition site length and chrsize file accession
    used for pairsqc."""
    exp_resp = ff_utils.get_metadata(acc, key=auth)
    exp_type = exp_resp.get('experiment_type')
    # get enzyme
    nz_num = ""
    nz = exp_resp.get('digestion_enzyme')
    if nz:
        nz_num = re_nz_sizes.get(nz['display_title'])
    if nz_num:
        pass
    # Use 6 for Chiapet and all without nz (Soo & Burak)
    elif exp_type in ['CHIA-pet', 'ChIA-PET', 'micro-C', 'DNase Hi-C', 'TrAC-loop']:
        nz_num = '6'
    else:
        return (None, None, 'No enzyme or accepted exp type')
    # get organism
    biosample = exp_resp['biosample']
    organisms = list(set([bs['individual']['organism']['name'] for bs in biosample['biosource']]))
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
        if len(same_type_wfrs) > 2:
            return {'status': "no complete run, too many errors"}

        return {'status': "no complete run, errrored"}
    # if other statuses, started running
    elif run_duration < run:
        return {'status': "running"}
    # this should be the timeout case
    else:
        return {'status': "no completed run, timout"}


def get_attribution(file_json):
    """give file response in embedded frame and extract attribution info"""
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
    try:
        e = ff_utils.post_metadata(input_json, 'WorkflowRun/run', key=auth)
        url = json.loads(e['input'])['_tibanna']['url']
        return url
    except Exception as e:
        return str(e)


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
    # add date
    s_date = kwargs.get('start_date')
    if s_date:
        pre_query += '&date_created.from=' + s_date
    # add lab
    lab = kwargs.get('lab_title')
    if lab:
        pre_query += '&lab.display_title=' + lab
    return pre_query


def find_fastq_info(my_rep_set, fastq_files, exclude_miseq=True):
    """Find fastq files from experiment set, exclude miseq by default
    expects my_rep_set to be set response in frame object (search result)
    will check if files are paired or not, and if paired will give list of lists for each exp
    if not paired, with just give list of files per experiment.

    result is 2 dictionaries
    - file dict  { exp1 : [file1, file2, file3, file4]}  # unpaired
      file dict  { exp1 : [ [file1, file2], [file3, file4]]} # paired
    - refs keys  {pairing, organism, enzyme, bwa_ref, chrsize_ref, enz_ref, f_size, lab}
    """
    file_dict = {}
    refs = {}
    # check pairing for the first file, and assume all same
    paired = ""
    rep_resp = my_rep_set['experiments_in_set']
    enzymes = []
    organisms = []
    total_f_size = 0
    for exp in rep_resp:
        exp_resp = exp
        file_dict[exp['accession']] = []
        if not organisms:
            biosample = exp['biosample']
            organisms = list(set([bs['individual']['organism']['name'] for bs in biosample['biosource']]))
            assert len(organisms) == 1
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
            # exclude miseq
            if exclude_miseq:
                if file_resp.get('instrument') == 'Illumina MiSeq':
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
                relations = file_resp['related_files']
                paired_files = [relation['file']['@id'] for relation in relations
                                if relation['relationship_type'] == 'paired with']
                assert len(paired_files) == 1
                f2 = paired_files[0]
                file_dict[exp_resp['accession']].append((f1, f2))
    # get the organism
    if len(list(set(organisms))) == 1:
        organism = organisms[0]
    else:
        organism = None

    # get the enzyme
    if len(list(set(enzymes))) == 1:
        enz = enzymes[0]
    else:
        enz = None

    bwa = bwa_index.get(organism)
    chrsize = chr_size.get(organism)
    if re_nz.get(organism):
        enz_file = re_nz[organism].get(enz)
    else:
        enz_file = None

    f_size = int(total_f_size / (1024 * 1024 * 1024))
    refs = {'pairing': paired,
            'organism': organism,
            'enzyme': enz,
            'bwa_ref': bwa,
            'chrsize_ref': chrsize,
            'enz_ref': enz_file,
            'f_size': str(f_size)+'GB'}
    return file_dict, refs


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
        set_summary = " - ".join([set_acc, refs['organism'], refs['enzyme'], refs['f_size']])
        # if no files were found
        if all(not value for value in exp_files.values()):
            set_summary += "| skipped - no usable file"
            check.brief_output.append(set_summary)
            check.full_output['skipped'].append({set_acc: 'skipped - no usable file'})
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
                elif step1_result['status'] == "no complete run, too many errors":
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
            elif step2_result['status'] == 'no complete run, too many errors':
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
            if missing_run:
                set_summary += "| missing step 1/2"
            elif running:
                set_summary += "| running step 1/2"
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
            assert len(list(set(all_step3s))) == 1
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
            elif step3_result['status'] == 'no complete run, too many errors':
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
                missing_run.append(['step3', ['hi-c-processing-pairs', refs['organism'], overwrite], inp_f, set_acc])
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
        check.summary += str(len(check.full_output['problematic_runs'])) + ' with repeated fail|'
        check.status = 'WARN'
    return check


def patch_complete_data(patch_data, pipeline_type, auth, move_to_pc=False):
    """If move to pc is set to true, if the exp_set or exp status is not released/to project
    it will move the files to processed_files"""
    titles = {"hic": "HiC Processing Pipeline - Preliminary Files",
              "repliseq": "Repli-Seq Pipeline - Preliminary Files",
              'chip': "ENCODE ChIP-Seq Pipeline - Preliminary Files",
              'atac': "ENCODE ATAC-Seq Pipeline - Preliminary Files"}
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
            if ex_pc_ids:
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


def start_missing_hic_run(run_info, auth, env):
    attr_keys = ['fastq1', 'input_pairs', 'input_bams']
    run_settings = run_info[1]
    inputs = run_info[2]
    name_tag = run_info[3]
    # find file to use for attribution
    for attr_key in attr_keys:
        if attr_key in inputs:
            attr_file = inputs[attr_key]
            if isinstance(attr_file, list):
                attr_file = attr_file[0]
            break
    attributions = get_attribution(ff_utils.get_metadata(attr_file, auth))
    settings = wfrset_utils.step_settings(run_settings[0], run_settings[1], attributions, run_settings[2])
    url = run_missing_wfr(settings, inputs, name_tag, auth, env)
    return url


def start_hic_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False):
    started_runs = 0
    patched_md = 0
    action.description = ""
    action_log = {'started_runs': [], 'failed_runs': [], 'patched_meta': [], 'failed_meta': []}
    if missing_runs:
        for a_case in missing_runs:
            now = datetime.utcnow()

            print((now-start).seconds)

            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break
            acc = list(a_case.keys())[0]
            for a_run in a_case[acc]:
                started_runs += 1
                url = start_missing_hic_run(a_run, my_auth, my_env)
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
            patched_md += 1
            error = patch_complete_data(a_completed_info, 'hic', my_auth, move_to_pc=move_to_pc)
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
