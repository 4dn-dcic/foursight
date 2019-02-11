from dcicutils import ff_utils
from datetime import datetime


def get_wfr_out(emb_file, wfr_name, auth, md_qc=False, run=100):
    """For a given file, fetches the status of last wfr_name
    If there is a successful run, it will return the output files as a dictionary of
    file_format:file_id, else, will return the status. Some runs, like qc and md5,
    does not have any file_format output, so they will simply return status 'complete'
    """
    workflows = emb_file.get('workflow_run_inputs')
    wfr = {}
    run_status = 'did not run'

    # some files don't have rev link to runs, for them, we check the md5 runs
    if not workflows:
        
        if wfr_name == 'md5':
            wfrs_url = '/search/?type=WorkflowRun&type=WorkflowRun&workflow.title=md5&input_files.value.accession=' + a_file['accession']
            workflows = ff_utils.search_metadata(wfrs_url, key=auth)

    # add run time to wfr
    if workflows:
        for a_wfr in workflows:
            wfr_type, time_info = a_wfr['display_title'].split(' run ')
            # user submitted ones use run on insteand of run
            time_info = time_info.strip('on').strip()
            try:
                wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S.%f')
            # if the file was created at the percect moment
            except ValueError:
                wfr_time = datetime.strptime(time_info, '%Y-%m-%d %H:%M:%S')
            a_wfr['run_hours'] = (datetime.utcnow() - wfr_time).total_seconds() / 3600
            a_wfr['run_type'] = wfr_type.strip()
        # sort wfrs
        workflows = sorted(workflows, key=lambda k: (k['run_type'], -k['run_hours']))
        # exlude extra files md5 runs
        workflows = [i for i in workflows if not i['input_files'][0].get('format_if_extra')]

    try:
        last_wfr = [i for i in workflows if i['run_type'] == wfr_name][-1]
    except (KeyError, IndexError, TypeError):
        return {'status': "no workflow in file"}

    wfr = ff_utils.get_metadata(last_wfr['uuid'], key=auth)
    run_duration = last_wfr['run_hours']
    run_status = wfr['run_status']

    if run_status == 'complete':
        outputs = wfr.get('output_files')
        # some runs, like qc, don't have a real file output
        if md_qc:
            return {'status': 'complete'}
        # if expected output files, return a dictionary of file_type:file_id
        else:
            out_files = {}
            for output in outputs:
                if output.get('format'):
                    # with new file format objects, we need to parse the name
                    try:  # the new expected file format
                        f_format = output['format'].split('/')[2]
                    except IndexError:  # the old format
                        f_format = output['format']
                    out_files[f_format] = output['value']['@id']
            if out_files:
                out_files['status'] = 'complete'
                return out_files
            else:
                print('no output file was found, maybe this run is a qc?')
                return {'status': "no file found"}
    elif run_status != 'error' and run_duration < run:
        # print(run_duration)
        return {'status': "running"}
    else:
        return {'status': "no completed run"}


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


# Step Settings
def step_settings(step_name, my_organism, attribution, at=True):
    """Return a setting dict for given step, and modify variables in
    output files; genome assembly, file_type, desc, contributing lab."""
    genome = ""
    mapper = {'human': 'GRCh38', 'mouse': 'GRCm38', 'fruit-fly': 'dm6', 'chicken': 'galGal5'}
    genome = mapper.get(my_organism)

    out_n = "This is an output file of the Hi-C processing pipeline"
    int_n = "This is an intermediate file in the HiC processing pipeline"
    out_n_rep = "This is an output file of the RepliSeq processing pipeline"
    # int_n_rep = "This is an intermediate file in the Repliseq processing pipeline"

    wf_dict = [{
        'wf_name': 'md5',
        'wf_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6',
        'parameters': {}
    },
        {
        'wf_name': 'fastqc-0-11-4-1',
        'wf_uuid': '2324ad76-ff37-4157-8bcc-3ce72b7dace9',
        'parameters': {}
    },
        {
        'wf_name': 'bwa-mem',
        'wf_uuid': '3feedadc-50f9-4bb4-919b-09a8b731d0cc',
        'parameters': {"nThreads": 16},
        'custom_pf_fields': {
            'out_bam': {
                'genome_assembly': genome,
                'file_type': 'intermediate file',
                'description': int_n}
        }},
        {
        'wf_name': 'hi-c-processing-bam',
        'wf_uuid': '023bfb3e-9a8b-42b9-a9d4-216079526f68',
        'parameters': {"nthreads_merge": 16, "nthreads_parse_sort": 16},
        'custom_pf_fields': {
            'annotated_bam': {
                'genome_assembly': genome,
                'file_type': 'alignment',
                'description': out_n},
            'filtered_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-replicate',
                'description': out_n}
        }},
        {
        'wf_name': 'hi-c-processing-pairs',
        'wf_uuid': 'c9e0e6f7-b0ed-4a42-9466-cadc2dd84df0',
        'parameters': {"nthreads": 1, "maxmem": "32g"},
        'custom_pf_fields': {
            'cooler_normvector': {
                'genome_assembly': genome,
                'file_type': 'juicebox norm vector',
                'description': out_n},
            'hic': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': out_n}
        }},
        {
        'wf_name': 'hi-c-processing-pairs-nore',
        'wf_uuid': 'c19ee11e-9d5a-454f-af50-600a0cf990b6',
        'parameters': {"nthreads": 1, "maxmem": "32g"},
        'custom_pf_fields': {
            'cooler_normvector': {
                'genome_assembly': genome,
                'file_type': 'juicebox norm vector',
                'description': out_n},
            'hic': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': out_n}
        }},
        {
        'wf_name': 'hi-c-processing-pairs-nonorm',
        'wf_uuid': 'bd6e25ea-f368-4758-a821-d30e0b5a4100',
        'parameters': {"nthreads": 1, "maxmem": "32g"},
        'custom_pf_fields': {
            'hic': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': out_n}
        }},
        {
        'wf_name': 'hi-c-processing-pairs-nore-nonorm',
        'wf_uuid': '05b62bba-7bfa-46cc-8d8e-3d37f4feb8bd',
        'parameters': {"nthreads": 1, "maxmem": "32g"},
        'custom_pf_fields': {
            'hic': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': out_n},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': out_n}
        }},
        {
        'wf_name': 'repliseq-parta',
        'wf_uuid': '4459a4d8-1bd8-4b6a-b2cc-2506f4270a34',
        "parameters": {"nthreads": 4, "memperthread": "2G"},
        'custom_pf_fields': {
            'filtered_sorted_deduped_bam': {
                'genome_assembly': genome,
                'file_type': 'alignment',
                'description': out_n_rep},
            'count_bg': {
                'genome_assembly': genome,
                'file_type': 'counts',
                'description': 'read counts per 5 kb bin, unfiltered, unnormalized'}
        }},
        {
        "wf_name": "bedGraphToBigWig",
        "wf_uuid": "667b14a7-a47e-4857-adf1-12a6393c4b8e",
        "parameters": {},
        "config": {
            "instance_type": "t2.micro",
            "EBS_optimized": False,
            "ebs_size": 10,
            "ebs_type": "gp2",
            "json_bucket": "4dn-aws-pipeline-run-json",
            "ebs_iops": "",
            "shutdown_min": "now",
            "password": "",
            "log_bucket": "tibanna-output",
            "key_name": "4dn-encode"
        },
        "overwrite_input_extra": False
    },
        {"wf_name": "encode-chipseq",
         "wf_uuid": "5b44ce1b-0347-40a6-bc9c-f39fb5d7bce3",
         "parameters": {},
         "config": {
             "ebs_size": 0,
             "ebs_type": "gp2",
             "json_bucket": "4dn-aws-pipeline-run-json",
             "EBS_optimized": False,
             "ebs_iops": "",
             "shutdown_min": "now",
             "instance_type": "",
             "key_name": "4dn-encode",
             "password": "",
             "log_bucket": "tibanna-output"
         },
         'custom_pf_fields': {
             'chip.sig_fc': {
                 'genome_assembly': genome,
                 'file_type': 'intensity values',
                 'description': 'ChIP-seq signal fold change over control input'},
             'chip.peak_calls': {
                 'genome_assembly': genome,
                 'file_type': 'peaks',
                 'description': 'ChIP-seq peak calls'},
             'chip.qc_json': {
                 'genome_assembly': genome,
                 'file_type': 'qc',
                 'description': 'ChIP-seq QC json'}
         }
         },
        {"wf_name": "encode-atacseq",
         "wf_uuid": "6fb021e9-858c-4561-8ce1-e0adc673e0b5",
         "parameters": {},
         "config": {
             "ebs_size": 0,
             "ebs_type": "gp2",
             "json_bucket": "4dn-aws-pipeline-run-json",
             "EBS_optimized": True,
             "ebs_iops": "",
             "shutdown_min": "now",
             "instance_type": "c4.4xlarge",
             "key_name": "4dn-encode",
             "password": "",
             "log_bucket": "tibanna-output"
         },
         'custom_pf_fields': {
             'atac.sig_fc': {
                 'genome_assembly': genome,
                 'file_type': 'intensity values',
                 'description': 'ATAC-seq signal fold change over control input'},
             'atac.peak_calls': {
                 'genome_assembly': genome,
                 'file_type': 'peaks',
                 'description': 'ATAC-seq peak calls'},
             'atac.qc_json': {
                 'genome_assembly': genome,
                 'file_type': 'qc',
                 'description': 'ATAC-seq QC json'}
         }
         }
    ]

    template = [i for i in wf_dict if i['wf_name'] == step_name][0]

    if template.get('custom_pf_fields'):
        for a_file in template['custom_pf_fields']:
            template['custom_pf_fields'][a_file].update(attribution)
    template['wfr_meta'] = attribution
    return template
