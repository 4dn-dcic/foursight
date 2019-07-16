# Step Settings
lambda_limit = 240
mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5'}

pairs_mapper = {"GRCh38": "hg38",
                "GRCm38": "mm10",
                "dm6": 'dm6',
                "galGal5": "galGal5"}


def step_settings(step_name, my_organism, attribution, overwrite=None):
    """Return a setting dict for given step, and modify variables in
    output files; genome assembly, file_type, desc, contributing lab.
    overwrite is a dictionary, if given will overwrite keys in resulting template
    overwrite = {'config': {"a": "b"},
                 'parameters': {'c': "d"}
                    }
    """
    genome = ""
    genome = mapper.get(my_organism)
    pairs_assembly = pairs_mapper.get(genome)

    out_n = "This is an output file of the Hi-C processing pipeline"
    int_n = "This is an intermediate file in the HiC processing pipeline"
    # int_n_rep = "This is an intermediate file in the Repliseq processing pipeline"

    wf_dict = [{
        'app_name': 'md5',
        'workflow_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6'
    },
        {
        'app_name': 'fastqc-0-11-4-1',
        'workflow_uuid': '2324ad76-ff37-4157-8bcc-3ce72b7dace9'
    },
        {
        'app_name': 'pairsqc-single',
        'workflow_uuid': 'b8c533e0-f8c0-4510-b4a1-ac35158e27c3'
    },
        {
        'app_name': 'bwa-mem',
        'workflow_uuid': '3feedadc-50f9-4bb4-919b-09a8b731d0cc',
        'parameters': {"nThreads": 16},
        'custom_pf_fields': {
            'out_bam': {
                'genome_assembly': genome,
                'file_type': 'intermediate file',
                'description': int_n}
        }},
        {
        'app_name': 'hi-c-processing-bam',
        'workflow_uuid': '023bfb3e-9a8b-42b9-a9d4-216079526f68',
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
        'app_name': 'hi-c-processing-pairs',
        'workflow_uuid': '4dn-dcic-lab:wf-hi-c-processing-pairs-0.2.7',
        'parameters': {"nthreads": 4,
                       "maxmem": "32g",
                       "max_split_cooler": 10,
                       "no_balance": False
                       },
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
        'app_name': 'imargi-processing-fastq',
        'workflow_uuid': '7eedaaa8-4c2e-4c71-9d9a-04f05ab1becf',
        'config': {'mem': 8, 'cpu': 4, 'ebs': '3x', 'EBS_optimized': 'true'},
        'parameters': {"nThreads": 4},
        'custom_pf_fields': {
            'out_bam': {
                'genome_assembly': genome,
                'file_type': 'intermediate file',
                'description': "This is an intermediate file from the MARGI processing pipeline"}
        }},
        {
        'app_name': 'imargi-processing-bam',
        'workflow_uuid': '4918e659-6e6c-444f-93c4-276c0d753537',
        'config': {'mem': 8, 'cpu': 8, 'ebs_size': '3x', 'EBS_optimized': 'true'},
        'parameters': {"nthreads": 8, "assembly": pairs_assembly},
        'custom_pf_fields': {
            'out_qc': {
                'genome_assembly': genome,
                'file_type': 'QC',
                'description': 'This is an output file of the Hi-C processing pipeline'},
            'final_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-replicate',
                'description': 'This is an output file of the Hi-C processing pipeline'}
        }},
        {
        'app_name': 'imargi-processing-pairs',
        'workflow_uuid': 'd3e33c23-7442-4f43-8601-337d2f04980a',
        'config': {'mem': 8, 'cpu': 4, 'ebs': '3x', 'EBS_optimized': 'true'},
        'custom_pf_fields': {
            'out_mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': 'This is an output file of the Hi-C processing pipeline'},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': 'This is an output file of the Hi-C processing pipeline'}
        }},
        {
        'app_name': 'repliseq-parta',
        'workflow_uuid': '4dn-dcic-lab:wf-repliseq-parta-v16',
        "parameters": {"nthreads": 4, "memperthread": "2G"},
        'custom_pf_fields': {
            'filtered_sorted_deduped_bam': {
                'genome_assembly': genome,
                'file_type': 'alignment',
                'description': 'This is an output file of the RepliSeq processing pipeline'},
            'count_bg': {
                'genome_assembly': genome,
                'file_type': 'counts',
                'description': 'read counts, unfiltered, unnormalized'}
        }},
        {
        "app_name": "bedtobeddb",
        'parameters': {"assembly": pairs_assembly},
        "workflow_uuid": "9d575e99-5ffe-4ea4-b74f-ad40f621cd39",
        "overwrite_input_extra": False
        },
        {
        "app_name": "bedGraphToBigWig",
        "workflow_uuid": "667b14a7-a47e-4857-adf1-12a6393c4b8e",
        "overwrite_input_extra": False
        },
        {"app_name": "encode-chipseq",
         "workflow_uuid": "5b44ce1b-0347-40a6-bc9c-f39fb5d7bce3",
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
        {"app_name": "encode-atacseq",
         "workflow_uuid": "6fb021e9-858c-4561-8ce1-e0adc673e0b5",
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
         }},
        {
        "app_name": "mergebed",
        "wf_uuid": "2b10e472-065e-43ed-992c-fccad6417b65",
        "parameters": {"sortv": "0"},
        'custom_pf_fields': {
            'merged_bed': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Merged file, positions of aligned reads in bed format, one line per read mate'}
            }
        },
        {
        "app_name": "insulator-score-caller",
        "wf_uuid": "54a46fe7-cec2-4bfb-ab5f-470320f69fb0",
        "parameters": {"binsize": -1, "windowsize": 100000, "cutoff": 2},
        'custom_pf_fields': {
            'bwfile': {
                'genome_assembly': genome,
                'file_type': 'insulation score - diamond',
                'description': 'Diamond insulation scores from Hi-C Pipeline, called by cooltools.'}
            }
        }
    ]

    template = [i for i in wf_dict if i['app_name'] == step_name][0]
    update_config = {
        "ebs_type": "gp2",
        "spot_instance": True,
        "ebs_iops": "",
        "log_bucket": "tibanna-output",
        "key_name": "4dn-encode",
        "behavior_on_capacity_limit": "retry_without_spot"
        }
    if template.get('config'):
        temp_conf = template['config']
        for a_key in update_config:
            if a_key not in temp_conf:
                temp_conf[a_key] = update_config[a_key]
    else:
        template['config'] = update_config

    if not template.get('parameters'):
        template['parameters'] = {}
    if template.get('custom_pf_fields'):
        for a_file in template['custom_pf_fields']:
            template['custom_pf_fields'][a_file].update(attribution)
    template['wfr_meta'] = attribution
    template['custom_qc_fields'] = attribution
    if overwrite:
        for a_key in overwrite:
            for a_spec in overwrite[a_key]:
                template[a_key][a_spec] = overwrite[a_key][a_spec]
    return template
