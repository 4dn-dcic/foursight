# Step Settings
def step_settings(step_name, my_organism, attribution, overwrite=None):
    """Return a setting dict for given step, and modify variables in
    output files; genome assembly, file_type, desc, contributing lab.
    overwrite is a dictionary, if given will overwrite keys in resulting template
    overwrite = {'config': {"a": "b"},
                 'parameters': {'c': "d"}
                    }
    """
    genome = ""
    mapper = {'human': 'GRCh38',
              'mouse': 'GRCm38',
              'fruit-fly': 'dm6',
              'chicken': 'galGal5'}
    genome = mapper.get(my_organism)

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
        'wf_uuid': '4dn-dcic-lab:wf-hi-c-processing-pairs-0.2.7',
        'parameters': {"nthreads": 4,
                       "maxmem": "32g",
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
        'app_name': 'repliseq-parta',
        'workflow_uuid': '4459a4d8-1bd8-4b6a-b2cc-2506f4270a34',
        "parameters": {"nthreads": 4, "memperthread": "2G"},
        'custom_pf_fields': {
            'filtered_sorted_deduped_bam': {
                'genome_assembly': genome,
                'file_type': 'alignment',
                'description': 'This is an output file of the RepliSeq processing pipeline'},
            'count_bg': {
                'genome_assembly': genome,
                'file_type': 'counts',
                'description': 'read counts per 5 kb bin, unfiltered, unnormalized'}
        }},
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
        }
    ]

    template = [i for i in wf_dict if i['app_name'] == step_name][0]
    template['config'] = {
        "ebs_type": "gp2",
        "spot_instance": True,
        "ebs_iops": "",
        "log_bucket": "tibanna-output",
        "key_name": "4dn-encode"
        }
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
