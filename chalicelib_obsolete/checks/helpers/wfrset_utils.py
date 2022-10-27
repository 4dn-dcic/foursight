# Step Settings
lambda_limit = 750
load_wait = 8
random_wait = 20

mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5',
          'zebrafish': 'GRCz11'}

pairs_mapper = {"GRCh38": "hg38",
                "GRCm38": "mm10",
                "dm6": 'dm6',
                "galGal5": "galGal5",
                "GRCz11": "danRer11"}


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

    wf_dict = [
    {
        'app_name': 'md5',
        'workflow_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6',
        "config": {
            "ebs_size": 10,
            "instance_type": 't3.small',
            'EBS_optimized': True
        }
    },
    {
        'app_name': 'fastqc',
        'workflow_uuid': '49e96b51-ed6c-4418-a693-d0e9f79adfa5',
        "config": {
            "ebs_size": 10,
            "instance_type": 't3.small',
            'EBS_optimized': True
        }
    },
    {
        'app_name': 'pairsqc-single',
        'workflow_uuid': 'b8c533e0-f8c0-4510-b4a1-ac35158e27c3',
        "config": {"instance_type": 't3.small'}
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
        }
    },
    {
        'app_name': 'hi-c-processing-bam',
        'workflow_uuid': '023bfb3e-9a8b-42b9-a9d4-216079526f68',
        'parameters': {"nthreads_merge": 16, "nthreads_parse_sort": 16},
        'custom_pf_fields': {
            'annotated_bam': {
                'genome_assembly': genome,
                'file_type': 'alignments',
                'description': out_n},
            'filtered_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-replicate',
                'description': out_n}
        }
    },
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
        }
    },
    {
        'app_name': 'imargi-processing-fastq',
        'workflow_uuid': '7eedaaa8-4c2e-4c71-9d9a-04f05ab1becf',
        'config': {'mem': 8, 'cpu': 4, 'ebs_size': '12x', 'EBS_optimized': 'true'},
        'parameters': {"nThreads": 4},
        'custom_pf_fields': {
            'out_bam': {
                'genome_assembly': genome,
                'file_type': 'alignments',
                'description': "This is an alignment file for fastq pairs from the MARGI processing pipeline"}
        }
    },
    {
        'app_name': 'imargi-processing-bam',
        'workflow_uuid': '4918e659-6e6c-444f-93c4-276c0d753537',
        'config': {'mem': 8, 'cpu': 8, 'ebs_size': '10x', 'EBS_optimized': 'true'},
        'parameters': {"nthreads": 8, "assembly": pairs_assembly},
        'custom_pf_fields': {
            'out_qc': {
                'genome_assembly': genome,
                'file_type': 'QC',
                'description': 'This is an output file of the MARGI processing pipeline'},
            'out_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-replicate',
                'description': 'This is an output file of the MARGI processing pipeline'}
        }
    },
    {
        'app_name': 'imargi-processing-pairs',
        'workflow_uuid': 'd3e33c23-7442-4f43-8601-337d2f04980a',
        'config': {'mem': 8, 'cpu': 4, 'ebs_size': '10x', 'EBS_optimized': 'true'},
        'custom_pf_fields': {
            'out_mcool': {
                'genome_assembly': genome,
                'file_type': 'contact matrix',
                'description': 'This is an output file of the MARGI processing pipeline'},
            'merged_pairs': {
                'genome_assembly': genome,
                'file_type': 'contact list-combined',
                'description': 'This is an output file of the MARGI processing pipeline'}
        }
    },
    {
        'app_name': 'repliseq-parta',
        'workflow_uuid': '4dn-dcic-lab:wf-repliseq-parta-v16',
        "parameters": {"nthreads": 4, "memperthread": "2G"},
        'custom_pf_fields': {
            'filtered_sorted_deduped_bam': {
                'genome_assembly': genome,
                'file_type': 'alignments',
                'description': 'This is an output file of the RepliSeq processing pipeline'},
            'count_bg': {
                'genome_assembly': genome,
                'file_type': 'counts',
                'description': 'read counts, unfiltered, unnormalized'}
        }
    },
    {
        "app_name": "bedtobeddb",
        "workflow_uuid": "91049eef-d434-4e16-a1ad-06de73f079dc",
        "config": {'mem': 4, 'cpu': 2, "ebs_size": 10},
        "overwrite_input_extra": True
    },
    {
        "app_name": "bedtomultivec",
        "workflow_uuid": "a52b9b9d-1654-4967-883f-4d2adee77bc7",
        'config': {'mem': 4, 'cpu': 2, 'EBS_optimized': 'false'},
        "overwrite_input_extra": True
    },
    {
        "app_name": "bedGraphToBigWig",
        "workflow_uuid": "68d412a1-b78e-4101-b353-2f3da6272529",
        "config": {'mem': 4, 'cpu': 2, "ebs_size": 30},
        "overwrite_input_extra": True
    },
    {
        "app_name": "merge-fastq",
        "workflow_uuid": "e20ef13d-64d8-4d10-94b1-ed45e7d6a7c2",
        "parameters": {},
        'custom_pf_fields': {
            'merged_fastq': {
                'genome_assembly': genome,
                'file_type': 'reads-combined',
                'description': 'Merged fastq file'
            }
        }
    },
    {
        "app_name": "encode-chipseq-aln-chip",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-chipseq-aln-chip",
        "parameters": {},
        "config": {},
        'custom_pf_fields': {
            'chip.first_ta': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Positions of aligned reads in bed format, one line per read mate, for control experiment, from ENCODE ChIP-Seq Pipeline'
            },
            'chip.first_ta_xcor': {
                'genome_assembly': genome,
                'file_type': 'intermediate file',
                'description': 'Counts file used only for QC'
            }
        }
    },
    {
        "app_name": "encode-chipseq-aln-ctl",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-chipseq-aln-ctl",
        "parameters": {},
        "config": {},
        'custom_pf_fields': {
            'chip.first_ta_ctl': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Positions of aligned reads in bed format, one line per read mate, for control experiment, from ENCODE ChIP-Seq Pipeline',
                'disable_wfr_inputs': True}
        }
    },
    {
        "app_name": "encode-chipseq-postaln",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-chipseq-postaln",
        "parameters": {},
        "config": {},
        'custom_pf_fields': {
            'chip.optimal_peak': {
                'genome_assembly': genome,
                'file_type': 'peaks',
                'description': 'Peak calls from ENCODE ChIP-Seq Pipeline'},
            'chip.conservative_peak': {
                'genome_assembly': genome,
                'file_type': 'conservative peaks',
                'description': 'Conservative peak calls from ENCODE ChIP-Seq Pipeline'},
            'chip.sig_fc': {
                'genome_assembly': genome,
                'file_type': 'signal fold change',
                'description': 'ChIP-seq signal fold change over input control'}
        }
    },
    {
        "app_name": "encode-atacseq-aln",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-atacseq-aln",
        "parameters": {},
        "config": {},
        'custom_pf_fields': {
            'atac.first_ta': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Positions of aligned reads in bed format, one line per read mate, from ENCODE ATAC-Seq Pipeline'}
        }
    },
    {
        "app_name": "encode-atacseq-postaln",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-atacseq-postaln",
        "parameters": {},
        "config": {},
        'custom_pf_fields': {
            'atac.optimal_peak': {
                'genome_assembly': genome,
                'file_type': 'peaks',
                'description': 'Peak calls from ENCODE ATAC-Seq Pipeline'},
            'atac.conservative_peak': {
                'genome_assembly': genome,
                'file_type': 'conservative peaks',
                'description': 'Conservative peak calls from ENCODE ATAC-Seq Pipeline'},
            'atac.sig_fc': {
                'genome_assembly': genome,
                'file_type': 'signal fold change',
                'description': 'ATAC-seq signal fold change'}
        }
    },
    {
        "app_name": "mergebed",
        "workflow_uuid": "2b10e472-065e-43ed-992c-fccad6417b65",
        "parameters": {"sortv": "0"},
        'custom_pf_fields': {
            'merged_bed': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Merged file, positions of aligned reads in bed format, one line per read mate'}
        }
    },
    {
        "app_name": "insulation-scores-and-boundaries-caller",
        "workflow_uuid": "dc9efc2d-baa5-4304-b72b-14610d8d5fc4",
        "parameters": {"binsize": -1, "windowsize": 100000},
        "config": {'mem': 32},
        'custom_pf_fields': {
            'bwfile': {
                'genome_assembly': genome,
                'file_type': 'insulation score-diamond',
                'description': 'Diamond insulation scores calls on Hi-C contact matrices'},
            'bedfile': {
                'genome_assembly': genome,
                'file_type': 'boundaries',
                'description': 'Boundaries calls on Hi-C contact matrices'}
        }
    },
    {
        "app_name": "compartments-caller",
        "workflow_uuid": "d07fa5d4-8721-403e-89b5-e8f323ac9ece",
        "parameters": {"binsize": 250000, "contact_type": "cis"},
        "config": {'mem': 4, 'cpu': 1, 'ebs_size': '1.1x', 'EBS_optimized': 'false'},
        'custom_pf_fields': {
            'bwfile': {
                'genome_assembly': genome,
                'file_type': 'compartments',
                'description': 'Compartments signals on Hi-C contact matrices'}
        },
    },
    {
        "app_name": "rna-strandedness",
        "workflow_uuid": "af97597e-877a-40b7-b211-98ec0cfb17b4",
        'config': {'mem': 2, 'cpu': 2, "instance_type": "t3.small", 'ebs_size': '1.1x', 'EBS_optimized': 'false'}
    },
    # RNA SEQ
    {
        "app_name": "encode-rnaseq-stranded",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-rnaseq-stranded",
        "parameters": {
            'rna.strandedness': 'stranded',
            'rna.strandedness_direction': '',
            'rna.endedness': ''
        },
        'custom_pf_fields': {
            'rna.outbam': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.plusbw': {
                'genome_assembly': genome,
                'file_type': 'read counts (plus)',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.minusbw': {
                'genome_assembly': genome,
                'file_type': 'read counts (minus)',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.gene_expression': {
                'genome_assembly': genome,
                'file_type': 'gene expression',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.isoform_expression': {
                'genome_assembly': genome,
                'file_type': 'isoform expression',
                'description': 'Output file from RNA seq pipeline'
            }
        }
    },
    {
        "app_name": "encode-rnaseq-unstranded",
        "workflow_uuid": "4dn-dcic-lab:wf-encode-rnaseq-unstranded",
        "parameters": {
            'rna.strandedness': 'unstranded',
            'rna.strandedness_direction': 'unstranded',
            'rna.endedness': 'paired'
        },
        'custom_pf_fields': {
            'rna.outbam': {
                'genome_assembly': genome,
                'file_type': 'read positions',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.outbw': {
                'genome_assembly': genome,
                'file_type': 'read counts',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.gene_expression': {
                'genome_assembly': genome,
                'file_type': 'gene expression',
                'description': 'Output file from RNA seq pipeline'
            },
            'rna.isoform_expression': {
                'genome_assembly': genome,
                'file_type': 'isoform expression',
                'description': 'Output file from RNA seq pipeline'
            }
        }
    },
    {
        "app_name": "bamqc",
        "workflow_uuid": "42683ab1-59bf-4ec5-a973-030053a134f1",
        "overwrite_input_extra": False,
        "config": {"ebs_size": 10}
    },
    {
        "app_name": "fastq-first-line",
        "workflow_uuid": "93a1a931-d55d-4623-adfb-0fa735daf6ae",
        "overwrite_input_extra": False,
        'config': {'mem': 2, 'cpu': 2, "instance_type": "t3.small"}
    },
    {
        "app_name": "re_checker_workflow",
        "workflow_uuid": "8479d16e-667a-41e9-8ace-391128f50dc5",
        "parameters": {},
        "config": {"mem": 4,
                   "ebs_size": 10,
                   "instance_type": "t3.medium"
        }
    },
    {
        "app_name": "mad_qc_workflow",
        "workflow_uuid": "4dba38f0-af7a-4432-88e4-ca804dea64f8",
        "parameters": {},
        "config": {"ebs_size": 10, "instance_type": "t3.medium"}
    },
    {
        "app_name": "mcoolQC",
        "workflow_uuid": "0bf9f47a-dec1-4324-9b41-fa183880a7db",
        "overwrite_input_extra": False,
        "config": {"ebs_size": 10, "instance_type": "c5ad.2xlarge"}
    },
    # temp
    {
        "app_name": "",
        "workflow_uuid": "",
        "parameters": {},
        'custom_pf_fields': {
            '': {
                'genome_assembly': genome,
                'file_type': '',
                'description': ''}
        }
    }]

    template = [i for i in wf_dict if i['app_name'] == step_name][0]

    update_config = {
        "ebs_type": "gp2",
        "spot_instance": False,
        "ebs_iops": "",
        "log_bucket": "tibanna-output",
        "key_name": "4dn-encode",
        "public_postrun_json": True,
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
    template['common_fields'] = attribution
    if overwrite:
        for a_key in overwrite:
            for a_spec in overwrite[a_key]:
                # if the key value is a dictionary, set default and use update
                if isinstance(overwrite[a_key][a_spec], dict):
                    template[a_key].setdefault(a_spec, {}).update(overwrite[a_key][a_spec])
                # if it is string array bool, set the value
                else:
                    template[a_key][a_spec] = overwrite[a_key][a_spec]
    return template
