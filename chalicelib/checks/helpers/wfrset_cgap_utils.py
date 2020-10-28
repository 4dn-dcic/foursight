# Step Settings
lambda_limit = 800
mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5'}

pairs_mapper = {"GRCh38": "hg38",
                "GRCm38": "mm10",
                "dm6": 'dm6',
                "galGal5": "galGal5"}

wf_dict = [
    {
        'app_name': 'md5',
        'workflow_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6',
        "config": {"ebs_size": 10}
    },
    {
        'app_name': 'fastqc',
        'workflow_uuid': '49e96b51-ed6c-4418-a693-d0e9f79adfa5',
        "config": {
            "ebs_size": 10,
            "instance_type": 't3.micro',
            'EBS_optimized': True
            }
    },
    {  # cram to fastq converter
        'app_name': 'workflow_cram2fastq',
        'workflow_uuid': '7bbf3487-a1fc-4073-952a-d5771973e875',
        'parameters': {},
        "config": {
            "instance_type": "c5.4xlarge",
            "ebs_size": "30x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'fastq1': {
                'file_type': 'reads',
                'description': 'Fastq files produced from CRAM files - paired end:1'},
            'fastq2': {
                'file_type': 'reads',
                'description': 'Fastq files produced from CRAM files - paired end:2'}
                }
    },
    {  # cram to bam converter
        'app_name': 'workflow_cram2bam-check',
        'workflow_uuid': '2a086f2b-7be4-4708-9516-1b39639292bf',
        'parameters': {},
        "config": {
            "instance_type": "c5.2xlarge",
            "ebs_size": "4.5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'converted_bam': {
                'file_type': 'alignments',
                'description': 'BAM file converted from CRAM file'
            }
        }
    },
    # http://patorjk.com/software/taag/#p=display&v=1&f=Graceful&t=QC
    #   __    ___
    #  /  \  / __)
    # (  O )( (__
    #  \__\) \___)
    {
       "app_name": "workflow_qcboard-bam",
       "parameters": {},
       "workflow_uuid": "ad8716a3-b6e8-4021-bbc6-b0cefc9c4dd8",
       "config": {
         "instance_type": "t3.medium",
         "ebs_size": "1.3x",
         "EBS_optimized": True
       }
    },
    #  ____   __   ____  ____    __
    # (  _ \ / _\ (  _ \(_  _)  (  )
    #  ) __//    \ )   /  )(     )(
    # (__)  \_/\_/(__\_) (__)   (__)
    # step1
    {
        'app_name': 'workflow_bwa-mem_no_unzip-check',
        'workflow_uuid': '50e75343-2e00-471d-a667-4acb083287d8',
        'parameters': {},
        "config": {
            "instance_type": "c5n.18xlarge",
            "ebs_size": "5x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
            },
        'custom_pf_fields': {
            'raw_bam': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    # step2
    {
        'app_name': 'workflow_add-readgroups-check',
        'workflow_uuid': 'd554d59b-e709-4c35-a81f-68a0cb3dd38a',
        'parameters': {},
        "config": {
            "instance_type": "c5.2xlarge",
            "ebs_size": "2.2x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
            },
        'custom_pf_fields': {
            'bam_w_readgroups': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    # step3
    {
        'app_name': 'workflow_merge-bam-check',
        'workflow_uuid': '4853a03a-8c0c-4624-a45d-c5206a72907b',
        'parameters': {},
        "config": {
            "instance_type": "c5.2xlarge",
            "ebs_size": "2.2x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        },
        'custom_pf_fields': {
            'merged_bam': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    {  # step 4
        'app_name': 'workflow_picard-MarkDuplicates-check',
        'workflow_uuid': 'beb2b340-94ee-4afe-b4e3-66caaf063397',
        'parameters': {},
        "config": {
            "instance_type": "c5n.18xlarge",
            "ebs_size": "3x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        },
        'custom_pf_fields': {
            'dupmarked_bam': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    {  # step 5
        'app_name': 'workflow_sort-bam-check',
        'workflow_uuid': '560f5194-cd3a-4799-9b1a-6a2d2c371c89',
        'parameters': {},
        "config": {
            "instance_type": "m5a.2xlarge",
            "ebs_size": "3.2x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        },
        'custom_pf_fields': {
            'sorted_bam': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    {  # step 6
        'app_name': 'workflow_gatk-BaseRecalibrator',
        'workflow_uuid': '455b3056-64ca-4a9b-b546-294b01c9ca92',
        'parameters': {},
        "config": {
            "instance_type": "t3.small",
            "ebs_size": "2x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        },
        'custom_pf_fields': {
            'recalibration_report': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    },
    {  # step 7
        'app_name': 'workflow_gatk-ApplyBQSR-check',
        'workflow_uuid': '6c9c6f49-f954-4e76-8dfb-d385cddcebd6',
        'parameters': {},
        "config": {
            "instance_type": "t3.micro",
            "ebs_size": "2.5x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        },
        'custom_pf_fields': {
            'recalibrated_bam': {
                'file_type': 'alignments',
                'description': 'processed output from cgap upstream pipeline'}
                }
    },
    # part 1 - step 8   (only run for samples that will go to part3)
    {  # mpileupCounts
        'app_name': 'workflow_granite-mpileupCounts',
        'workflow_uuid': 'c6dac0af-631d-402f-a1c1-282e091f1b3e',
        'parameters': {"nthreads": 15},
        "config": {
            "instance_type": "c5.4xlarge",
            "ebs_size": 200,
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'rck': {
                'file_type': 'read counts (rck)',
                'description': 'read counts (rck) file'
            }
        }
    },
    # step 9
    {
        'app_name': 'workflow_gatk-HaplotypeCaller',
        'workflow_uuid': '7fd67e19-3425-45f8-8149-c7cac4278fdb',
        'parameters': {"nthreads": 20},
        "config": {
            "instance_type": "c5n.18xlarge",
            "ebs_size": "5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'gvcf': {
                'file_type': 'gVCF',
                'description': 'processed output from cgap upstream pipeline'}
                }
    },
    {  # step 10 bamqc
        'app_name': 'cgap-bamqc',
        'workflow_uuid': 'd6651132-ab7c-40c0-886f-94f88ef6bdce',
        'parameters': {},
        "config": {
            "instance_type": "c5n.2xlarge",
            "ebs_size": "2.5x",
            "EBS_optimized": True,
            "behavior_on_capacity_limit": "wait_and_retry"
        }
    },
    #  ____   __   ____  ____    __  __
    # (  _ \ / _\ (  _ \(_  _)  (  )(  )
    #  ) __//    \ )   /  )(     )(  )(
    # (__)  \_/\_/(__\_) (__)   (__)(__)
    # Multi sample analysis
    {
        'app_name': 'workflow_gatk-CombineGVCFs',
        'workflow_uuid': 'c7223a1c-ed48-4c54-a39f-35f05d61e850',
        'parameters': {},
        "config": {
            "instance_type": "c5n.4xlarge",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'combined_gvcf': {
                'file_type': 'combined gVCF',
                'description': 'processed output from cgap downstream pipeline'}
                }
    },
    {
        'app_name': 'workflow_gatk-GenotypeGVCFs-check',
        'workflow_uuid': '4fbad226-859d-40d4-8192-10c305e819da',
        'parameters': {},
        "config": {
            "instance_type": "c5n.4xlarge",
            "ebs_size": "1.5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'vcf': {
                'file_type': 'raw VCF',
                'description': 'processed output from cgap downstream pipeline'}
                }
    },
    {  # VEP
        'app_name': 'workflow_vep-parallel',
        'workflow_uuid': 'adc588cf-1c6c-4281-9193-9645726eb792',
        'parameters': {"nthreads": 15},
        "config": {
            "instance_type": "c5.9xlarge",
            "ebs_size": "10x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'microannot_mti': {
                'file_type': 'intermediate file',
                'description': 'Intermediate file'
                },
            'annot_mti': {
                'file_type': 'intermediate file',
                'description': 'Intermediate file'
                }
        }
    },
    {  # micro-annotation
        'app_name': 'workflow_mutanno-micro-annot-check',
        'workflow_uuid': 'ca3469c6-ac71-4a7d-97ea-477037b05f2f',
        'parameters': {"nthreads": 70},
        "config": {
            "instance_type": "c5n.18xlarge",
            "ebs_size": "3x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'annotated_vcf': {
                'file_type': 'micro-annotated VCF',
                'description': 'micro-annotated VCF file'
            }
        }
    },
    #  ____   __   ____  ____    __  __  __
    # (  _ \ / _\ (  _ \(_  _)  (  )(  )(  )
    #  ) __//    \ )   /  )(     )(  )(  )(
    # (__)  \_/\_/(__\_) (__)   (__)(__)(__)
    {  # step1a rckTar
        'app_name': 'workflow_granite-rckTar',
        'workflow_uuid': '64ff003a-b25d-4856-a9fc-ad8702b8c6d4',
        'parameters': {},
        "config": {
            "instance_type": "c5.xlarge",
            "ebs_size": "2.5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'rck_tar': {
                'file_type': 'tarred read counts (rck)',
                'description': 'tarred read counts (rck) file'
            }
        }
    },
    {  # Step2 - filtering
        'app_name': 'workflow_granite-filtering-check',
        'workflow_uuid': 'e43171b4-4ee5-4074-8734-727399e3179d',
        'parameters': {"aftag": "gnomADgenome", "afthr": 0.01},
        "config": {
            "instance_type": "t3.medium",
            "ebs_size": "6x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'merged_vcf': {
                'file_type': 'intermediate file',
                'description': 'Intermediate VCF file'
            }
        }
    },
    {  # Step3 - novocaller
        'app_name': 'workflow_granite-novoCaller-rck-check',
        'workflow_uuid': '55c9ebf7-ef39-4eb0-9685-c090f2e788ae',
        'parameters': {},
        "config": {
            "instance_type": "c5.xlarge",
            "ebs_size": "1.5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'novoCaller_vcf': {
                'file_type': 'intermediate file',
                'description': 'Intermediate VCF file'
            }
        }
    },
    {  # Step4 - compHet
        'app_name': 'workflow_granite-comHet-check',
        'workflow_uuid': 'f43c5ac3-d755-4acc-a6ed-7f65b8b4961b',
        'parameters': {},
        "config": {
            "instance_type": "t3.small",
            "ebs_size": "2.5x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'comHet_vcf': {
                'file_type': 'intermediate file',
                'description': 'Intermediate VCF file'
            }
        }
    },
    {  # Step5 - full annotation
        'app_name': 'workflow_mutanno-annot-check',
        'workflow_uuid': '04da27aa-204c-4db2-9d66-a1624a463c13',
        'parameters': {"nthreads": 1},
        "config": {
            "instance_type": "c5.large",
            "ebs_size": "1.2x",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'annotated_vcf': {
                'file_type': 'full annotated VCF',
                'description': 'full annotated VCF file'
            }
        }
    },
    {  # Step 6 = bamsnap
        'app_name': 'bamsnap',
        'workflow_uuid': 'a4016214-e4ce-4a34-93a1-c0751bbd1d37',
        'parameters': {"nproc": 16},
        "config": {
            "instance_type": "c5.4xlarge",
            "ebs_size": 10,
            "EBS_optimized": True
        }
    },
    {  # VCFQC used in Part III & Part II
        'app_name': 'workflow_granite-qcVCF',
        'workflow_uuid': '33a85705-b757-49e0-aaef-d786695d6d03',
        'parameters': {"trio_errors": True,
                       "het_hom": True,
                       "ti_tv": True},
        "config": {
            "instance_type": "t3.small",
            "ebs_size": "1.5x",
            "EBS_optimized": True
        }
    },
    {  # temp
        'app_name': '',
        'workflow_uuid': '',
        'parameters': {},
        "config": {
            "instance_type": "",
            "ebs_size": "",
            "EBS_optimized": True
        },
        'custom_pf_fields': {
            'temp': {
                'file_type': 'intermediate file',
                'description': 'Intermediate alignment file'}
                }
    }
]


def step_settings(step_name, my_organism, attribution, overwrite=None):
    """Return a setting dict for given step, and modify variables in
    output files; genome assembly, file_type, desc
    overwrite is a dictionary, if given will overwrite keys in resulting template
    overwrite = {'config': {"a": "b"},
                 'parameters': {'c': "d"},
                 'custom_pf_fields': { 'file_arg': {'e': 'f'}}
                    }
    """
    genome = ""
    genome = mapper.get(my_organism)

    templates = [i for i in wf_dict if i['app_name'] == step_name]
    # every app name should exist only once in wf_dict
    if len(templates) != 1:
        raise ValueError('There are multiple {} settings on wfr_cgap_utils.py'.format(step_name))
    template = templates[0]

    # add genomes to output files
    if template.get('custom_pf_fields'):
        for an_output_file in template['custom_pf_fields']:
            template['custom_pf_fields'][an_output_file]['genome_assembly'] = genome

    update_config = {
        "spot_instance": True,
        "log_bucket": "tibanna-output",
        "key_name": "4dn-encode",
        "public_postrun_json": True,
        "behavior_on_capacity_limit": "wait_and_retry"
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

    if not template.get('wfr_meta'):
        template['wfr_meta'] = {}
    template['wfr_meta'].update(attribution)

    if not template.get('custom_qc_fields'):
        template['custom_qc_fields'] = {}
    template['custom_qc_fields'].update(attribution)

    if overwrite:
        for a_key in overwrite:
            for a_spec in overwrite[a_key]:
                # if the key value is a dictionary, use update
                if isinstance(overwrite[a_key][a_spec], dict):
                    template[a_key][a_spec].update(overwrite[a_key][a_spec])
                # if it is string array bool, set the value
                else:
                    template[a_key][a_spec] = overwrite[a_key][a_spec]
    return template
