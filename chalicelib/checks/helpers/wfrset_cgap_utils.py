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
    output files; genome assembly, file_type, desc
    overwrite is a dictionary, if given will overwrite keys in resulting template
    overwrite = {'config': {"a": "b"},
                 'parameters': {'c': "d"}
                    }
    """
    genome = ""
    genome = mapper.get(my_organism)
    # int_n_rep = "This is an intermediate file in the Repliseq processing pipeline"

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
            {
                'app_name': 'workflow_bwa-mem_no_unzip-check',
                'workflow_uuid': '9e094699-561b-4396-8d6a-ffc45f98c5e1',
                'parameters': {},
                "config": {
                    "instance_type": "c5n.18xlarge",
                    "ebs_size": "5x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                    },
                'custom_pf_fields': {
                    'raw_bam': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alignment file'}
                        }
            },
            {
                'app_name': 'workflow_add-readgroups-check',
                'workflow_uuid': '6ea6bf43-76c2-4616-8dd6-dd60d72a2bf2',
                'parameters': {},
                "config": {
                    "instance_type": "c5.2xlarge",
                    "ebs_size": "2.2x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                    },
                'custom_pf_fields': {
                    'bam_w_readgroups': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alignment file'}
                        }
            },
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
                        'genome_assembly': genome,
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
                        'genome_assembly': genome,
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
                        'genome_assembly': genome,
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
                        'genome_assembly': genome,
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
                        'genome_assembly': genome,
                        'file_type': 'alignments',
                        'description': 'processed output from cgap upstream pipeline'}
                        }
            },
            # obsolete old step 8
            # {
            #     'app_name': 'workflow_index-sorted-bam',
            #     'workflow_uuid': '502e4846-a4ab-4da1-a5a3-d835442004a3',
            #     'parameters': {},
            #     "config": {
            #         "instance_type": "t3.small",
            #         "ebs_size": "1.2x",
            #         "EBS_optimized": True,
            #         "behavior_on_capacity_limit": "wait_and_retry"
            #     }
            # },
            # step 8
            {
                'app_name': 'workflow_gatk-HaplotypeCaller',
                'workflow_uuid': '7fd67e19-3425-45f8-8149-c7cac4278fdb',
                'parameters': {"nthreads": 20},
                "config": {
                    "instance_type": "c5n.18xlarge",
                    "ebs_size": "3x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'gvcf': {
                        'genome_assembly': genome,
                        'file_type': 'gVCF',
                        'description': 'processed output from cgap upstream pipeline'}
                        }
            },
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
                        'genome_assembly': genome,
                        'file_type': 'combined VCF',
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
                        'genome_assembly': genome,
                        'file_type': 'raw VCF',
                        'description': 'processed output from cgap downstream pipeline'}
                        }
            },
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
                        'genome_assembly': genome,
                        'file_type': 'reads',
                        'description': 'Fastq files produced from CRAM files - paired end:1'},
                    'fastq2': {
                        'genome_assembly': genome,
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
                        'genome_assembly': genome,
                        'file_type': 'alignments',
                        'description': 'BAM file converted from CRAM file'
                    }
                }
            },
            {  # micro-annotation
                'app_name': 'workflow_mutanno-micro-annot-check',
                'workflow_uuid': '04caaa82-6a32-46d3-b52c-c1017cc0490a',
                'parameters': {},
                "config": {
                    "instance_type": "c5n.18xlarge",
                    "ebs_size": "3x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'annotated_vcf': {
                        'genome_assembly': genome,
                        'file_type': 'micro-annotated VCF',
                        'description': 'micro-annotated VCF file'
                    }
                }
            },
            {  # whitelist
                'app_name': 'workflow_granite-whiteList-check',
                'workflow_uuid': 'ce7f9e0b-a0d1-4119-bd66-373ccfcabac7',
                'parameters': {},
                "config": {
                    "instance_type": "c5.large",
                    "ebs_size": "2x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'whiteList_vcf': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate VCF file'
                    }
                }
            },
            {  # blacklist
                'app_name': 'workflow_granite-blackList-check',
                'workflow_uuid': 'c258d1ec-397d-4b0a-a0be-7c8211d65e6a',
                'parameters': {
                    "aftag": "gnomADgenome",
                    "afthr": 0.01
                },
                "config": {
                    "instance_type": "t3.small",
                    "ebs_size": "1.2x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'blackList_vcf': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate VCF file'
                    }
                 }
            },
            {  # novocaller
                'app_name': 'workflow_granite-novoCaller-rck-check',
                'workflow_uuid': '35daf195-4fc5-4e2a-ada3-7a0cce08a7e4',
                'parameters': {},
                "config": {
                    "instance_type": "c5.xlarge",
                    "ebs_size": "1.5x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'novoCaller_vcf': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate VCF file'
                    }
                }
            },
            {  # full annotation
                'app_name': 'workflow_mutanno-annot-check',
                'workflow_uuid': '883b7846-8c62-4f5a-a691-c84706420b93',
                'parameters': {},
                "config": {
                    "instance_type": "c5.large",
                    "ebs_size": "1.2x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'annotated_vcf': {
                        'genome_assembly': genome,
                        'file_type': 'full-annotated VCF',
                        'description': 'full-annotated VCF file'
                    }
                }
            },
            # part 1 - step 7a
            {  # mpileupCounts
                'app_name': 'workflow_granite-mpileupCounts',
                'workflow_uuid': 'e5c178cf-4b5c-488b-b4cc-08273d11697d',
                'parameters': {"nthreads": 15},
                "config": {
                    "instance_type": "c5.4xlarge",
                    "ebs_size": "4.5x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'rck': {
                        'genome_assembly': genome,
                        'file_type': 'read counts (rck)',
                        'description': 'read counts (rck) file'
                    }
                }
            },
            {  # rckTar
                'app_name': 'workflow_granite-rckTar',
                'workflow_uuid': '778149e7-98d7-4362-83a4-9e80af1da101',
                'parameters': {},
                "config": {
                    "instance_type": "c5.xlarge",
                    "ebs_size": "2.5x",
                    "EBS_optimized": True
                },
                'custom_pf_fields': {
                    'rck_tar': {
                        'genome_assembly': genome,
                        'file_type': 'tarred read counts (rck)',
                        'description': 'tarred read counts (rck) file'
                    }
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
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alignment file'}
                        }
            }
            ]

    template = [i for i in wf_dict if i['app_name'] == step_name][0]
    update_config = {
        "spot_instance": False,
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
    template['wfr_meta'] = attribution
    template['custom_qc_fields'] = attribution
    if overwrite:
        for a_key in overwrite:
            for a_spec in overwrite[a_key]:
                template[a_key][a_spec] = overwrite[a_key][a_spec]
    return template
