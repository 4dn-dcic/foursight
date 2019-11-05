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
                'workflow_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6'
            },
            {
                'app_name': 'fastqc-0-11-4-1',
                'workflow_uuid': '2324ad76-ff37-4157-8bcc-3ce72b7dace9'
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
                        'description': 'Intermediate alingnment file'}
                        }
            },
            {
                'app_name': 'workflow_add-readgroups-check',
                'workflow_uuid': '1ebf697c-949d-46b2-b9a4-8b1a7699ef1f',
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
                        'description': 'Intermediate alingnment file'}
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
                        'description': 'Intermediate alingnment file'}
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
                        'description': 'Intermediate alingnment file'}
                        }
            },
            {  # step 5
                'app_name': 'workflow_sort-bam-check',
                'workflow_uuid': '560f5194-cd3a-4799-9b1a-6a2d2c371c89',
                'parameters': {},
                "config": {
                    "instance_type": "m5a.2xlarge",
                    "ebs_size": "3x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                },
                'custom_pf_fields': {
                    'sorted_bam': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alingnment file'}
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
                        'description': 'Intermediate alingnment file'}
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
            {  # step 8
                'app_name': 'workflow_index-sorted-bam',
                'workflow_uuid': '502e4846-a4ab-4da1-a5a3-d835442004a3',
                'parameters': {},
                "config": {
                    "instance_type": "t3.small",
                    "ebs_size": "1.2x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                }
            },
            {
               "app_name": "workflow_qcboard-bam",
               "parameters": {},
               "workflow_uuid": "ad8716a3-b6e8-4021-bbc6-b0cefc9c4dd8",
               "config": {
                 "instance_type": "t3.medium",
                 "ebs_size": "1.3x",
                 "EBS_optimized": True,
                 "behavior_on_capacity_limit": "wait_and_retry"
               }
            },

            # ROUND 2 runs
            # S2 run1
            {
                'app_name': 'workflow_gatk-HaplotypeCaller',
                'workflow_uuid': '7fd67e19-3425-45f8-8149-c7cac4278fdb',
                'parameters': {},
                "config": {
                    "instance_type": "c5n.18xlarge",
                    "ebs_size": "3x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                },
                'custom_pf_fields': {
                    'gvcf': {
                        'genome_assembly': genome,
                        'file_type': 'gVCF',
                        'description': 'processed output from cgap upstream pipeline'}
                        }
            },
            # S2 run2
            {
                'app_name': 'workflow_gatk-GenotypeGVCFs-check',
                'workflow_uuid': '4fbad226-859d-40d4-8192-10c305e819da',
                'parameters': {},
                "config": {
                    "instance_type": "t3.medium",
                    "ebs_size": "2x",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                },
                'custom_pf_fields': {
                    'vcf': {
                        'genome_assembly': genome,
                        'file_type': 'raw VCF',
                        'description': 'processed output from cgap upstream pipeline'}
                        }
            },
            {  # temp
                'app_name': '',
                'workflow_uuid': '',
                'parameters': {},
                "config": {
                    "instance_type": "",
                    "ebs_size": "",
                    "EBS_optimized": True,
                    "behavior_on_capacity_limit": "wait_and_retry"
                },
                'custom_pf_fields': {
                    'temp': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alingnment file'}
                        }
            }
            ]

    template = [i for i in wf_dict if i['app_name'] == step_name][0]
    update_config = {
        "spot_instance": True,
        "log_bucket": "tibanna-output",
        "key_name": "4dn-encode",
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
