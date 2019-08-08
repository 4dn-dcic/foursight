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
    # int_n_rep = "This is an intermediate file in the Repliseq processing pipeline"

    wf_dict = [
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
                    'output': {
                        'genome_assembly': genome,
                        'file_type': 'intermediate file',
                        'description': 'Intermediate alingnemnt file'}
                        }}
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
