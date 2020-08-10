#### Here are the common steps for adding a new workflow to wfr_cgap_checks.py

1. on cgap_utils.py,  add your workflow to the workflow_details dictionary.

```python
'app_name' : {
    "run_time": "integer value of accaptable run time in hours"
    "accepted_versions": "array of versions of this workflow"
    }
```

for example
```python
"md5": {
    "run_time": 12,
    "accepted_versions": ["0.0.4", "0.2.6"]
}
```

2. on wfrset_cgap_utils.py add your workflow specs using the following template

```python
{
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
```
If there are any hardcoded parameters or configs, you can add them on this item.
If you have multiple versions of the workflow, you should add the uuid for the
latest one in production here.


3. Write your check
You can look at the examples in the wfr_cgap_checks.py. There is a function called
`stepper` that is a wrapper that streamlines many aspects of a pipeline.



```python
cgap_utils.stepper(
    library,
    keep,
    step_tag, # if multi step pipeline, add step ie step1
    sample_tag, #  main tag (same for different steps of a pipeline ie sample)
    new_step_input_file, # which file to calculate attribution from
    input_file_dict, # all input files
    new_step_name, # app name of the workflow
    new_step_output_arg, # which output arg to return as output file
    additional_input={},
    organism='human',
    no_output=False
    )

# library contains
library['files']
library['wfrs']
library['qcs']

# keep contains
keep['running']
keep['problematic_run']
keep['missing_run']

# returns
keep, this_runs_status, this_runs_output = cgap_utils.stepper()
```


```python
```


```python
```
