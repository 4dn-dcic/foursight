from dcicutils import ff_utils, s3Utils
from datetime import datetime
from operator import itemgetter
from . import wfrset_cgap_utils
import json
lambda_limit = wfrset_cgap_utils.lambda_limit
# use wf_dict in workflow version check to make sure latest version and workflow uuid matches
wf_dict = wfrset_cgap_utils.wf_dict
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
    "fastqc": {
        "run_time": 50,
        "accepted_versions": ["v1", "v2"]
    },
    "workflow_bwa-mem_no_unzip-check": {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_add-readgroups-check": {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_merge-bam-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_picard-MarkDuplicates-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_sort-bam-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_gatk-BaseRecalibrator": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_gatk-ApplyBQSR-check": {
        "run_time": 12,
        "accepted_versions": ["v9", "v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    # defunct step 8
    "workflow_index-sorted-bam": {
        "run_time": 12,
        "accepted_versions": ["v9"]
    },
    "workflow_granite-mpileupCounts": {
        "run_time": 12,
        "accepted_versions": ["v14", "v15", "v16", "v17"]
    },
    # new step 8
    'workflow_gatk-HaplotypeCaller': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    # step 9
    'workflow_granite-mpileupCounts': {
        "run_time": 12,
        "accepted_versions": ["v14", "v15", "v16", "v17"]
    },
    # step 10
    'cgap-bamqc': {
        "run_time": 12,
        "accepted_versions": ["v2", "v3"]
    },
    # # PART II
    # part II step 1
    'workflow_gatk-CombineGVCFs': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    # part II step 2
    'workflow_gatk-GenotypeGVCFs-check': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13", "v15", "v16", "v17"]
    },
    # part III step 3
    'workflow_gatk-VQSR-check': {
        "run_time": 12,
        "accepted_versions": ["v10", "v11", "v12", "v13"]
    },
    "workflow_qcboard-bam": {
        "run_time": 12,
        "accepted_versions": ["v3"]
    },
    "workflow_cram2fastq": {
        "run_time": 12,
        "accepted_versions": ["v12", "v13", "v15", "v16", "v17"]
    },
    "workflow_cram2bam-check": {
        "run_time": 12,
        "accepted_versions": ["v13", "v15", "v16", "v17"]
    },
    "workflow_vep-parallel": {
        "run_time": 12,
        "accepted_versions": ["v17"]
    },
    "workflow_mutanno-micro-annot-check": {
        "run_time": 12,
        "accepted_versions": ["v17"]
    },
    # Part III
    "workflow_granite-rckTar": {
        "run_time": 12,
        "accepted_versions": ["v16", "v17"]
    },
    "workflow_granite-filtering-check": {
        "run_time": 12,
        "accepted_versions": ["v16", "v17"]
    },
    "workflow_granite-novoCaller-rck-check": {
        "run_time": 12,
        "accepted_versions": ["v16", "v17"]
    },
    "workflow_granite-comHet-check": {
        "run_time": 12,
        "accepted_versions": ["v16", "v17"]
    },
    "workflow_mutanno-annot-check": {
        "run_time": 12,
        "accepted_versions": ["v17"]
    },
    "bamsnap": {
        "run_time": 12,
        "accepted_versions": ["v17"]
    },
    "workflow_granite-qcVCF": {
        "run_time": 12,
        "accepted_versions": ["v2", "v3"]
    },
    "workflow_peddy": {
        "run_time": 12,
        "accepted_versions": ["v3"]
    }
}


# Reference Files (should be @id)
bwa_index = {'human': '/files-reference/GAPFI4U1HXIY/'}


def remove_parents_without_sample(samples_pedigree):
    individuals = [i['individual'] for i in samples_pedigree]
    for a_member in samples_pedigree:
        parents = a_member['parents']
        new_parents = [i for i in parents if i in individuals]
        a_member['parents'] = new_parents
    return samples_pedigree


def analyze_pedigree(samples_pedigree_json, all_samples):
    """extract pedigree for qc for trio (father/mother/proband) or proband
    - input samples accession list
    - qc pedigree
    - run mode (trio or proband_only)
    - error (expected empty)
    """
    # remove parent ids that are not in the sample_pedigree as individual
    samples_pedigree = remove_parents_without_sample(samples_pedigree_json)
    # Define run Mode: Trio or Proband only
    # order samples by father/mother/proband
    run_mode = ""
    error = ""
    input_samples = []
    qc_pedigree = []  # used by vcfqc
    # trio analysis
    if len(all_samples) > 2:
        for member in ['father', 'mother', 'proband']:
            sample_info = [i for i in samples_pedigree if i.get('relationship') == member]
            if not sample_info:
                break
            member_qc_pedigree = {
                'gender': sample_info[0].get('sex', ''),
                'individual': sample_info[0].get('individual', ''),
                'parents': sample_info[0].get('parents', []),
                'sample_name': sample_info[0].get('sample_name', '')
                }
            qc_pedigree.append(member_qc_pedigree)
            input_samples.append(sample_info[0]['sample_accession'])
        if len(input_samples) != 3:
            error = 'does not have mother father proband info'
            return "", "", "", error
        run_mode = 'trio'
    # if there are only 2 or less members, go for proband only
    else:
        sample_info = [i for i in samples_pedigree if i.get('relationship') == 'proband']
        if not sample_info:
            error = 'does not have proband info'
            return "", "", "", error
        input_samples.append(sample_info[0]['sample_accession'])
        member_qc_pedigree = {
            'gender': sample_info[0].get('sex', ''),
            'individual': sample_info[0].get('individual', ''),
            'parents': sample_info[0].get('parents', []),
            'sample_name': sample_info[0].get('sample_name', '')
            }
        qc_pedigree.append(member_qc_pedigree)
        run_mode = 'proband_only'
    # remove parents from mother and father (temporary fix until vcfqc V4 is in production)
    qc_pedigree = remove_parents_without_sample(qc_pedigree)
    return input_samples, qc_pedigree, run_mode, error


def get_bamsnap_parameters(samples_pedigree, all_samples):
    """collect bam @ids and titles for all samples in the sample_procesing item
    used by bamsnap
    start with proband, and continue with close relatives
    - args: samples and samples_pedigree fields of sample_processing
    - returns: 2 lists, collected bams and titles
    """

    def get_summary(a_role_info, all_samples):
        """given a samples pedigree item and all samples information (from samples pedigree)
        return a summary for bamsnap"""
        relation = a_role_info['relationship']
        sample_acc = a_role_info['sample_accession']
        sample_name = a_role_info.get('sample_name', '')
        sample_title = "{} ({})".format(sample_name, relation)
        # get sample info from samples field
        sample_info = [i for i in all_samples if sample_acc in i['@id']][0]
        bams = [i['@id'] for i in sample_info['processed_files'] if i['display_title'].endswith('bam')]
        if not bams:
            raise ValueError('can not locate bam file on sample {} to be used by bamsnap'.format(sample_acc))
        bam = bams[0]
        return {'bam': bam, 'title': sample_title, 'accession': sample_acc}

    # remove parent ids that are not in the sample_pedigree as individual
    samples_pedigree = remove_parents_without_sample(samples_pedigree)
    summary = []

    for member in ['proband', 'mother', 'father', 'brother', 'sister', 'sibling', 'half-brother', 'half-sister', 'half-sibling']:
        # if multiple relationships of same type are in the family they will have enumeration in the relationships
        # ie brother, brother II
        role_pedigree_infos = [i for i in samples_pedigree if i.get('relationship', '').split(' ')[0] == member]
        if not role_pedigree_infos:
            continue
        for a_role_pedigree in role_pedigree_infos:
            sample_summary = get_summary(a_role_pedigree, all_samples)
            summary.append(sample_summary)
    # for family members not listed on primary relations, get same information and continue appending
    all_sample_accs = [i['@id'].split('/')[2] for i in all_samples]
    seen_sample_accs = [i['accession'] for i in summary]
    remaining_samples = [i for i in all_sample_accs if i not in seen_sample_accs]

    for a_sample_acc in remaining_samples:
        a_role_pedigree = [i for i in samples_pedigree if i['sample_accession'] == a_sample_acc][0]
        sample_summary = get_summary(a_role_pedigree, all_samples)
        summary.append(sample_summary)
    bams = [i['bam'] for i in summary]
    titles = [i['title'] for i in summary]
    return bams, titles


def check_latest_workflow_version(workflows):
    """Some sanity checks for workflow versions
    expectations:
     - All workflows that we are currently active should be listed both on
       cgap_utils.py (workflow_details) and wfrset_cgap_utils.py (wf_dict)
     - The lastest workflow version on foursight (workflow_details) should be carried by the
       latest released workflow item on the data portal.
       If a new version is released on the portal, we need it to be on foursight too, if not stop the check.
       If a new version is decleared on foursight, it should be released on the portal, if not stop the check.
    """
    errors = []
    for a_wf in workflows:
        wf_name = a_wf['app_name']
        # make sure the workflow is in our control list on cgap_utils.py
        if wf_name not in workflow_details:
            continue
        # make sure the workflow is in our settings list on wfrset_cgap_utils.py
        if wf_name not in [i['app_name'] for i in wf_dict]:
            continue
        wf_info = workflow_details[wf_name]
        versions = wf_info['accepted_versions']
        # latest version should be the last one on the list
        last_version = versions[-1]
        # sometimes there are 2 or more workflows with same app name
        # and the old one might not have the latest version
        # look for all wfs with same name and make sure the latest version is on one
        same_wf_name_workflows = [i for i in workflows if i['app_name'] == wf_name]
        all_wf_versions = [i.get('app_version', '') for i in same_wf_name_workflows]
        # make sure the latest is also on one of the wfrs
        if last_version not in all_wf_versions:
            err = '{} version {} is not on any wf app_version)'.format(wf_name, last_version)
            errors.append(err)
            continue
        # fist item on same_wf_name_workflows should be the latest released workflow, check if we added that to foursight
        last_wf_version_on_portal = all_wf_versions[0]
        if last_wf_version_on_portal not in all_wf_versions:
            err = '{} version {} is not decleared on foursight)'.format(wf_name, last_wf_version_on_portal)
            errors.append(err)
            continue
        # check if the lastest version workflow uuids is correct on wfr_dict (wfrset_cgap_utils.py)
        latest_workflow_uuid = [i['uuid'] for i in same_wf_name_workflows if i['app_version'] == last_version][0]
        wf_dict_item = [i['workflow_uuid'] for i in wf_dict if i['app_name'] == wf_name][0]
        if latest_workflow_uuid != wf_dict_item:
            err = '{} item on wf_dict does not have the latest workflow uuid'.format(wf_name)
            errors.append(err)
            continue
    # return unique errors
    return list(set(errors))


def check_qcs_on_files(file_meta, all_qcs):
    """Go over qc related fields, and check for overall quality score."""
    def check_qc(file_accession, resp, failed_qcs_list):
        """format errors and return a errors list."""
        quality_score = resp.get('overall_quality_status', '')
        if quality_score.upper() != 'PASS':
            failed_qcs_list.append([file_accession, resp['display_title'], resp['uuid']])
        return failed_qcs_list

    failed_qcs = []
    if not file_meta.get('quality_metric'):
        return
    qc_result = [i for i in all_qcs if i['@id'] == file_meta['quality_metric']['@id']][0]
    if qc_result['display_title'].startswith('QualityMetricQclist'):
        if not qc_result.get('qc_list'):
            return
        for qc in qc_result['qc_list']:
            qc_resp = [i for i in all_qcs if i['@id'] == qc['value']['@id']][0]
            failed_qcs = check_qc(file_meta['accession'], qc_resp, failed_qcs)
    else:
        failed_qcs = check_qc(file_meta['accession'], qc_result, failed_qcs)
    return failed_qcs


def order_input_dictionary(input_file_dict):
    """Keep the order of file_arg keys in dictionary, but if the value is a list of files
    order them to be able to compare them"""
    ordered_input = {}
    for an_input_arg in input_file_dict:
        if isinstance(input_file_dict[an_input_arg], (list, tuple)):
            ordered_input[an_input_arg] = sorted(input_file_dict[an_input_arg])
        else:
            ordered_input[an_input_arg] = input_file_dict[an_input_arg]
    return ordered_input


def collect_input_files_from_input_dictionary(input_file_dict):
    """Collect all @ids from the input file dictionary"""
    all_inputs = []
    for an_input_arg in input_file_dict:
        if an_input_arg == 'additional_file_parameters':
            continue
        if isinstance(input_file_dict[an_input_arg], (list, tuple)):
            all_inputs.extend(input_file_dict[an_input_arg])
        else:
            all_inputs.append(input_file_dict[an_input_arg])
    return all_inputs


def collect_inputs_from_workflow_run(wfr_resp):
    """Given a wfr item in embedded frame, collect input files as a list of @ids"""
    input_files = []
    inputs = wfr_resp['input_files']
    for an_input in inputs:
        if an_input.get('value'):
            input_files.append(an_input['value']['@id'])
    return input_files


def remove_duplicate_need_runs(need_runs_dictionary):
    """In rare cases, the same run can be triggered by two different sample_processings
    example is a quad analyzed for 2 different probands. In this case you want a single
    combineGVCF step, but 2 sample_processings will be generated trying to run same job,
    identify and remove duplicates, used by PartII
    input_structure:
    [{"sample_processing_id": [
            # list of items per run
            ["step_id",  # name for the run
             ["workflow_app_name", "organism", "additional_parameters"],  # run settings
             {"input_arg1": ["file_id1", "file_id2"], "input_arg2": "file_id3"},  # run input files
             "GAPFI1RGV5US_GAPFIOASB96R_GAPFIVPH2MVG_GAPFICMTHU2P"  # all input files going in the run
             ]]}]
    """
    # keep a track of sorted runs
    sorted_runs = []
    unique_needs_runs_dictionary = []

    for an_item in need_runs_dictionary:
        for an_sp_id in an_item:
            keep_runs = []
            all_runs = an_item[an_sp_id]
            for a_run in all_runs:
                # sort input files
                run_setttings = a_run[1]
                input_files = a_run[2]
                ordered_input = order_input_dictionary(input_files)
                # get elements that should be unique and store as string
                run_info = str(run_setttings) + str(ordered_input)
                if run_info in sorted_runs:
                    continue
                else:
                    keep_runs.append(a_run)
                    sorted_runs.append(run_info)
            if keep_runs:
                unique_needs_runs_dictionary.append({an_sp_id: keep_runs})
    return unique_needs_runs_dictionary


def filter_wfrs_with_input_and_tag(all_wfr_items, app_name, input_file_dict, tag, match_all_input=True):
    """given an input file dictionary and list of workflow_run items, filter wfrs
    for input files that match the input file dictionary. If a filter tag is given
    also filter for workflow_runs that have the given filter_tag in tags field
    -- args
    all_wfr_items: all workflow run items collected from ES
    step_name: workflow_app_name
    input_file_dict: all input files
    tag: if filter should look for a tag (ie sample_processing uuid) on wfr
    match_all_input: (Bool) True by default. Looks for exact match between input file dict and the workflowrun
    # TODO: for combine GVCF, we need to implement the False case, where we utilize the partial match
    """
    # filter workflows for workflow_name, the ones with same input files, and if exist, tags
    # filtering with input - important for steps like combine gVCF there same input file
    #                        might have multiple runs of same type with different input
    #                        combinations, coming from different sample_procesing items.
    # filtering with tag - for some steps, even if the input files are the same,
    #                      you need to run different versions for different sample processing items
    #                      (ie 2 quads made up of the same samples with different probands.)

    # filter for app_name
    wfrs_with_app_name = [i for i in all_wfr_items if i['display_title'].startswith(app_name)]
    # filter for tag
    if tag:
        wfrs_with_tag = [i for i in wfrs_with_app_name if tag in i.get('tags', [])]
    else:
        wfrs_with_tag = wfrs_with_app_name
    # filter for input files
    # collect input files
    input_files = collect_input_files_from_input_dictionary(input_file_dict)
    # check for workflows with same inputs
    filtered_wfrs = []
    for a_wfr in wfrs_with_tag:
        # get all input files from the workflow item
        wfr_inputs = collect_inputs_from_workflow_run(a_wfr)
        # expectation is exact match of input files, if so pass the filter
        if match_all_input:
            if sorted(wfr_inputs) == sorted(input_files):
                filtered_wfrs.append(a_wfr)
        # if input files are contained by the wfr input files, pass the filter
        else:
            if all(file in wfr_inputs for file in input_files):
                filtered_wfrs.append(a_wfr)
    return filtered_wfrs


def check_input_structure_at_id(input_file_dict):
    """Check all input file strings and make sure they are @id format."""
    all_inputs = []
    error = ''
    for an_input_arg in input_file_dict:
        # skip the parameter key
        if an_input_arg == 'additional_file_parameters':
            continue
        inputs = input_file_dict[an_input_arg]
        # if the input is array
        if isinstance(inputs, list) or isinstance(inputs, tuple):
            all_inputs.extend(inputs)
        else:
            all_inputs.append(inputs)
    for an_input in all_inputs:
        if an_input.count('/') != 3:
            error += an_input + ' '
    if error:
        error += 'files are not @ids, foursight needs update'
        return [error, ]
    else:
        return []


def stepper(library, keep,
            step_tag, new_step_input_file,
            input_file_dict,  new_step_name, new_step_output_arg,
            additional_input=None, organism='human', no_output=False, tag=''):
    """This functions packs the core of wfr check, for a given workflow and set of
    input files, it will return the status of process on these files.
    It will also check for failed qcs on input files.
    - args
      -library:   dictionary with keys files/wfrs/qcs that contain all related items
      -keep:      tracking run progress with keys running/problematic_run/missing_run
      -step_tag:  informative summary used in the output (ie step name + input file accession)
      -new_step_input_file:  files to check for qc and get attribution from
      -input_file_dict:      all files and arguments used in the wfr
      -new_step_name:        workflow app_name
      -new_step_output_arg:  can be str or list, will return str or list of @id for output files with given argument(s)
      -additional_input:     overwrite or add to final json, example {"parameters": {'key': 'value'}, "config": {"key": "value"}}
      -organism:    by default human, used for adding genome assembly
      -no_output:   default False, should be True for workflows that don't produce an output file (ie QC runs)
      -tag:         if used, filter workflow_run items for the ones which have this as a tag, or add the tag when creating runs
                    (used for distinguishing sample_processing specific steps even when input files are the same (ie micro annotation))
    - returns
        - keep : same as input, with addition from this check
        - step_status : a short summary of this functions result (complete, running, no complete run)
    """
    if not additional_input:
        additional_input = {}
    step_output = ''
    # unpack library
    all_files = library['files']
    all_wfrs = library['wfrs']
    all_qcs = library['qcs']
    # unpack keep
    running = keep['running']
    problematic_run = keep['problematic_run']
    missing_run = keep['missing_run']

    # make sure input files are @ids, if not foursight needs an update, report it as error
    at_id_errors = check_input_structure_at_id(input_file_dict)

    # Lets get the repoinse from one of the input files that will be used in this step
    # if it is a list take the first item, if not use it as is
    # new_step_input_file must be the @id
    # also check for qc status
    qc_errors = []
    if isinstance(new_step_input_file, list) or isinstance(new_step_input_file, tuple):
        for an_input in new_step_input_file:
            input_resp = [i for i in all_files if i['@id'] == an_input][0]
            errors = check_qcs_on_files(input_resp, all_qcs)
            if errors:
                qc_errors.extend(errors)
        name_tag = '_'.join([i.split('/')[2] for i in new_step_input_file])
    else:
        input_resp = [i for i in all_files if i['@id'] == new_step_input_file][0]
        errors = check_qcs_on_files(input_resp, all_qcs)
        if errors:
            qc_errors.extend(errors)
        name_tag = new_step_input_file.split('/')[2]
    # if there are @id errors return it
    if at_id_errors:
        problematic_run.append([step_tag + ' foursight error', at_id_errors])
        step_status = "no complete run, foursight error"
    # if there are qc errors, return with qc qc_errors
    elif qc_errors:
        problematic_run.append([step_tag + ' input file qc error', qc_errors])
        step_status = "no complete run, qc error"
    # if no qc problem, go on with the run check
    else:
        # filter workflows for the ones with same input files, and if exist, tags
        # filtering with input - important for steps like combine gVCF there same input file
        #                        might have multiple runs of same type with different input
        #                        combinations, coming from different sample_procesing items.
        # filtering with tag - for some steps, even if the input files are the same,
        #                      you need to run different versions for different sample processing items
        #                      (ie 2 quads made up of the same samples with different probands.)
        filtered_wfrs = filter_wfrs_with_input_and_tag(all_wfrs, new_step_name, input_file_dict, tag=tag)

        if no_output:
            step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=filtered_wfrs, md_qc=True)
        else:
            step_result = get_wfr_out(input_resp, new_step_name, all_wfrs=filtered_wfrs)
        step_status = step_result['status']
        # if successful
        input_file_accession = input_resp['accession']
        if step_status == 'complete':
            if new_step_output_arg:
                if isinstance(new_step_output_arg, list):
                    output_list = []
                    for an_output_arg in new_step_output_arg:
                        output_list.append(step_result[an_output_arg])
                    step_output = output_list
                else:
                    step_output = step_result[new_step_output_arg]
            pass
        # if still running
        elif step_status == 'running':
            running.append([step_tag, input_file_accession])
        # if run is not successful
        elif step_status.startswith("no complete run, too many"):
            problematic_run.append([step_tag, input_file_accession])
        else:
            # add missing run
            # if there is a tag, pass it to the workflow_run metadata
            if tag:
                if not additional_input.get('wfr_meta'):
                    additional_input['wfr_meta'] = {'tags': [tag, ]}
                else:
                    additional_input['wfr_meta']['tags'] = [tag, ]
            missing_run.append([step_tag, [new_step_name, organism, additional_input], input_file_dict, name_tag])

    keep['running'] = running
    keep['problematic_run'] = problematic_run
    keep['missing_run'] = missing_run
    return keep, step_status, step_output


def get_wfr_out(emb_file, wfr_name, key=None, all_wfrs='not given', versions=None, md_qc=False, run=None):
    """For a given file, fetches the status of last wfr (of wfr_name type)
    If there is a successful run, it will return the output files as a dictionary of
    argument_name:file_id, else, will return the status. Some runs, like qc and md5,
    does not have any file_format output, so they will simply return 'complete'
    Can be run in two modes
     - Search through given wfr library on all_wfrs parameter, dont give any key
     - Search on the database without a given library (all_wfrs = not given and there should be a key) used by qcs
    args:
     emb_file: embedded frame file info
     wfr_name: base name without version
     key: authorization
     all_wfrs : all releated wfrs in embedded frame
                to distinguish
     versions: acceptable versions for wfr
     md_qc: if no output file is excepted, set to True
     run: if run is still running beyond this hour limit, assume problem
    """
    # sanity checks
    # we need key if all wfrs is not supplied (it can even be empty list but needs to be provided)
    if not key and all_wfrs == 'not given':
        raise ValueError('library or key is required for get_wfr_out function')

    error_at_failed_runs = 1
    # you should provide key or all_wfrs
    # assert key or all_wfrs
    assert wfr_name in workflow_details
    # get default accepted versions if not provided
    if not versions:
        versions = workflow_details[wfr_name]['accepted_versions']
    # get default run out time
    if not run:
        run = workflow_details[wfr_name]['run_time']
    workflows = emb_file.get('workflow_run_inputs', [])
    wfr = {}
    run_status = 'did not run'
    wfrs_on_file = [i for i in workflows if i['display_title'].startswith(wfr_name)]
    # if all_wfrs is not given, get workflows from the file
    if all_wfrs == 'not given':
        my_workflows = wfrs_on_file
    # otherwise, limit the workflows to the ones from all_wfrs
    else:
        library_uuids = [i['uuid'] for i in all_wfrs]
        my_workflows = [i for i in wfrs_on_file if i['uuid'] in library_uuids]
    if not my_workflows:
        return {'status': "no workflow on file"}
    # if all_wfrs were given and there were no wfrs, it means that prefiltering did not return any
    if not key and not all_wfrs:
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
    if all_wfrs == 'not given':
        wfr = ff_utils.get_metadata(last_wfr['uuid'], key)
    else:
        wfr = [i for i in all_wfrs if i['uuid'] == last_wfr['uuid']][0]
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
        if len(same_type_wfrs) >= error_at_failed_runs:
            return {'status': "no complete run, too many errors"}

        return {'status': "no complete run, errrored"}
    # if other statuses, started running
    elif run_duration < run:
        return {'status': "running"}
    # this should be the timeout case
    else:
        if len(same_type_wfrs) >= error_at_failed_runs:
            return {'status': "no complete run, too many time-outs"}
        else:
            return {'status': "no completed run, time-out"}


def get_attribution(file_json):
    """give file response in embedded frame and extract attribution info"""
    attributions = {
        'project': file_json['project']['@id'],
        'institution': file_json['institution']['@id']
    }
    return attributions


def extract_file_info(obj_id, arg_name, additional_parameters, auth, env, rename=[]):
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
        if additional_parameters:
            template.update(additional_parameters)

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
        if additional_parameters:
            template.update(additional_parameters)
    return template


def start_missing_run(run_info, auth, env):
    # arguments for finding the file with the attribution (as opposed to reference files)
    attr_keys = ['fastq1', 'fastq', 'input_pairs', 'input_bams',
                 'fastq_R1', 'input_bam', 'input_gvcf', 'cram',
                 'input_gvcfs', 'input_rcks', 'input_vcf',
                 '']
    run_settings = run_info[1]
    inputs = run_info[2]
    name_tag = run_info[3]
    # find file to use for attribution
    attr_file = ''
    for attr_key in attr_keys:
        if attr_key in inputs:
            attr_file = inputs[attr_key]
            if isinstance(attr_file, list):
                attr_file = attr_file[0]
            break
    if not attr_file:
        possible_keys = [i for i in inputs.keys() if i != 'additional_file_parameters']
        error_message = ('one of these argument names {} which carry the input file -not the references-'
                         ' should be added to att_keys dictionary on foursight cgap_utils.py function start_missing_run').format(possible_keys)
        raise ValueError(error_message)
    attributions = get_attribution(ff_utils.get_metadata(attr_file, auth))
    settings = wfrset_cgap_utils.step_settings(run_settings[0], run_settings[1], attributions, run_settings[2])
    url = run_missing_wfr(settings, inputs, name_tag, auth, env)
    return url


def run_missing_wfr(input_json, input_files_and_params, run_name, auth, env):
    all_inputs = []
    # input_files container
    input_files = {k: v for k, v in input_files_and_params.items() if k != 'additional_file_parameters'}
    # additional input file parameters
    input_file_parameters = input_files_and_params.get('additional_file_parameters', {})
    for arg, files in input_files.items():
        additional_params = input_file_parameters.get(arg, {})
        inp = extract_file_info(files, arg, additional_params, auth, env)
        all_inputs.append(inp)
    # tweak to get bg2bw working
    all_inputs = sorted(all_inputs, key=itemgetter('workflow_argument_name'))
    my_s3_util = s3Utils(env=env)
    out_bucket = my_s3_util.outfile_bucket
    # shorten long name_tags
    # they get combined with workflow name, and total should be less then 80
    # (even less since repeats need unique names)
    if len(run_name) > 30:
        run_name = run_name[:30] + '...'
    """Creates the trigger json that is used by foufront endpoint.
    """
    input_json['input_files'] = all_inputs
    input_json['output_bucket'] = out_bucket
    input_json["_tibanna"] = {
        "env": env,
        "run_type": input_json['app_name'],
        "run_id": run_name}
    # input_json['env_name'] = CGAP_ENV_WEBPROD  # e.g., 'fourfront-cgap'
    input_json['step_function_name'] = 'tibanna_zebra'
    input_json['public_postrun_json'] = True
    try:
        e = ff_utils.post_metadata(input_json, 'WorkflowRun/run', key=auth)
        url = json.loads(e['input'])['_tibanna']['url']
        return url
    except Exception as e:
        return str(e)


def check_runs_without_output(res, check, run_name, my_auth, start):
    """Common processing for checks that are running on files and not producing output files
    like qcs ones producing extra files"""
    # no successful run
    missing_run = []
    # successful run but no expected metadata change (qc or extra file)
    missing_meta_changes = []
    # still running
    running = []
    # multiple failed runs
    problems = []

    for a_file in res:
        # lambda has a time limit (300sec), kill before it is reached so we get some results
        now = datetime.utcnow()
        if (now-start).seconds > lambda_limit:
            check.brief_output.append('did not complete checking all')
            break
        file_id = a_file['accession']
        report = get_wfr_out(a_file, run_name,  key=my_auth, md_qc=True)
        if report['status'] == 'running':
            running.append(file_id)
        elif report['status'].startswith("no complete run, too many"):
            problems.append(file_id)
        elif report['status'] != 'complete':
            missing_run.append(file_id)
        # There is a successful run, but no extra_file
        elif report['status'] == 'complete':
            missing_meta_changes.append(file_id)
    if running:
        check.summary = 'Some files are running'
        check.brief_output.append(str(len(running)) + ' files are still running.')
        check.full_output['running'] = running
    if problems:
        check.summary = 'Some files have problems'
        check.brief_output.append(str(len(problems)) + ' files have multiple failed runs')
        check.full_output['problems'] = problems
        check.status = 'WARN'
    if missing_run:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_run)) + ' files lack a successful run')
        check.full_output['files_without_run'] = missing_run
        check.status = 'WARN'
    if missing_meta_changes:
        check.allow_action = True
        check.summary = 'Some files are missing runs'
        check.brief_output.append(str(len(missing_meta_changes)) + ' files have successful run but no qc/extra file')
        check.full_output['files_without_changes'] = missing_meta_changes
        check.status = 'WARN'
    check.summary = check.summary.strip()
    if not check.brief_output:
        check.brief_output = ['All Good!']
    return check


def find_fastq_info(my_sample, fastq_files, organism='human'):
    """Find fastq files from sample
    expects my_rep_set to be set response in frame object (search result)
    will check if files are paired or not, and if paired will give list of lists for sample
    if not paired, with just give list of files for sample.

    result is 2 lists
    - file [file1, file2, file3, file4]  # unpaired
      file [ [file1, file2], [file3, file4]] # paired
    - refs keys  {pairing, organism, bwa_ref, f_size}
    """
    # # TODO: re word for samples
    files = []
    refs = {}
    # check pairing for the first file, and assume all same
    paired = ""
    # check if files are FileFastq or FileProcessed
    f_type = ""
    total_f_size = 0
    sample_files = my_sample['files']
    # Assumption: Fastq files are either all FileFastq or File processed
    # File Processed ones don't have paired end information
    # Assumption: File Processed fastq files are paired end in the order they are in sample files
    types = [i['@id'].split('/')[1] for i in fastq_files]
    f_type = list(set(types))
    msg = '{} has mixed fastq files types {}'.format(my_sample['accession'], f_type)
    assert len(f_type) == 1, msg
    f_type = f_type[0]

    if f_type == 'files-processed':
        for fastq_file in sample_files:
            file_resp = [i for i in fastq_files if i['uuid'] == fastq_file['uuid']][0]
            if file_resp.get('file_size'):
                total_f_size += file_resp['file_size']
        # we are assuming that this files are processed
        # # TODO: make sure that this is encoded in the metadata
        paired = 'Yes'
        file_ids = [i['@id'] for i in sample_files]
        files = [file_ids[i:i+2] for i in range(0, len(file_ids), 2)]

    elif f_type == 'files-fastq':
        for fastq_file in sample_files:
            file_resp = [i for i in fastq_files if i['uuid'] == fastq_file['uuid']][0]
            if file_resp.get('file_size'):
                total_f_size += file_resp['file_size']
            # skip pair no 2
            if file_resp.get('paired_end') == '2':
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
                files.append(f1)
            elif paired == 'Yes':
                relations = file_resp['related_files']
                paired_files = [relation['file']['@id'] for relation in relations
                                if relation['relationship_type'] == 'paired with']
                assert len(paired_files) == 1
                f2 = paired_files[0]
                files.append((f1, f2))
    bwa = bwa_index.get(organism)
    # chrsize = chr_size.get(organism)

    f_size = int(total_f_size / (1024 * 1024 * 1024))
    refs = {'pairing': paired,
            'organism': organism,
            'bwa_ref': bwa,
            'f_size': str(f_size)+'GB'}
    return files, refs


def start_tasks(missing_runs, patch_meta, action, my_auth, my_env, start, move_to_pc=False, runtype='hic'):
    started_runs = 0
    patched_md = 0
    action.description = ""
    action_log = {'started_runs': [], 'failed_runs': [], 'patched_meta': [], 'failed_meta': []}
    if missing_runs:
        for a_case in missing_runs:
            now = datetime.utcnow()
            acc = list(a_case.keys())[0]
            print((now-start).seconds, acc)
            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break

            for a_run in a_case[acc]:
                started_runs += 1
                url = start_missing_run(a_run, my_auth, my_env)
                log_message = acc + ' started running ' + a_run[0] + ' with ' + a_run[3]
                if url.startswith('http'):
                    action_log['started_runs'].append([log_message, url])
                else:
                    action_log['failed_runs'].append([log_message, url])
    if patch_meta:
        action_log['patched_meta'] = []
        for a_completed_info in patch_meta:
            exp_acc = a_completed_info[0]
            patch_body = a_completed_info[1]
            now = datetime.utcnow()
            if (now-start).seconds > lambda_limit:
                action.description = 'Did not complete action due to time limitations.'
                break
            patched_md += 1
            ff_utils.patch_metadata(patch_body, exp_acc, my_auth)
            action_log['patched_meta'].append(exp_acc)

    # did we complete without running into time limit
    for k in action_log:
        if action_log[k]:
            add_desc = "| {}: {} ".format(k, str(len(action_log[k])))
            action.description += add_desc

    action.output = action_log
    action.status = 'DONE'
    return action


def is_there_my_qc_metric(file_meta, qc_metric_name, my_auth):
    if not file_meta.get('quality_metric'):
        return False
    qc_results = ff_utils.get_metadata(file_meta['quality_metric']['uuid'], key=my_auth)
    if qc_results['display_title'].startswith('QualityMetricQclist'):
        if not qc_results.get('qc_list'):
            return False
        for qc in qc_results['qc_list']:
            if qc_metric_name not in qc['value']['display_title']:
                return False
    else:
        if qc_metric_name not in qc_results['display_title']:
            return False
    return True


def fetch_wfr_associated(wfr_info):
    """Given wfr embedded frame, find associated output files and qcs"""
    wfr_as_list = []
    wfr_as_list.append(wfr_info['uuid'])
    if wfr_info.get('output_files'):
        for o in wfr_info['output_files']:
            if o.get('value'):
                wfr_as_list.append(o['value']['uuid'])
            elif o.get('value_qc'):
                wfr_as_list.append(o['value_qc']['uuid'])
    if wfr_info.get('output_quality_metrics'):
        for qc in wfr_info['output_quality_metrics']:
            if qc.get('value'):
                wfr_as_list.append(qc['value']['uuid'])
    if wfr_info.get('quality_metric'):
        wfr_as_list.append(wfr_info['quality_metric']['uuid'])
    return list(set(wfr_as_list))


def string_to_list(string):
    "Given a string that is either comma separated values, or a python list, parse to list"
    for a_sep in "'\":[] ":
        values = string.replace(a_sep, ",")
    values = [i.strip() for i in values.split(',') if i]
    return values
