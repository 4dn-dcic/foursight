from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils, es_utils
import re
import requests
import datetime
import json


def compare_badges(obj_ids, item_type, badge, ffenv):
    '''
    Compares items that should have a given badge to items that do have the given badge.
    Used for badges that utilize a single message choice.
    Input (first argument) should be a list of item @ids.
    '''
    search_url = 'search/?type={}&badges.badge.@id=/badges/{}/'.format(item_type, badge)
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', ff_env=ffenv)
    needs_badge = []
    badge_ok = []
    remove_badge = {}
    for item in has_badge:
        if item['@id'] in obj_ids:
            # handle differences in badge messages
            badge_ok.append(item['@id'])
        else:
            keep = [badge_dict for badge_dict in item['badges'] if badge not in badge_dict['badge']]
            remove_badge[item['@id']] = keep
    for other_item in obj_ids:
        if other_item not in badge_ok:
            needs_badge.append(other_item)
    return needs_badge, remove_badge, badge_ok


def compare_badges_and_messages(obj_id_dict, item_type, badge, ffenv):
    '''
    Compares items that should have a given badge to items that do have the given badge.
    Also compares badge messages to see if the message is the right one or needs to be updated.
    Input (first argument) should be a dictionary of item's @id and the badge message it should have.
    '''
    search_url = 'search/?type={}&badges.badge.@id=/badges/{}/'.format(item_type, badge)
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', ff_env=ffenv)
    needs_badge = {}
    badge_edit = {}
    badge_ok = []
    remove_badge = {}
    for item in has_badge:
        if item['@id'] in obj_id_dict.keys():
            # handle differences in badge messages
            for a_badge in item['badges']:
                if a_badge['badge'].endswith(badge + '/'):
                    if a_badge['message'] == obj_id_dict[item['@id']]:
                        badge_ok.append(item['@id'])
                    else:
                        a_badge['message'] = obj_id_dict[item['@id']]
                        badge_edit[item['@id']] = item['badges']
        else:
            this_badge = [a_badge for a_badge in item['badges'] if badge in a_badge['badge']][0]
            item['badges'].remove(this_badge)
            remove_badge[item['@id']] = item['badges']
    for key, val in obj_id_dict.items():
        if key not in badge_ok + list(badge_edit.keys()):
            needs_badge[key] = val
    return needs_badge, remove_badge, badge_edit, badge_ok


def patch_badges(full_output, badge_name, output_keys, ffenv, single_message=''):
    '''
    General function for patching badges.
    For badges with single message choice:
    - single_message kwarg should be assigned a string to be used for the badge message;
    - full_output[output_keys[0]] should be a list of item @ids;
    - no badges are edited, they are only added or removed.
    For badges with multiple message options:
    - single_message kwarg should not be used, but left as empty string.
    - full_output[output_keys[0]] should be a list of item @ids and message to patch into badge.
    - badges can also be edited to change the message.
    '''
    patches = {'add_badge_success': [], 'add_badge_failure': [],
               'remove_badge_success': [], 'remove_badge_failure': []}
    badge_id = '/badges/' + badge_name + '/'
    if isinstance(full_output[output_keys[0]], list):
        add_list = full_output[output_keys[0]]
    elif isinstance(full_output[output_keys[0]], dict):
        patches['edit_badge_success'] = []
        patches['edit_badge_failure'] = []
        add_list = full_output[output_keys[0]].keys()
    for add_key in add_list:
        add_result = ff_utils.get_metadata(add_key + '?frame=object', ff_env=ffenv)
        badges = add_result['badges'] if add_result.get('badges') else []
        badges.append({'badge': badge_id, 'message': single_message if single_message else full_output[output_keys[0]][add_key]})
        if [b['badge'] for b in badges].count(badge_id) > 1:
            # print an error message?
            patches['add_badge_failure'].append('{} already has badge'.format(add_key))
            continue
        try:
            response = ff_utils.patch_metadata({"badges": badges}, add_key[1:], ff_env=ffenv)
            if response['status'] == 'success':
                patches['add_badge_success'].append(add_key)
            else:
                patches['add_badge_failure'].append(add_key)
        except Exception:
            patches['add_badge_failure'].append(add_key)
    for remove_key, remove_val in full_output[output_keys[1]].items():
        # delete field if no badges?
        try:
            if remove_val:
                response = ff_utils.patch_metadata({"badges": remove_val}, remove_key, ff_env=ffenv)
            else:
                response = ff_utils.patch_metadata({}, remove_key + '?delete_fields=badges', ff_env=ffenv)
            if response['status'] == 'success':
                patches['remove_badge_success'].append(remove_key)
            else:
                patches['remove_badge_failure'].append(remove_key)
        except Exception:
            patches['remove_badge_failure'].append(remove_key)
    if len(output_keys) > 2:
        for edit_key, edit_val in full_output[output_keys[2]].items():
            try:
                response = ff_utils.patch_metadata({"badges": edit_val}, edit_key, ff_env=ffenv)
                if response['status'] == 'success':
                    patches['edit_badge_success'].append(edit_key)
                else:
                    patches['edit_badge_failure'].append(edit_key)
            except Exception:
                patches['edit_badge_failure'].append(edit_key)
    return patches


@check_function()
def biosource_cell_line_value(connection, **kwargs):
    '''
    checks cell line biosources to make sure they have an associated ontology term
    '''
    check = init_check_res(connection, 'biosource_cell_line_value')

    cell_line_types = ["primary cell", "primary cell line", "immortalized cell line",
                       "in vitro differentiated cells", "induced pluripotent stem cell line",
                       "stem cell", "stem cell derived cell line"]
    biosources = ff_utils.search_metadata('search/?type=Biosource&frame=object',
                                          ff_env=connection.ff_env, page_limit=200)
    missing = []
    for biosource in biosources:
        # check if the biosource type is a cell/cell line
        if biosource.get('biosource_type') and biosource.get('biosource_type') in cell_line_types:
            # append if cell_line field is missing
            if not biosource.get('cell_line'):
                missing.append({'uuid': biosource['uuid'],
                                '@id': biosource['@id'],
                                'biosource_type': biosource.get('biosource_type'),
                                'description': biosource.get('description'),
                                'error': 'Missing cell_line metadata'})
    check.full_output = missing
    check.brief_output = [item['uuid'] for item in missing]
    if missing:
        check.status = 'WARN'
        check.summary = 'Cell line biosources found missing cell_line metadata'
    else:
        check.status = 'PASS'
        check.summary = 'No cell line biosources are missing cell_line metadata'
    return check


@check_function()
def external_expsets_without_pub(connection, **kwargs):
    '''
    checks external experiment sets to see if they are attributed to a publication
    '''
    check = init_check_res(connection, 'external_expsets_without_pub')

    ext = ff_utils.search_metadata('search/?award.project=External&type=ExperimentSet&frame=object',
                                   ff_env=connection.ff_env, page_limit=50)
    no_pub = []
    for expset in ext:
        if not expset.get('publications_of_set') and not expset.get('produced_in_pub'):
            no_pub.append({'uuid': expset['uuid'],
                           '@id': expset['@id'],
                           'description': expset.get('description'),
                           'lab': expset.get('lab'),
                           'error': 'Missing attribution to a publication'})
    if no_pub:
        check.status = 'WARN'
        check.summary = 'External experiment sets found without associated publication. Searched %s' % len(ext)
        check.description = '{} external experiment sets are missing attribution to a publication.'.format(len(no_pub))
    else:
        check.status = 'PASS'
        check.summary = 'No external experiment sets are missing publication. Searched %s' % len(ext)
        check.description = '0 external experiment sets are missing attribution to a publication.'
    check.full_output = no_pub
    check.brief_output = [item['uuid'] for item in no_pub]
    return check


@check_function()
def expset_opfsets_unique_titles(connection, **kwargs):
    '''
    checks experiment sets with other_processed_files to see if each collection
    of other_processed_files has a unique title within that experiment set
    '''
    check = init_check_res(connection, 'expset_opfsets_unique_titles')

    opf_expsets = ff_utils.search_metadata('search/?type=ExperimentSet&other_processed_files.files.uuid%21=No+value&frame=object',
                                           ff_env=connection.ff_env, page_limit=50)
    errors = []
    for expset in opf_expsets:
        e = []
        fileset_names = [fileset.get('title') for fileset in expset['other_processed_files']]
        if None in fileset_names or '' in fileset_names:
            e.append('Missing title')
        if len(list(set(fileset_names))) != len(fileset_names):
            e.append('Duplicate title')
        if e:
            info = {'uuid': expset['uuid'],
                    '@id': expset['@id'],
                    'errors': []}
            if 'Missing title' in e:
                info['errors'] += ['ExperimentSet {} has an other_processed_files collection with a missing title.'.format(expset['accession'])]
            if 'Duplicate title' in e:
                info['errors'] += ['ExperimentSet {} has 2+ other_processed_files collections with duplicated titles.'.format(expset['accession'])]
            errors.append(info)

    if errors:
        check.status = 'WARN'
        check.summary = 'Experiment Sets found with duplicate/missing titles in other_processed_files'
        check.description = '{} Experiment Sets have other_processed_files collections with missing or duplicate titles.'.format(len(errors))
    else:
        check.status = 'PASS'
        check.summary = 'No issues found with other_processed_files of experiment sets'
        check.description = '0 Experiment Sets have other_processed_files collections with missing or duplicate titles.'
    check.full_output = errors
    check.brief_output = {'missing title': [item['uuid'] for item in errors if 'missing' in ''.join(item['errors'])],
                          'duplicate title': [item['uuid'] for item in errors if 'duplicated' in ''.join(item['errors'])]}
    return check


@check_function()
def expset_opf_unique_files_in_experiments(connection, **kwargs):
    '''
    checks experiment sets with other_processed_files and looks for other_processed_files collections
    in child experiments to make sure that (1) the collections have titles and (2) that if the titles
    are shared with the parent experiment set, that the filenames contained within are unique
    '''
    check = init_check_res(connection, 'expset_opf_unique_files_in_experiments')

    opf_expsets = ff_utils.search_metadata('search/?type=ExperimentSet&other_processed_files.files.uuid%21=No+value',
                                           ff_env=connection.ff_env, page_limit=25)
    errors = []
    for expset in opf_expsets:
        expset_titles = {fileset.get('title'): fileset.get('files') for fileset in expset['other_processed_files'] if fileset.get('title')}
        if not expset.get('experiments_in_set'):
            continue
        for expt in (exp for exp in expset.get('experiments_in_set') if exp.get('other_processed_files')):
            e = []
            for opf_set in expt['other_processed_files']:
                # look for missing names
                if not opf_set.get('title'):
                    e.append('Experiment {} in Experiment Set {} has an other_processed_files set '
                             'missing a title.'.format(expt['accession'], expset['accession']))
                # look for duplicate names
                elif opf_set.get('title') in expset_titles.keys() and opf_set.get('files'):
                    for opf_file in opf_set['files']:
                        # if duplicate names, look for duplicate file names
                        if opf_file in expset_titles[opf_set['title']]:
                            e.append('Experiment {} other_processed_files collection with title `{}` has file {} which '
                                     'is also present in parent ExperimentSet {} other_processed_files collection of the '
                                     'same name.'.format(expt['accession'], opf_set['title'], opf_file['accession'], expset['accession']))
            if e:
                errors.append({'uuid': expt['uuid'],
                               '@id': expt['@id'],
                               'error_details': e})
    if errors:
        check.status = 'WARN'
        check.summary = '{} experiments found with issues in other_processed_files'.format(len(errors))
        check.description = ('{} Experiments found that are either missing titles for sets of other_processed_files,'
                             ' or have non-uniquefilenames in other_processed_files'.format(len(errors)))
    else:
        check.status = 'PASS'
        check.summary = 'No issues found with other_processed_files of experiments'
        check.description = ('0 Experiments found to be missing titles for sets of other_processed_files,'
                             ' or have non-unique filenames in other_processed_files')
    check.full_output = errors
    check.brief_output = {'missing title': [item['uuid'] for item in errors if 'missing' in ''.join(item['error_details'])],
                          'duplicate title': [item['uuid'] for item in errors if 'also present in parent' in ''.join(item['error_details'])]}
    return check


@check_function()
def workflow_properties(connection, **kwargs):
    check = init_check_res(connection, 'workflow_properties')

    workflows = ff_utils.search_metadata('search/?type=Workflow&category!=provenance&frame=object',
                                         ff_env=connection.ff_env)
    bad = {'Duplicate Input Names in Workflow Step': [],
           'Duplicate Output Names in Workflow Step': [],
           'Duplicate Input Source Names in Workflow Step': [],
           'Duplicate Output Target Names in Workflow Step': [],
           'Missing meta.file_format property in Workflow Step Input': [],
           'Missing meta.file_format property in Workflow Step Output': []}
    by_wf = {}
    for wf in workflows:
        # print(wf['@id'])
        issues = []
        for step in wf.get('steps'):
            # no duplicates in input names
            step_inputs = step.get('inputs')
            for step_input in step_inputs:
                if (step_input['meta'].get('type') in ['data file', 'reference file'] and not
                    step_input['meta'].get('file_format')):
                    issues.append('Missing meta.file_format property in Workflow Step `{}` Input `{}`'
                                  ''.format(step.get('name'), step_input.get('name')))
            input_names = [step_input.get('name') for step_input in step_inputs]
            if len(list(set(input_names))) != len(input_names):
                issues.append('Duplicate Input Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in input source names
            sources = [(source.get('name'), source.get('step', "GLOBAL")) for
                       step_input in step_inputs for source in step_input.get('source')]
            if len(sources) != len(list(set(sources))):
                issues.append('Duplicate Input Source Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in output names
            step_outputs = step.get('outputs')
            for step_output in step_outputs:
                if (step_output['meta'].get('type') in ['data file', 'reference file'] and not
                    step_output['meta'].get('file_format')):
                    issues.append('Missing meta.file_format property in Workflow Step `{}` Output `{}`'
                                  ''.format(step.get('name'), step_output.get('name')))
            output_names = [step_output.get('name') for step_output in step_outputs]
            if len(list(set(output_names))) != len(output_names):
                issues.append('Duplicate Output Names in Workflow Step {}'.format(step.get('name')))
            # no duplicates in output target names
            targets = [(target.get('name'), target.get('step', 'GLOBAL')) for step_output in
                       step_outputs for target in step_output.get('target')]
            if len(targets) != len(list(set(targets))):
                issues.append('Duplicate Output Target Names in Workflow Step {}'.format(step.get('name')))
        if not issues:
            continue
        errors = ' '.join(issues)
        if 'Duplicate Input Names' in errors:
            bad['Duplicate Input Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Output Names' in errors:
            bad['Duplicate Output Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Input Source Names' in errors:
            bad['Duplicate Input Source Names in Workflow Step'].append(wf['@id'])
        if 'Duplicate Output Target Names' in errors:
            bad['Duplicate Output Target Names in Workflow Step'].append(wf['@id'])
        if '` Input `' in errors:
            bad['Missing meta.file_format property in Workflow Step Input'].append(wf['@id'])
        if '` Output `' in errors:
            bad['Missing meta.file_format property in Workflow Step Output'].append(wf['@id'])
        by_wf[wf['@id']] = issues

    if by_wf:
        check.status = 'WARN'
        check.summary = 'Workflows found with issues in `steps`'
        check.description = ('{} workflows found with duplicate item names or missing fields'
                             ' in `steps`'.format(len(by_wf.keys())))
    else:
        check.status = 'PASS'
        check.summary = 'No workflows with issues in `steps` field'
        check.description = ('No workflows found with duplicate item names or missing fields'
                             ' in steps property')
    check.brief_output = bad
    check.full_output = by_wf
    return check


@check_function()
def repsets_have_bio_reps(connection, **kwargs):
    check = init_check_res(connection, 'repsets_have_bio_reps')

    results = ff_utils.search_metadata('search/?type=ExperimentSetReplicate&frame=object',
                                       ff_env=connection.ff_env, page_limit=50)

    audits = {'single_experiment': [], 'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []}
    by_exp = {}
    for result in results:
        rep_dict = {}
        exp_audits = []
        if result.get('replicate_exps'):
            rep_dict = {}
            for exp in result['replicate_exps']:
                if exp['bio_rep_no'] in rep_dict.keys():
                    rep_dict[exp['bio_rep_no']].append(exp['tec_rep_no'])
                else:
                    rep_dict[exp['bio_rep_no']] = [exp['tec_rep_no']]
        if rep_dict:
        # check if only 1 experiment present in set
            if len(result['replicate_exps']) == 1:
                audits['single_experiment'].append('{} contains only 1 experiment'.format(result['@id']))
                exp_audits.append('Replicate set contains only a single experiment')
            # check for technical replicates only
            elif len(rep_dict.keys()) == 1:
                audits['single_biorep'].append('{} contains only a single biological replicate'.format(result['@id']))
                exp_audits.append('Replicate set contains only a single biological replicate')
            # check if bio rep numbers not in sequence
            elif sorted(list(rep_dict.keys())) != list(range(min(rep_dict.keys()), max(rep_dict.keys()) + 1)):
                audits['biorep_nums'].append('Biological replicate numbers of {} are not in sequence:'
                                             ' {}'.format(result['@id'], str(sorted(list(rep_dict.keys())))))
                exp_audits.append('Biological replicate numbers are not in sequence')
        # check if tech rep numbers not in sequence
            else:
                for key, val in rep_dict.items():
                    if sorted(val) != list(range(min(val), max(val) + 1)):
                        audits['techrep_nums'].append('Technical replicates of Bio Rep {} in {} are not in '
                                                      'sequence {}'.format(key, result['@id'], str(sorted(val))))
                        exp_audits.append('Technical replicate numbers of biological replicate {}'
                                          ' are not in sequence'.format(key))
        if exp_audits:
            by_exp[result['@id']] = '; '.join(exp_audits)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(by_exp, 'ExperimentSetReplicate',
                                                                 'replicatenumbers', connection.ff_env)
    check.action = 'patch_badges_for_replicate_numbers'
    if by_exp:
        check.status = 'WARN'
        check.summary = 'Replicate experiment sets found with replicate number issues'
        check.description = '{} replicate experiment sets found with replicate number issues'.format(len(by_exp.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'No replicate experiment sets found with replicate number issues'
        check.description = '0 replicate experiment sets found with replicate number issues'
    check.full_output = {'New replicate sets with replicate number issues': to_add,
                         'Old replicate sets with replicate number issues': ok,
                         'Replicate sets that no longer have replicate number issues': to_remove,
                         'Replicate sets with a replicate_numbers badge that needs editing': to_edit}
    check.brief_output = audits
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_replicate_numbers(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_replicate_numbers')

    rep_check = init_check_res(connection, 'repsets_have_bio_reps')
    rep_check_result = rep_check.get_result_by_uuid(kwargs['called_by'])

    rep_keys = ['New replicate sets with replicate number issues',
                'Replicate sets that no longer have replicate number issues',
                'Replicate sets with a replicate_numbers badge that needs editing']

    action.output = patch_badges(rep_check_result['full_output'], 'replicatenumbers', rep_keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for replicate numbers'
    return action


@check_function()
def tier1_metadata_present(connection, **kwargs):
    check = init_check_res(connection, 'tier1_metadata_present')

    results = ff_utils.search_metadata('search/?biosource.cell_line_tier=Tier+1&type=Biosample',
                                       ff_env=connection.ff_env)
    missing = {}
    msg_dict = {'culture_start_date': 'Tier 1 Biosample missing Culture Start Date',
                # item will fail validation if missing a start date - remove this part of check?
                'culture_duration': 'Tier 1 Biosample missing Culture Duration',
                'morphology_image': 'Tier 1 Biosample missing Morphology Image'}
    for result in results:
        if len(result.get('biosource')) != 1:
            continue
        elif not result.get('cell_culture_details'):
            missing[result['@id']] = 'Tier 1 Biosample missing Cell Culture Details'
        else:
            messages = [val for key, val in msg_dict.items() if not result['cell_culture_details'].get(key)]
            if messages:
                missing[result['@id']] = '; '.join(messages)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(missing, 'Biosample',
                                                                 'tier1metadatamissing',
                                                                 connection.ff_env)
    check.action = 'patch_badges_for_tier1_metadata'
    if missing:
        check.status = 'WARN'
        check.summary = 'Tier 1 biosamples found missing required metadata'
        check.description = '{} tier 1 biosamples found missing required metadata'.format(len(missing.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'All Tier 1 biosamples have required metadata'
        check.description = '0 tier 1 biosamples found missing required metadata'
    check.full_output = {'New tier1 biosamples missing required metadata': to_add,
                         'Old tier1 biosamples missing required metadata': ok,
                         'Tier1 biosamples no longer missing required metadata': to_remove,
                         'Biosamples with a tier1_metadata_missing badge that needs editing': to_edit}
    check.brief_output = list(missing.keys())
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_tier1_metadata(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_tier1_metadata')

    tier1_check = init_check_res(connection, 'tier1_metadata_present')
    tier1_check_result = tier1_check.get_result_by_uuid(kwargs['called_by'])

    tier1keys = ['New tier1 biosamples missing required metadata',
                 'Tier1 biosamples no longer missing required metadata',
                 'Biosamples with a tier1_metadata_missing badge that needs editing']

    action.output = patch_badges(tier1_check_result['full_output'], 'tier1metadatamissing', tier1keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for missing tier1 metadata.'
    return action


@check_function()
def exp_has_raw_files(connection, **kwargs):
    check = init_check_res(connection, 'exp_has_raw_files')
    # search all experiments except microscopy experiments for missing files field
    no_files = ff_utils.search_metadata('search/?type=Experiment&%40type%21=ExperimentMic&files.uuid=No+value',
                                        ff_env=connection.ff_env)
    # also check sequencing experiments whose files items are all uploading/archived/deleted
    bad_status = ff_utils.search_metadata('search/?status=uploading&status=archived&status=deleted&status=upload+failed'
                                          '&type=FileFastq&experiments.uuid%21=No+value',
                                          ff_env=connection.ff_env)
    bad_status_ids = [item['@id'] for item in bad_status]
    exps = list(set([exp['@id'] for fastq in bad_status for exp in
                     fastq.get('experiments') if fastq.get('experiments')]))
    missing_files = [e['@id'] for e in no_files]
    for expt in exps:
        result = ff_utils.get_metadata(expt, ff_env=connection.ff_env)
        if result.get('status') == 'archived':
            continue
        raw_files = False
        if result.get('files'):
            for fastq in result.get('files'):
                if fastq['@id'] not in bad_status_ids:
                    raw_files = True
                    break
        if not raw_files:
            missing_files.append(expt)

    to_add, to_remove, ok = compare_badges(missing_files, 'Experiment', 'norawfiles', connection.ff_env)

    if missing_files:
        check.status = 'WARN'
        check.summary = 'Experiments missing raw files found'
        check.description = '{} sequencing experiments are missing raw files'.format(len(missing_files))
    else:
        check.status = 'PASS'
        check.summary = 'No experiments missing raw files'
        check.description = '0 sequencing experiments are missing raw files'
    check.action = 'patch_badges_for_raw_files'
    check.full_output = {'Experiments newly missing raw files': to_add,
                         'Old experiments missing raw files': ok,
                         'Experiments no longer missing raw files': to_remove}
    check.brief_output = missing_files
    if to_add or to_remove:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_raw_files(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_raw_files')

    raw_check = init_check_res(connection, 'exp_has_raw_files')
    raw_check_result = raw_check.get_result_by_uuid(kwargs['called_by'])

    raw_keys = ['Experiments newly missing raw files', 'Experiments no longer missing raw files']

    action.output = patch_badges(raw_check_result['full_output'], 'norawfiles', raw_keys,
                                 connection.ff_env, single_message='Raw files missing')
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for experiments missing raw files.'
    return action


@check_function()
def paired_end_info_consistent(connection, **kwargs):
    # check that fastqs with a paired_end number have a paired_with related_file, and vice versa
    check = init_check_res(connection, 'paired_end_info_consistent')

    search1 = 'search/?type=FileFastq&related_files.relationship_type=paired+with&paired_end=No+value'
    search2 = 'search/?type=FileFastq&related_files.relationship_type!=paired+with&paired_end%21=No+value'

    results1 = ff_utils.search_metadata(search1 + '&frame=object', ff_env=connection.ff_env)
    results2 = ff_utils.search_metadata(search2 + '&frame=object', ff_env=connection.ff_env)

    results = {'paired with file missing paired_end number':
               [result1['@id'] for result1 in results1],
               'file with paired_end number missing "paired with" related_file':
               [result2['@id'] for result2 in results2]}
    results_rev = {item: key for key, val in results.items() for item in val}

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(results_rev, 'FileFastq',
                                                                 'pairedendsconsistent',
                                                                 connection.ff_env)

    if [val for val in results.values() if val]:
        check.status = 'WARN'
        check.summary = 'Inconsistencies found in FileFastq paired end info'
        check.description = ('{} files found with a "paired with" related_file but missing a paired_end number; '
                             '{} files found with a paired_end number but missing related_file info'
                             ''.format(len(results['paired with file missing paired_end number']),
                                       len(results['file with paired_end number missing "paired with" related_file'])))
    else:
        check.status = 'PASS'
        check.summary = 'No inconsistencies in FileFastq paired end info'
        check.description = 'All paired end fastq files have both paired end number and "paired with" related_file'
    check.full_output = {'New fastq files with inconsistent paired end info': to_add,
                         'Old fastq files with inconsistent paired end info': ok,
                         'Fastq files with paired end info now consistent': to_remove,
                         'Fastq files with paired end badge that needs editing': to_edit}
    check.brief_output = results
    check.action = 'patch_badges_for_paired_end_consistency'
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_paired_end_consistency(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_paired_end_consistency')

    pe_check = init_check_res(connection, 'paired_end_info_consistent')
    pe_check_result = pe_check.get_result_by_uuid(kwargs['called_by'])

    pe_keys = ['New fastq files with inconsistent paired end info',
               'Fastq files with paired end info now consistent',
               'Fastq files with paired end badge that needs editing']

    action.output = patch_badges(pe_check_result['full_output'], 'pairedendsconsistent', pe_keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching successful for paired end fastq files.'
    return action


@check_function()
def page_children_routes(connection, **kwargs):
    check = init_check_res(connection, 'page_children_routes')

    page_search = 'search/?type=Page&format=json&children.name%21=No+value'
    results = ff_utils.search_metadata(page_search, ff_env=connection.ff_env)
    problem_routes = {}
    for result in results:
        bad_children = [child['name'] for child in result['children'] if
                        child['name'] != result['name'] + '/' + child['name'].split('/')[-1]]
        if bad_children:
            problem_routes[result['name']] = bad_children

    if problem_routes:
        check.status = 'WARN'
        check.summary = 'Pages with bad routes found'
        check.description = ('{} child pages whose route is not a direct sub-route of parent'
                             ''.format(sum([len(val) for val in problem_routes.values()])))
    else:
        check.status = 'PASS'
        check.summary = 'No pages with bad routes'
        check.description = 'All routes of child pages are a direct sub-route of parent page'
    check.full_output = problem_routes
    return check


@check_function()
def check_help_page_urls(connection, **kwargs):
    check = init_check_res(connection, 'check_help_page_urls')

    server = ff_utils.get_authentication_with_server(ff_env=connection.ff_env)['server']
    results = ff_utils.search_metadata('search/?type=StaticSection&q=help&status!=draft&field=body',
                                       ff_env=connection.ff_env)
    sections_w_broken_links = {}
    for result in results:
        broken_links = []
        urls = re.findall('[\(|\[|=]["]*(http[^\s\)\]]+|/[^\s\)\]]+)[\)|\]|"]', result.get('body', ''))
        for url in urls:
            if url.startswith('/'):
                # fill in appropriate url
                url = server + url
            request = requests.get(url)
            if request.status_code not in [200, 412]:
                broken_links.append((url, request.status_code))
        if broken_links:
            sections_w_broken_links[result['@id']] = broken_links
    if sections_w_broken_links:
        check.status = 'WARN'
        check.summary = 'Broken links found'
        check.description = ('{} static sections currently have broken links.'
                             ''.format(len(sections_w_broken_links.keys())))
    else:
        check.status = 'PASS'
        check.summary = 'No broken links found'
        check.description = check.summary
    check.full_output = sections_w_broken_links
    return check


@check_function(id_list=None)
def check_status_mismatch(connection, **kwargs):
    check = init_check_res(connection, 'check_status_mismatch')
    id_list = kwargs['id_list']
    ffkey = ff_utils.get_authentication_with_server(ff_env=connection.ff_env)
    if not ffkey:
        check.status = 'FAIL'
        check.description = "not able to get data from fourfront"
        return check

    MIN_CHUNK_SIZE = 200

    # embedded sub items should have an equal or greater level
    # than that of the item in which they are embedded
    STATUS_LEVEL = {
        'released': 3,
        'archived': 3,
        'current': 3,
        'revoked': 3,
        'released to project': 3,
        'pre-release': 3,
        'planned': 2,
        'archived to project': 2,
        'in review by lab': 1,
        'submission in progress': 1,
        'to be uploaded by workflow': 1,
        'uploading': 1,
        'uploaded': 1,
        'upload failed': 1,
        'draft': 1,
        'deleted': 0,
        'replaced': 0,
        'obsolete': 0,
    }

    id2links = {}
    id2status = {}
    id2item = {}
    stati2search = ['released', 'released_to_project', 'pre-release']
    items2search = ['ExperimentSet']
    item_search = 'search/?frame=object'
    for item in items2search:
        item_search += '&type={}'.format(item)
    for status in stati2search:
        item_search += '&status={}'.format(status)

    if id_list:
        itemids = re.split(',|\s+', id_list)
        itemids = [id for id in itemids if id]
    else:
        itemres = ff_utils.search_metadata(item_search, key=ffkey, page_limit=500)
        itemids = [item.get('uuid') for item in itemres]
    es_items = andys_get_es_metadata(itemids, key=ffkey, chunk_size=200, is_generator=True)
    # oes = open('/Users/andrew/Desktop/es_item.out', 'w')
    for es_item in es_items:
        # oes.write(json.dumps(es_item, indent=4))
        import pdb; pdb.set_trace()
        label = es_item.get('embedded').get('display_title')
        status = es_item.get('properties').get('status', 'in review by lab')
        opfs = _get_all_other_processed_files(es_item)

        id2links[es_item.get('uuid')] = es_item.get('linked_uuids')
        id2status[es_item.get('uuid')] = STATUS_LEVEL.get(status)
        id2item[es_item.get('uuid')] = {'label': label, 'status': status, 'to_ignore': list(set(opfs))}

    mismatches = {}
    linked2get = {}
    for i, iid in enumerate(itemids):
        linkedids = id2links.get(iid)
        if not linkedids:  # item with no link
            continue
        istatus = id2status.get(iid)
        for lid in linkedids:
            lstatus = id2status.get(lid)
            if not lstatus:  # add to list to get
                linked2get.setdefault(lid, []).append(iid)
            elif lstatus < istatus:  # status mismatch for an item we've seen before
                mismatches.setdefault(iid, []).append(lid)

        if len(linked2get) > MIN_CHUNK_SIZE or i + 1 == len(itemids):  # only query es when we have more than a set number of ids (500)
            linked2chk = andys_get_es_metadata(list(linked2get.keys()), key=ffkey, chunk_size=200, is_generator=True)
            # if linked2chk:
            for litem in linked2chk:
                luuid = litem.get('uuid')
                listatus = litem.get('properties').get('status', 'in review by lab')
                llabel = litem.get('item_type')
                lstatus = STATUS_LEVEL.get(listatus)

                # add info to tracking dict
                id2status[luuid] = lstatus
                id2item[luuid] = {'label': llabel, 'status': listatus}
                for lfid in set(linked2get[luuid]):
                    # check to see if the linked item is something to ignore for that item
                    ignore = id2item[lfid].get('to_ignore')
                    if ignore is not None and luuid in ignore:
                        continue
                    elif lstatus < id2status[lfid]:  # status mismatch so add to report
                        mismatches.setdefault(lfid, []).append(luuid)
            linked2get = {}  # reset the linked id dict
    if mismatches:
        brief_output = {}
        full_output = {}
        for eid, mids in mismatches.items():
            eset = id2item.get(eid)
            key = '{}    {}    {}'.format(eid, eset.get('label'), eset.get('status'))
            brief_output[key] = len(mids)
            for mid in mids:
                mitem = id2item.get(mid)
                val = '{}    {}    {}'.format(mid, mitem.get('label'), mitem.get('status'))
                full_output.setdefault(key, []).append(val)
        check.status = 'WARN'
        check.summary = "MISMATCHED STATUSES FOUND"
        check.description = 'Released or pre-release items have linked items with unreleased status'
        check.brief_output = brief_output
        check.full_output = full_output
    else:
        check.status = 'PASS'
        check.summary = "NO MISMATCHES FOUND"
        check.description = 'all statuses present and correct'
    return check


def _get_all_other_processed_files(item):
    toignore = []
    # get directly linked other processed files
    for pfinfo in item.get('properties').get('other_processed_files'):
        toignore.extend([pf for pf in pfinfo.get('files', []) if pf is not None])
    # experiment sets can also have linked opfs from experiment
    expts = item.get('embedded').get('experiments_in_set')
    if expts is not None:
        for exp in expts:
            opfs = exp.get('other_processed_files')
            if opfs is not None:
                for pfinfo in opfs:
                    toignore.extend([pf.get('uuid') for pf in pfinfo.get('files', []) if pf is not None])
    return toignore


def _get_es_metadata(uuids, es_client=None, filters={}, chunk_size=200,
                     key=None, ff_env=None, is_generator=False):
    """
    Given a list of string item uuids, will return a
    dictionary response of the full ES record for those items (or an empty
    dictionary if the items don't exist/ are not indexed)
    You can pass in an Elasticsearch client (initialized by create_es_client)
    through the es_client param to save init time.
    Advanced users can optionally pass a dict of filters that will be added
    to the Elasticsearch query.
        For example: filters={'status': 'released'}
        You can also specify NOT fields:
            example: filters={'status': '!released'}
        You can also specifiy lists of values for fields:
            example: filters={'status': ['released', archived']}
    NOTES:
        - different filter field are combined using AND queries (must all match)
            example: filters={'status': ['released'], 'public_release': ['2018-01-01']}
        - values for the same field and combined with OR (such as multiple statuses)
    Integer chunk_size may be used to control the number of uuids that are
    passed to Elasticsearch in each query; setting this too high may cause
    ES reads to timeout.
    Same auth mechanism as the other metadata functions
    """
    if es_client is None:
        es_url = ff_utils.get_health_page(key, ff_env)['elasticsearch']
        es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    # match all given uuids to _id fields
    # sending in too many uuids in the terms query can crash es; break them up
    # into groups of max size 100
    es_res = []
    for i in range(0, len(uuids), chunk_size):
        query_uuids = uuids[i:i + chunk_size]
        es_query = {
            'query': {
                'bool': {
                    'must': [
                        {'terms': {'_id': query_uuids}}
                    ],
                    'must_not': []
                }
            },
            'sort': [{'_uid': {'order': 'desc'}}]
        }
        if filters:
            if not isinstance(filters, dict):
                print('Invalid filter for get_es_metadata: %s' % filters)
            else:
                for k, v in filters.items():
                    key_terms = []
                    key_not_terms = []
                    iter_terms = [v] if not isinstance(v, list) else v
                    for val in iter_terms:
                        if val.startswith('!'):
                            key_not_terms.append(val[1:])
                        else:
                            key_terms.append(val)
                    if key_terms:
                        es_query['query']['bool']['must'].append(
                            {'terms': {'embedded.' + k + '.raw': key_terms}}
                        )
                    if key_not_terms:
                        es_query['query']['bool']['must_not'].append(
                            {'terms': {'embedded.' + k + '.raw': key_not_terms}}
                        )
        # use chunk_limit as page size for performance reasons
        for es_page in ff_utils.get_es_search_generator(es_client, '_all', es_query,
                                                        page_size=chunk_size):
            for hit in es_page:
                yield hit['_source']


def andys_get_es_metadata(uuids, es_client=None, filters={}, chunk_size=200,
                          key=None, ff_env=None, is_generator=False):
    meta = _get_es_metadata(uuids, es_client, filters, chunk_size, key, ff_env)
    if is_generator:
        return meta
    return list(meta)
