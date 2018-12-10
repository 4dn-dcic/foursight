from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils


class RankedBadge:
    def __init__(self, level, badge_id):
        self.level = level
        self.badge_id = badge_id
        self.to_compare = []


def get_badges(item_type):
    if item_type in ['biosample', 'experiment']:
        gold = RankedBadge('Gold', 'gold-{}'.format(item_type))
        silver = RankedBadge('Silver', 'silver-{}'.format(item_type))
        bronze = RankedBadge('Bronze', 'bronze-{}'.format(item_type))
        return gold, silver, bronze


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


def reformat_and_patch_single(full_output, badge_name, output_keys, ffenv, message=True, single_message=''):
    '''
    Reorganize check output and performs patches for a single badge.
    For badges with single message choice:
    - single_message kwarg should be assigned a string to be used for the badge message;
    - full_output[output_keys[0]] should be a list of item @ids;
    - no badges are edited, they are only added or removed.
    For badges with multiple message options:
    - single_message kwarg should not be used, but left as empty string.
    - full_output[output_keys[0]] should be a list of item @ids and message to patch into badge.
    - badges can also be edited to change the message.

    full_output    : full_output from check - dict
    badge_name     : badgename for patching - str - not full @ids. ex. 'tier1metadatamissing'
    output_keys    : upper-level keys of check output - list of strings
    message        : boolean - whether badge has an accompanying message - default True
    single_message : str - message that goes with badge - if multiple messages,
                     this should be left as empty string (default)
    '''
    to_patch = {'Add': {}}
    already = []
    badge_id = '/badges/' + badge_name + '/'
    if isinstance(full_output[output_keys[0]], list):
        add_list = full_output[output_keys[0]]
    elif isinstance(full_output[output_keys[0]], dict):
        add_list = full_output[output_keys[0]].keys()
    for add_key in add_list:
        add_result = ff_utils.get_metadata(add_key + '?frame=object', ff_env=ffenv)
        badges = add_result['badges'] if add_result.get('badges') else []
        if message:
            badges.append({'badge': badge_id, 'message': single_message if single_message else full_output[output_keys[0]][add_key]})
        else:
            badges.append({'badge': badge_id})
        if [b['badge'] for b in badges].count(badge_id) > 1:
            already.append('{} already has badge'.format(add_key))
            continue
        to_patch['Add'][add_key] = badges
    to_patch['Remove'] = full_output[output_keys[1]]
    if len(output_keys) > 2:
        to_patch['Edit'] = full_output[output_keys[2]]
    return patch_badges(to_patch, already, ffenv)


def reformat_and_patch_multiple(output, output_keys, badges, ffenv):
    '''
    Reorganize check output and performs patches for multiple badges.
    Works to ensure that a 'remove' patch after an 'add' patch doesn't remove
    the newly added badge - instead combines all badges to be added and removed
    into a single patch.

    output      : full_output from check
    output_keys : upper-level keys of check output
                  ex. ['Gold Biosample', 'Silver Biosample', 'Bronze Biosample']
    badges      : badgenames for patching - not full @ids
                  ex. ['gold-biosample', 'silver-biosample', 'bronze-biosample']
    '''
    new_dict = {}
    for key in output_keys:
        for at_id in output[key]['Need badge']:
            if at_id in new_dict.keys() and not new_dict[at_id]['Add']:
                new_dict[at_id]['Add'] = key
            elif at_id not in new_dict.keys():
                new_dict[at_id] = {'Add': key, 'Remove': [], 'Get': True}
        for at_id, val in output[key]['Need badge removed'].items():
            if at_id in new_dict.keys() and val:
                new = [v for v in val if v not in new_dict[at_id]['Remove']]
                new_dict[at_id]['Remove'] += new
            elif at_id not in new_dict.keys():
                new_dict[at_id] = {'Add': '', 'Remove': val}
            new_dict[at_id]['Get'] = False
    print(new_dict)
    patches_dict = {'Add': {}, 'Remove': {}, 'Both': {}}
    has_badge_already = []
    for item in new_dict.keys():
        if new_dict[item]['Add']:
            badge_id = '/badges/' + badges[output_keys.index(new_dict[item]['Add'])] + '/'
            if not new_dict[item]['Get']:
                patches_dict['Both'][item] = new_dict[item]['Remove'] + [{'badge': badge_id}]
            else:
                add_result = ff_utils.get_metadata(item + '?frame=object', ff_env=ffenv)
                badges = add_result['badges'] if add_result.get('badges') else []
                if badge_id in [b['badge'] for b in badges]:
                    has_badge_already.append('{} already has badge'.format(item))
                else:
                    badges.append({'badge': badge_id})
                    patches_dict['Add'][item] = badges
        else:
            patches_dict['Remove'][item] = new_dict[item]['Remove']
    return patch_badges(patches_dict, has_badge_already, ffenv)


def patch_badges(patch_dict, already_list, ffenv):
    '''
    Function for patching badges to items returned in check output.

    patch_dict : dict - keys should be item @ids, values should be list of dicts
                 to patch for badges
    already_list : list of items that check output indicated should have badges
                   added, but already had the badge. Meant to avoid duplicate
                   badges if an action fails halfway through and needs to be rerun.
    '''
    patches = {
        'Add badge success': [],
        'Add badge failure': [],
        'Remove badge success': [],
        'Remove badge failure': []
    }
    if 'Edit' in patch_dict.keys():
        patches['Edit badge success'] = []
        patches['Edit badge failure'] = []
    elif 'Both' in patch_dict.keys():
        patches['Multi-badge patch success'] = []
        patches['Multi-badge patch failure'] = []
    for key in patch_dict:
        if key == 'Both':
            label = 'Multi-badge patch '
        else:
            label = key + ' badge '
        for itemid, val in patch_dict[key].items():
            try:
                if val:
                    response = ff_utils.patch_metadata({"badges": val}, itemid[1:], ff_env=ffenv)
                else:
                    response = ff_utils.patch_metadata({}, itemid[1:] + '?delete_fields=badges', ff_env=ffenv)
                if response['status'] == 'success':
                    patches[label + 'success'].append(itemid)
                else:
                    patches[label + 'failure'].append(itemid)
            except Exception:
                patches[label + 'failure'].append(itemid)
    patches['Add badge failure'] += already_list
    return patches


@check_function()
def good_biosamples(connection, **kwargs):
    check = init_check_res(connection, 'good_biosamples')

    # needs to be tiered cell line and have harvest date to get any ranked badge
    tiered = ('search/?biosource.cell_line_tier=Tier+1&biosource.cell_line_tier=Tier+2'
              '&type=Biosample&cell_culture_details.culture_harvest_date%21=No+value')
    results = ff_utils.search_metadata(tiered, ff_env=connection.ff_env)
    gold, silver, bronze = get_badges('biosample')

    for result in results:
        bcc = result['cell_culture_details']
        # go through results and categorize into gold, silver, bronze
        if (result['biosource'][0].get('cell_line_tier') == 'Tier 1' and bcc.get('culture_duration') and
            bcc.get('morphology_image') and bcc['morphology_image'].get('attachment')):
            if bcc.get('karyotype') and bcc.get('follows_sop') == 'Yes':
                gold.to_compare.append(result['@id'])
            elif bcc.get('follows_sop') == 'Yes' or bcc.get('protocols_additional'):
                silver.to_compare.append(result['@id'])
            else:
                bronze.to_compare.append(result['@id'])
        else:
            bronze.to_compare.append(result['@id'])
    output = {}
    patch = False
    for badge in [gold, silver, bronze]:
        # for each badge type, compare to items that already have badge
        to_add, to_remove, ok = compare_badges(badge.to_compare, 'biosample', badge.badge_id, connection.ff_env)
        if to_add or to_remove:
            patch = True
        output['{} Biosamples'.format(badge.level)] = {
            "Need badge": to_add,
            "Need badge removed": to_remove,
            "Badge OK": ok
        }
    check.full_output = output  # dict of items that either need to be patched or have badge and don't need patch
    check.brief_output = {  # dict containing list of items that qualify for each badge
        "Gold Biosamples": gold.to_compare,
        "Silver Biosamples": silver.to_compare,
        "Bronze Biosamples": bronze.to_compare
    }
    check.action = 'patch_ranked_biosample_badges'
    if not patch:
        check.status = 'PASS'
        check.summary = 'All good biosamples have proper badges'
        check.description = '0 biosamples need badge patching'
    else:
        check.status = 'WARN'
        check.summary = 'Some biosamples need badge patching'
        check.description = '{} biosample need badge patching'.format(
            sum([len(output[key]['Need badge']) + len(output[key]['Need badge removed'].keys()) for key in output.keys()]))
        check.full_output = output
        check.allow_action = True
    return check


@check_function()
def good_experiments(connection, **kwargs):
    check = init_check_res(connection, 'good_experiments')
    # get 4DN certified protocols for checking expts against
    protocol_url = ('search/?protocol_type=Experimental+protocol&tags=4DN+Standard'
                    '&tags=4DN+Joint+Analysis+2018&type=Protocol')
    protocols = [item['@id'] for item in ff_utils.search_metadata(protocol_url, ff_env=connection.ff_env)]
    approved_types = ['in situ Hi-C', 'DNase Hi-C', 'Repli-seq', 'CUT&RUN', 'ChIA-PET']
    tiered = 'search/?type=Experiment&processed_files.uuid%21=No+value'
    results = ff_utils.search_metadata(tiered, ff_env=connection.ff_env)
    gold, silver, bronze = get_badges('experiment')
    for result in results:
        badges = [badge.get('badge') for badge in result.get('badges', [])]
        badges += [badge.get('badge') for badge in result['biosample'].get('badges', [])]
        # Gold:
        # follows_sop=Yes and protocol is 4DN certified
        # biosample badge
        # has processed files and raw files
        # if HiC: check # reads
        if result.get('processed_files') and 'norawfiles' not in ''.join(badges):
            if (result.get('follows_sop') == 'Yes' and result.get('experiment_type') in approved_types
                and 'biosample' in ''.join(badges)):
                if 'Hi-C' in result.get('experiment_type') and 'single cell' not in result.get('experiment_type'):
                    # add up reads
                    file_url = 'search/?type=FileFastq&experiments.@id={}'.format(result['@id'])
                    fastqs = ff_utils.search_metadata(file_url, ff_env=connection.ff_env)
                    reads = sum([fq['quality_metric'].get('Total Sequences', 0) for
                                 fq in fastqs if 'quality_metric' in fq.keys()])
                    if reads >= 500000000:
                        gold.to_compare.append(result['@id'])
                    else:
                        silver.to_compare.append(result['@id'])
                else:
                    gold.to_compare.append(result['@id'])
            elif (result.get('experiment_type') in approved_types and
                  (result.get('protocol_variation') or result.get('follows_sop') == 'Yes')):
                # Silver:
                # 4DN approved protocol or linked protocol deviations
                # has processed files
                silver.to_compare.append(result['@id'])
            elif result.get('protocol'):
                # bronze
                bronze.to_compare.append(result['@id'])
    patch = False
    for badge in [gold, silver, bronze]:
        # for each badge type, compare to items that already have badge
        to_add, to_remove, ok = compare_badges(badge.to_compare, 'experiment', badge.badge_id, connection.ff_env)
        if to_add or to_remove:
            patch = True
        output['{} Experiments'.format(badge.level)] = {
            "Need badge": to_add,
            "Need badge removed": to_remove,
            "Badge OK": ok
        }
    check.full_output = output
    check.brief_output = {  # dict containing list of items that qualify for each badge
        "Gold Biosamples": gold.to_compare,
        "Silver Biosamples": silver.to_compare,
        "Bronze Biosamples": bronze.to_compare
    }
    check.action = 'patch_ranked_experiment_badges'
    if not patch:
        check.status = 'PASS'
        check.summary = 'All qualifying experiments have proper ranked badges'
        check.description = '0 biosamples need ranked badge patching'
    else:
        check.status = 'WARN'
        check.summary = 'Some biosamples need ranked badge patching'
        check.description = '{} biosamples need badge patching'.format(
            sum([len(output[key]['Need badge']) + len(output[key]['Need badge removed'].keys()) for key in output.keys()]))
        check.full_output = output
        check.allow_action = True
    return check


@action_function()
def patch_ranked_biosample_badges(connection, **kwargs):
    action = init_action_res(connection, 'patch_ranked_biosample_badges')

    bs_check = init_check_res(connection, 'good_biosamples')
    bs_check_result = bs_check.get_result_by_uuid(kwargs['called_by'])
    outputkeys = ['Gold Biosamples', 'Silver Biosamples', 'Bronze Biosamples']
    bs_badges = ['gold-biosample', 'silver-biosample', 'bronze-biosample']
    action.output = reformat_and_patch_multiple(
        bs_check_result['full_output'], outputkeys, bs_badges, connection.ff_env
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching ranked badges successful for biosamples'
    return action


@action_function()
def patch_ranked_experiment_badges(connection, **kwargs):
    action = init_action_res(connection, 'patch_ranked_experiment_badges')

    exp_check = init_check_res(connection, 'good_experiments')
    exp_check_result = bs_check.get_result_by_uuid(kwargs['called_by'])
    outputkeys = ['Gold Experiments', 'Silver Experiments', 'Bronze Experiments']
    exp_badges = ['gold-experiment', 'silver-experiment', 'bronze-experiment']
    action.output = reformat_and_patch_multiple(
        exp_check_result['full_output'], outputkeys, exp_badges, connection.ff_env
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching ranked badges successful for experiments'
    return action


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
    check.full_output = {
        'New replicate sets with replicate number issues': to_add,
        'Old replicate sets with replicate number issues': ok,
        'Replicate sets that no longer have replicate number issues': to_remove,
        'Replicate sets with a replicate_numbers badge that needs editing': to_edit
    }
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
    action.output = reformat_and_patch_single(
        rep_check_result['full_output'], 'replicatenumbers', rep_keys, connection.ff_env
    )
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
    msg_dict = {
        'culture_start_date': 'Tier 1 Biosample missing Culture Start Date',
        # item will fail validation if missing a start date - remove this part of check?
        'culture_duration': 'Tier 1 Biosample missing Culture Duration',
        'morphology_image': 'Tier 1 Biosample missing Morphology Image'
    }
    for result in results:
        if len(result.get('biosource')) != 1:
            continue
        elif not result.get('cell_culture_details'):
            missing[result['@id']] = 'Tier 1 Biosample missing Cell Culture Details'
        else:
            messages = [val for key, val in msg_dict.items() if not result['cell_culture_details'].get(key)]
            if messages:
                missing[result['@id']] = '; '.join(messages)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(
        missing, 'Biosample', 'tier1metadatamissing', connection.ff_env
    )
    check.action = 'patch_badges_for_tier1_metadata'
    if missing:
        check.status = 'WARN'
        check.summary = 'Tier 1 biosamples found missing required metadata'
        check.description = '{} tier 1 biosamples found missing required metadata'.format(len(missing.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'All Tier 1 biosamples have required metadata'
        check.description = '0 tier 1 biosamples found missing required metadata'
    check.full_output = {
        'New tier1 biosamples missing required metadata': to_add,
        'Old tier1 biosamples missing required metadata': ok,
        'Tier1 biosamples no longer missing required metadata': to_remove,
        'Biosamples with a tier1_metadata_missing badge that needs editing': to_edit
    }
    check.brief_output = list(missing.keys())
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_tier1_metadata(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_tier1_metadata')

    tier1_check = init_check_res(connection, 'tier1_metadata_present')
    tier1_check_result = tier1_check.get_result_by_uuid(kwargs['called_by'])
    tier1keys = [
        'New tier1 biosamples missing required metadata',
        'Tier1 biosamples no longer missing required metadata',
        'Biosamples with a tier1_metadata_missing badge that needs editing'
    ]
    action.output = reformat_and_patch_single(
        tier1_check_result['full_output'], 'tier1metadatamissing', tier1keys, connection.ff_env
    )
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
    check.full_output = {
        'Experiments newly missing raw files': to_add,
        'Old experiments missing raw files': ok,
        'Experiments no longer missing raw files': to_remove
    }
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
    action.output = reformat_and_patch_single(
        raw_check_result['full_output'], 'norawfiles', raw_keys, connection.ff_env, single_message='Raw files missing'
    )
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

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(
        results_rev, 'FileFastq', 'pairedendsconsistent', connection.ff_env
    )
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
    check.full_output = {
        'New fastq files with inconsistent paired end info': to_add,
        'Old fastq files with inconsistent paired end info': ok,
        'Fastq files with paired end info now consistent': to_remove,
        'Fastq files with paired end badge that needs editing': to_edit
    }
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
    pe_keys = [
        'New fastq files with inconsistent paired end info',
        'Fastq files with paired end info now consistent',
        'Fastq files with paired end badge that needs editing'
    ]
    action.output = reformat_and_patch_single(
        pe_check_result['full_output'], 'pairedendsconsistent', pe_keys, connection.ff_env
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching successful for paired end fastq files.'
    return action
