import re
import requests
import datetime
import json
from dcicutils import ff_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


REV = ['in review by lab', 'submission in progress']
REV_KEY = 'In review by lab/Submission in progress'
RELEASED_KEY = 'Released/Released to project/Pre-release/Archived'


def stringify(item):
    if isinstance(item, str):
        return item
    elif isinstance(item, list):
        return ', '.join(sorted([stringify(i) for i in item]))
    elif isinstance(item, float) and abs(item - int(item)) == 0:
        return str(int(item))
    return str(item)


def compare_badges(obj_ids, item_type, badge, ff_keys):
    '''
    Compares items that should have a given badge to items that do have the given badge.
    Used for badges that utilize a single message choice.
    Input (first argument) should be a list of item @ids.
    '''
    search_url = 'search/?type={}&badges.badge.@id=/badges/{}/'.format(item_type, badge)
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', key=ff_keys)
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


def replace_messages_content(messages, ff_keys):
    """ replace any occurrence of @id with its display_title """

    # an approximate way to check for @id: start and end with "/", and with one more "/" in between
    at_id_pattern = r"(/[^/]+/[^/]+/)"

    def _get_db_item(matchobj):
        at_id = matchobj.group(0)
        if at_id is not None:
            item = ff_utils.get_metadata(at_id, key=ff_keys, add_on='frame=object')
            return item.get('display_title', item['@id'])
        else:
            return matchobj

    new_messages = []
    for message in messages:
        mes_key, mes_val = message.split(": ", 1)
        mes_val_new = re.sub(at_id_pattern, _get_db_item, mes_val)
        new_messages.append(mes_key + ": " + mes_val_new)
    return new_messages


def compare_badges_and_messages(obj_id_dict, item_type, badge, ff_keys,
                                ignore_details=False, replace_messages=False):
    """
    Compares items that should have a given badge to items that do have the given badge.
    Also compares badge messages to see if the message is the right one or needs to be updated.
    Input (first argument) should be a dictionary of item's @id and the badge message it should have.

    ignore_details argument can be used to check only the first part of each message.
    replace_messages replaces an @id with the item's display_title.
    """
    search_url = f'search/?type={item_type}&badges.badge.@id=/badges/{badge}/'
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', key=ff_keys)
    needs_badge = {}
    badge_edit = {}
    badge_ok = []
    remove_badge = {}
    for item in has_badge:
        if item['@id'] in obj_id_dict.keys():
            # handle differences in badge messages
            for a_badge in item['badges']:
                if a_badge['badge'].endswith(badge + '/'):
                    messages_for_comparison = obj_id_dict[item['@id']]
                    if ignore_details:
                        a_badge['messages'] = [a_message.split(":")[0] for a_message in a_badge.get('messages', []) if a_message]
                        messages_for_comparison = [a_message.split(":")[0] for a_message in obj_id_dict[item['@id']] if a_message]

                    if a_badge.get('messages') == messages_for_comparison:
                        badge_ok.append(item['@id'])
                    else:  # new message is different
                        if not ignore_details and replace_messages:  # try replacing @id in messages and check again
                            messages_for_comparison = replace_messages_content(messages_for_comparison, ff_keys)
                            if a_badge.get('messages') == messages_for_comparison:
                                badge_ok.append(item['@id'])
                                break
                        if a_badge.get('message'):
                            del a_badge['message']
                        a_badge['messages'] = obj_id_dict[item['@id']] if not replace_messages else\
                            replace_messages_content(obj_id_dict[item['@id']], ff_keys)
                        badge_edit[item['@id']] = item['badges']
                    break
        else:
            this_badge = [a_badge for a_badge in item['badges'] if badge in a_badge['badge']][0]
            item['badges'].remove(this_badge)
            remove_badge[item['@id']] = item['badges']
    for key, val in obj_id_dict.items():
        if key not in badge_ok + list(badge_edit.keys()):
            needs_badge[key] = val if not replace_messages else replace_messages_content(val, ff_keys)
    return needs_badge, remove_badge, badge_edit, badge_ok


def patch_badges(full_output, badge_name, ff_keys, single_message=''):
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
    output_keys = ['Add badge', 'Remove badge']
    if isinstance(full_output[output_keys[0]], list):
        add_list = full_output[output_keys[0]]
    elif isinstance(full_output[output_keys[0]], dict):
        patches['edit_badge_success'] = []
        patches['edit_badge_failure'] = []
        output_keys.append('Keep badge and edit messages')
        add_list = full_output[output_keys[0]].keys()
    for add_key in add_list:
        add_result = ff_utils.get_metadata(add_key + '?frame=object&field=badges', key=ff_keys)
        badges = add_result['badges'] if add_result.get('badges') else []
        badges.append({'badge': badge_id, 'messages': [single_message] if single_message else full_output[output_keys[0]][add_key]})
        if [b['badge'] for b in badges].count(badge_id) > 1:
            # print an error message?
            patches['add_badge_failure'].append('{} already has badge'.format(add_key))
            continue
        try:
            response = ff_utils.patch_metadata({"badges": badges}, add_key[1:], key=ff_keys)
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
                response = ff_utils.patch_metadata({"badges": remove_val}, remove_key, key=ff_keys)
            else:
                response = ff_utils.patch_metadata({}, remove_key + '?delete_fields=badges', key=ff_keys)
            if response['status'] == 'success':
                patches['remove_badge_success'].append(remove_key)
            else:
                patches['remove_badge_failure'].append(remove_key)
        except Exception:
            patches['remove_badge_failure'].append(remove_key)
    if len(output_keys) > 2:
        for edit_key, edit_val in full_output[output_keys[2]].items():
            try:
                response = ff_utils.patch_metadata({"badges": edit_val}, edit_key, key=ff_keys)
                if response['status'] == 'success':
                    patches['edit_badge_success'].append(edit_key)
                else:
                    patches['edit_badge_failure'].append(edit_key)
            except Exception:
                patches['edit_badge_failure'].append(edit_key)
    return patches


@check_function(action="patch_biosample_warning_badges")
def yellow_flag_biosamples(connection, **kwargs):
    '''
    Checks biosamples for required metadata:
    1. Culture harvest date, doubling number, passage number, culture duration
    2. Morphology image
    3. Karyotyping (authentication doc or string field) for any biosample derived
    from pluripotent cell line that has been passaged more than 10 times beyond
    the first thaw of the original vial.
    4. Differentiation authentication for differentiated cells.
    5. HAP-1 biosamples must have ploidy authentication.
    6. For phase 2 samples must include FBS info (post 2022-05-10)
    '''
    check = CheckResult(connection, 'yellow_flag_biosamples')

    results = ff_utils.search_metadata('search/?type=Biosample', key=connection.ff_keys)
    flagged = {}
    check.brief_output = {RELEASED_KEY: {}, REV_KEY: []}

    fbs_chk_date = '2022-05-10'
    for result in results:
        messages = []
        bs_types = [bs.get('biosource_type') for bs in result.get('biosource', [])]
        karyotype = False
        diff_auth = False
        ploidy = False
        bccs = result.get('cell_culture_details', [])
        if not bccs:
            if len([t for t in bs_types if t in ['primary cell', 'tissue', 'multicellular organism']]) != len(bs_types):
                messages.append('Biosample missing Cell Culture Details')
        else:
            tier = re.search(r'\(Tier (1|2)\)', result.get('biosource_summary'))
            for bcc in bccs:
                for item in [
                    'culture_harvest_date', 'doubling_number', 'passage_number', 'culture_duration', 'morphology_image'
                ]:
                    if not bcc.get(item):
                        messages.append('Biosample missing {}'.format(item))
                if bcc.get('karyotype'):
                    karyotype = True
                for protocol in bcc.get('authentication_protocols', []):
                    protocol_item = ff_utils.get_metadata(protocol['@id'], key=connection.ff_keys)
                    auth_type = protocol_item.get('protocol_classification')
                    if not karyotype and auth_type == 'Karyotype Authentication':
                        karyotype = True
                    elif auth_type == 'Differentiation Authentication':
                        diff_auth = True
                    elif auth_type == 'Ploidy Authentication':
                        ploidy = True
                passages = bcc.get('passage_number', 0)
                if 'tem cell' in ''.join(bs_types) and not karyotype:
                    if passages > 10:
                        messages.append('Biosample is a stem cell line over 10 passages but missing karyotype')
                    elif not passages:
                        messages.append('Biosample is a stem cell line with unknown passage number missing karyotype')
                if tier and bcc.get('culture_start_date', '2000-01-01') > fbs_chk_date:
                    valid_fbs = ["VWR 97068-091 Lot 035B15 (phase 1)", "Peak Serum PS-FBS2 Lot 21E1202 (phase 2)", "VWR 89510-184 lot 310B19 (phase 2)"]
                    fbs_info = bcc.get('fbs_vendor_lot', '').strip()
                    if fbs_info not in valid_fbs:
                        messages.append('Tiered cell line cultured after {} missing 4DN specified FBS vendor and lot info'.format(fbs_chk_date))
        if result.get('biosample_type') == 'In vitro differentiated cells' and not diff_auth:
            messages.append('Differentiated biosample missing differentiation authentication')
        if 'HAP-1' in result.get('biosource_summary') and not ploidy:
            messages.append('HAP-1 biosample missing ploidy authentication')
        if messages:
            messages = [messages[i] for i in range(len(messages)) if messages[i] not in messages[:i]]
            if result.get('status') in REV:
                check.brief_output[REV_KEY].append('{} missing {}'.format(
                    result['@id'], ', '.join(list(set([item[item.index('missing') + 8:] for item in messages])))
                ))
            else:
                flagged[result['@id']] = messages

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(
        flagged, 'Biosample', 'biosample-metadata-incomplete', connection.ff_keys
    )
    check.action = 'patch_biosample_warning_badges'
    if to_add or to_remove or to_edit:
        check.status = 'WARN'
        check.summary = 'Yellow flag biosample badges need patching'
        check.description = '{} biosamples need warning badges patched'.format(
            len(to_add.values()) + len(to_remove.values()) + len(to_edit.values())
        )
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'Yellow flag biosample badges up-to-date'
        check.description = 'No yellow flag biosample badges need patching'
    check.full_output = {'Add badge': to_add,
                         'Remove badge': to_remove,
                         'Keep badge and edit messages': to_edit,
                         'Keep badge (no change)': ok}
    check.brief_output[RELEASED_KEY] = {
        'Add badge': ['{} missing {}'.format(
            k, ', '.join([item[item.index('missing') + 8:] for item in flagged[k]])
        ) for k in to_add.keys()],
        'Remove badge': list(to_remove.keys()),
        'Keep badge and edit messages': ['{} missing {}'.format(
            k, ', '.join([item[item.index('missing') + 8:] for item in flagged[k]])
        ) for k in to_edit.keys()]
    }
    return check


@action_function()
def patch_biosample_warning_badges(connection, **kwargs):
    action = ActionResult(connection, 'patch_biosample_warning_badges')
    bs_check_result = action.get_associated_check_result(kwargs)

    action.output = patch_badges(
        bs_check_result['full_output'], 'biosample-metadata-incomplete', connection.ff_keys
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for yellow flag biosamples.'
    return action


@check_function(action="patch_gold_biosample_badges")
def gold_biosamples(connection, **kwargs):
    '''
    Gold level commendation criteria:
    1. Tier 1 or Tier 2 Cells obtained from the approved 4DN source and grown
    precisely according to the approved SOP including any additional
    authentication (eg. HAP-1 haploid line requires ploidy authentication).
    2. All required metadata present (does not have a biosample warning badge).
    '''
    check = CheckResult(connection, 'gold_biosamples')

    search_url = ('search/?biosource.cell_line_tier=Tier+1&biosource.cell_line_tier=Tier+2'
                  '&type=Biosample&badges.badge.warning=No+value')
    results = ff_utils.search_metadata(search_url, key=connection.ff_keys)
    gold = []
    for result in results:
        # follows SOP w/ no deviations
        sop = True if all([bcc.get('follows_sop', '') == 'Yes' for bcc in result.get('cell_culture_details', [])]) else False
        if sop and result.get('status') not in REV:
            gold.append(result['@id'])
    to_add, to_remove, ok = compare_badges(gold, 'Biosample', 'gold-biosample', connection.ff_keys)
    check.action = 'patch_gold_biosample_badges'
    if to_add or to_remove:
        check.status = 'WARN'
        check.summary = 'Gold biosample badges need patching'
        check.description = '{} biosamples need gold badges patched. '.format(len(to_add) + len(to_remove.keys()))
        check.description += 'Yellow_flag_biosamples check must pass before patching.'
        yellow_check = CheckResult(connection, 'yellow_flag_biosamples')
        latest_yellow = yellow_check.get_latest_result()
        if latest_yellow['status'] == 'PASS':
            check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'Gold biosample badges up-to-date'
        check.description = 'No gold biosample badges need patching'
    check.full_output = {'Add badge': to_add,
                         'Remove badge': to_remove,
                         'Keep badge (no change)': ok}
    return check


@action_function()
def patch_gold_biosample_badges(connection, **kwargs):
    action = ActionResult(connection, 'patch_gold_biosample_badges')
    gold_check_result = action.get_associated_check_result(kwargs)

    action.output = patch_badges(
        gold_check_result['full_output'], 'gold-biosample', connection.ff_keys,
        single_message=('Biosample receives gold status for being a 4DN Tier 1 or Tier 2'
                        ' cell line that follows the approved SOP and contains all of the '
                        'pertinent metadata information as required by the 4DN Samples working group.')
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for yellow flag biosamples.'
    return action


@check_function(action="patch_badges_for_replicate_numbers")
def repsets_have_bio_reps(connection, **kwargs):
    '''
    Check for replicate experiment sets that have one of the following issues:
    1) Only a single biological replicate (includes sets with single experiment)
    2) Biological replicate numbers that are not in sequence
    3) Technical replicate numbers that are not in sequence

    Action patches badges with a message detailing which of the above issues is relevant.
    '''
    check = CheckResult(connection, 'repsets_have_bio_reps')

    results = ff_utils.search_metadata('search/?type=ExperimentSetReplicate&frame=object',
                                       key=connection.ff_keys, page_limit=50)

    audits = {
        REV_KEY: {'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []},
        RELEASED_KEY: {'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []}
    }
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
            if result.get('status') in REV:
                audit_key = REV_KEY
            else:
                audit_key = RELEASED_KEY

            # check if single biological replicate
            if len(rep_dict.keys()) == 1:
                # this tag labels an ExpSet with many replicates, but only one present in the database (typically imaging datasets)
                if 'many_replicates' in result.get('tags', []):  # skip false positive
                    continue
                audits[audit_key]['single_biorep'].append(result['@id'])
                exp_audits.append('Replicate set contains only a single biological replicate')
            # check if bio rep numbers not in sequence
            if sorted(list(rep_dict.keys())) != list(range(min(rep_dict.keys()), max(rep_dict.keys()) + 1)):
                audits[audit_key]['biorep_nums'].append('{} - bio rep #s:'
                                             ' {}'.format(result['@id'], str(sorted(list(rep_dict.keys())))))
                exp_audits.append('Biological replicate numbers are not in sequence')
        # check if tech rep numbers not in sequence
            for key, val in rep_dict.items():
                if sorted(val) != list(range(min(val), max(val) + 1)):
                    audits[audit_key]['techrep_nums'].append('{} - tech rep #s of biorep {}:'
                                                  ' {}'.format(result['@id'], key, str(sorted(val))))
                    exp_audits.append('Technical replicate numbers of biological replicate {}'
                                      ' are not in sequence'.format(key))
        if exp_audits and result.get('status') not in REV:
            by_exp[result['@id']] = sorted(exp_audits)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(by_exp, 'ExperimentSetReplicate',
                                                                 'replicate-numbers', connection.ff_keys)
    check.action = 'patch_badges_for_replicate_numbers'
    if to_add or to_remove or to_edit:
        check.status = 'WARN'
        check.summary = 'Replicate number badges need patching'
        check.description = '{} replicate experiment sets need replicate badges patched'.format(
            len(to_add.values()) + len(to_remove.values()) + len(to_edit.values())
        )
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'Replicate number badges up-to-date'
        check.description = 'No replicate number badges need patching'
    check.full_output = {'Add badge': to_add,
                         'Remove badge': to_remove,
                         'Keep badge and edit messages': to_edit,
                         'Keep badge (no change)': len(ok)}
    check.brief_output = {REV_KEY: audits[REV_KEY]}
    check.brief_output[RELEASED_KEY] = {
        k: {'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []} for k in check.full_output.keys()
    }
    for k, v in audits[RELEASED_KEY].items():
        nochg_cnt = 0
        for item in v:
            name = item.split(' ')[0]
            for key in ["Add badge", 'Remove badge', 'Keep badge and edit messages']:
                if name in check.full_output[key].keys():
                    check.brief_output[RELEASED_KEY][key][k].append(item)
            if name in ok:
                nochg_cnt += 1
        check.brief_output[RELEASED_KEY]['Keep badge (no change)'][k] = nochg_cnt
    return check


@action_function()
def patch_badges_for_replicate_numbers(connection, **kwargs):
    action = ActionResult(connection, 'patch_badges_for_replicate_numbers')
    rep_check_result = action.get_associated_check_result(kwargs)

    action.output = patch_badges(rep_check_result['full_output'], 'replicate-numbers', connection.ff_keys)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for replicate numbers'
    return action


@check_function(action="patch_badges_for_raw_files")
def exp_has_raw_files(connection, **kwargs):
    '''
    Check for sequencing experiments that don't have raw files
    Action patches badges
    '''
    check = CheckResult(connection, 'exp_has_raw_files')
    # search all experiments except microscopy experiments for missing files field
    no_files = ff_utils.search_metadata('search/?type=Experiment&%40type%21=ExperimentMic&files.uuid=No+value',
                                        key=connection.ff_keys)
    # also check sequencing experiments whose files items are all uploading/archived/deleted
    bad_status = ff_utils.search_metadata('search/?status=uploading&status=archived&status=deleted&status=upload+failed'
                                          '&type=FileFastq&experiments.uuid%21=No+value',
                                          key=connection.ff_keys)
    bad_status_ids = {item['@id']: item['status'] for item in bad_status}
    exps = list(set([exp['@id'] for fastq in bad_status for exp in
                     fastq.get('experiments') if fastq.get('experiments')]))
    missing_files_released = [e['@id'] for e in no_files if e.get('status') not in REV]
    missing_files_in_rev = [e['@id'] for e in no_files if e.get('status') in REV]
    for expt in exps:
        result = ff_utils.get_metadata(expt, key=connection.ff_keys)
        raw_files = False
        if result.get('files'):
            for fastq in result.get('files'):
                if fastq['@id'] not in bad_status_ids or result['status'] == bad_status_ids[fastq['@id']]:
                    raw_files = True
                    break
        if not raw_files:
            if result.get('status') in REV:
                missing_files_in_rev.append(expt)
            else:
                missing_files_released.append(expt)

    to_add, to_remove, ok = compare_badges(missing_files_released, 'Experiment', 'no-raw-files', connection.ff_keys)

    if to_add or to_remove:
        check.status = 'WARN'
        check.summary = 'Raw Files badges need patching'
        check.description = '{} sequencing experiments need raw files badges patched'.format(
            len(to_add) + len(to_remove)
        )
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'Raw Files badges up-to-date'
        check.description = 'No sequencing experiments need raw files badges patched'
    check.action = 'patch_badges_for_raw_files'
    check.full_output = {'Add badge': to_add,
                         'Remove badge': to_remove,
                         'Keep badge': ok}
    check.brief_output = {REV_KEY: missing_files_in_rev,
                          RELEASED_KEY: {'Add badge': to_add, 'Remove badge': to_remove}}
    return check


@action_function()
def patch_badges_for_raw_files(connection, **kwargs):
    action = ActionResult(connection, 'patch_badges_for_raw_files')
    raw_check_result = action.get_associated_check_result(kwargs)

    action.output = patch_badges(
        raw_check_result['full_output'], 'no-raw-files', connection.ff_keys, single_message='Raw files missing'
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for experiments missing raw files.'
    return action


@check_function(ignore_details=False, action="patch_badges_for_inconsistent_replicate_info")
def consistent_replicate_info(connection, **kwargs):
    """
    Check for replicate experiment sets that have discrepancies in metadata between
    replicate experiments.

    Action patches badges with a message detailing which fields have the inconsistencies
    and what the inconsistent values are.

    ignore_details argument (default: False) can be used for faster check runs, comparing
    only part of the message (field name, but not values). This will reduce the number of
    get_metadata requests. This argument is set False right now (Nov 2022), as the check
    runs fine, but it could be changed in the future if a lighter check is needed.
    """
    check = CheckResult(connection, 'consistent_replicate_info')

    repset_url = 'search/?type=ExperimentSetReplicate&field=experiments_in_set.%40id' + ''.join(
        ['&field=' + field for field in ['uuid', 'status', 'lab.display_title']])
    exps_url = 'search/?type=Experiment&frame=object'
    exps_bio_url = 'search/?type=Experiment&field=biosample'
    exps_path_url = 'search/?type=ExperimentMic&field=imaging_paths.channel' + ''.join(
        ['&field=imaging_paths.path.' + field for field in ['display_title', 'imaging_rounds']])
    repsets = [item for item in ff_utils.search_metadata(repset_url, key=connection.ff_keys) if item.get('experiments_in_set')]
    exps = ff_utils.search_metadata(exps_url, key=connection.ff_keys)
    exps_w_biosamples = ff_utils.search_metadata(exps_bio_url, key=connection.ff_keys)
    exps_w_paths = ff_utils.search_metadata(exps_path_url, key=connection.ff_keys)
    exp_keys = {exp['@id']: exp for exp in exps}
    bio_keys = {exp['@id']: exp['biosample'] for exp in exps_w_biosamples}
    pth_keys = {exp['@id']: exp['imaging_paths'] for exp in exps_w_paths}
    fields2check = [
        'lab',
        'award',
        'experiment_type',
        'crosslinking_method',
        'crosslinking_time',
        'crosslinking_temperature',
        'digestion_enzyme',
        'enzyme_lot_number',
        'digestion_time',
        'digestion_temperature',
        'tagging_method',
        'tagging_rounds',
        'ligation_time',
        'ligation_temperature',
        'ligation_volume',
        'biotin_removed',
        'protocol',
        'protocol_variation',
        'follows_sop',
        'average_fragment_size',
        'fragment_size_range',
        'fragmentation_method',
        'fragment_size_selection_method',
        'rna_tag',
        'targeted_regions',
        'targeted_factor',
        'dna_label',
        'labeling_time',
        'antibody',
        'antibody_lot_id',
        'microscopy_technique',
        'microscope_configuration_master'
    ]
    check.brief_output = {REV_KEY: {}, RELEASED_KEY: {
        'Add badge': {}, 'Remove badge': {}, 'Keep badge and edit messages': {}
    }}
    compare = {}
    results = {}

    def _get_unique_values(input_list):
        """Given a list of any type of items (including non-hashable ones),
        return a list of unique items"""
        values_list = []
        for value in input_list:
            if value not in values_list:
                values_list.append(value)
        return values_list

    for repset in repsets:
        info_dict = {}
        exp_list = [item['@id'] for item in repset['experiments_in_set']]

        # check Experiment fields
        for field in fields2check:
            vals = _get_unique_values([exp_keys[exp_id].get(field) for exp_id in exp_list])
            # allow small deviations in average fragment size
            if field == 'average_fragment_size' and None not in vals:
                int_vals = [int(val) for val in vals]
                if (max(int_vals) - min(int_vals))/(sum(int_vals)/len(int_vals)) < 0.25:
                    continue
            # all replicates should have the same value, otherwise this is an inconsistency
            if len(vals) > 1:
                info_dict[field] = vals

        # check some Biosample fields
        for bfield in ['treatments_summary', 'modifications_summary']:
            bvals = list(set([bio_keys[exp_id].get(bfield) for exp_id in exp_list]))
            if len(bvals) > 1:
                info_dict[bfield] = bvals

        # check imaging paths (if an experiment has any)
        if 'imaging_paths' in exp_keys[exp_list[0]]:
            # NOTE: this compares path display_title and not path @id
            img_path_configurations = _get_unique_values([pth_keys[exp_id] for exp_id in exp_list])
            if len(img_path_configurations) > 1:
                length_vals = list(set([len(conf) for conf in img_path_configurations]))
                if len(length_vals) > 1:
                    info_dict['imaging_paths'] = 'different number of imaging paths'
                else:
                    # same length, compare fields 'channel', 'path.display_title', 'path.imaging_rounds'
                    for i in range(length_vals[0]):
                        # compare all paths in the same position i
                        paths_i = [conf[i] for conf in img_path_configurations]
                        channel_vals = list(set([p['channel'] for p in paths_i]))
                        if len(channel_vals) > 1:
                            info_dict[f'imaging_paths {i} channel'] = channel_vals
                        title_vals = list(set([p['path']['display_title'] for p in paths_i]))
                        if len(title_vals) > 1:
                            info_dict[f'imaging_paths {i} path'] = title_vals
                        round_vals = list(set([p['path']['imaging_rounds'] for p in paths_i]))
                        if len(round_vals) > 1:
                            info_dict[f'imaging_paths {i} imaging_rounds'] = round_vals

        # check some biosource fields
        biosource_vals = _get_unique_values(
            [[biosource['@id'] for biosource in bio_keys[exp_id]['biosource']] for exp_id in exp_list])
        if len(biosource_vals) > 1:
            info_dict['biosource'] = biosource_vals

        # check cell_culture_details
        all_cc_details = [bio_keys[exp_id].get('cell_culture_details') for exp_id in exp_list]
        if all(all_cc_details):
            for ccfield in ['synchronization_stage', 'differentiation_state', 'follows_sop']:
                ccvals = [[bcc.get(ccfield) for bcc in cc_details] for cc_details in all_cc_details]
                ccvals = _get_unique_values(ccvals)
                if len(ccvals) > 1:
                    info_dict[ccfield] = ccvals
        elif any(all_cc_details):
            info_dict['cell_culture_details'] = 'some are missing'

        # check biosample_protocols
        all_bs_prot = [bio_keys[exp_id].get('biosample_protocols') for exp_id in exp_list]
        if all(all_bs_prot):
            bp_vals = _get_unique_values([[protocol['@id'] for protocol in bs_prot] for bs_prot in all_bs_prot])
            if len(bp_vals) > 1:
                info_dict['biosample_protocols'] = bp_vals
        elif any(all_bs_prot):
            info_dict['biosample_protocols'] = 'some are missing'

        # now generate a message from the info_dict
        if info_dict:
            msgs = [f"Inconsistent replicate information in {field}: {stringify(values)}" for field, values in info_dict.items()]
            text = f"{repset['@id'][-13:-1]} - inconsistency in {', '.join(list(info_dict.keys()))}"
            lab = repset['lab']['display_title']
            audit_key = REV_KEY if repset['status'] in REV else RELEASED_KEY
            results[repset['@id']] = {'status': audit_key, 'lab': lab, 'info': text}
            if audit_key == REV_KEY:
                check.brief_output[audit_key][lab] = check.brief_output[audit_key].setdefault(lab, []).append(text)
            if repset['status'] not in REV:
                compare[repset['@id']] = msgs

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(
        compare, 'ExperimentSetReplicate', 'inconsistent-replicate-info', connection.ff_keys,
        kwargs['ignore_details'], replace_messages=True
    )

    key_dict = {'Add badge': to_add, 'Remove badge': to_remove, 'Keep badge and edit messages': to_edit}
    for result in results.keys():
        for k, v in key_dict.items():
            if result in v.keys():
                if results[result]['lab'] not in check.brief_output[RELEASED_KEY][k].keys():
                    check.brief_output[RELEASED_KEY][k][results[result]['lab']] = []
                check.brief_output[RELEASED_KEY][k][results[result]['lab']].append(results[result]['info'])
                break
    check.brief_output[RELEASED_KEY]['Remove badge'] = list(to_remove.keys())
    if to_add or to_remove or to_edit:
        check.status = 'WARN'
        check.summary = 'Replicate Info badges need patching'
        check.description = ('{} ExperimentSetReplicates found that need a replicate-info badge patched'
                             ''.format(len(to_add.keys()) + len(to_remove.keys()) + len(to_edit.keys())))
    else:
        check.status = 'PASS'
        check.summary = 'Replicate Info badges are up-to-date'
        check.description = 'No ExperimentSetReplicates found that need a replicate-info badge patched'
    check.full_output = {'Add badge': to_add,
                         'Remove badge': to_remove,
                         'Keep badge and edit messages': to_edit,
                         'Keep badge (no change)': ok}
    check.action = 'patch_badges_for_inconsistent_replicate_info'
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_inconsistent_replicate_info(connection, **kwargs):
    action = ActionResult(connection, 'patch_badges_for_inconsistent_replicate_info')
    rep_info_check_result = action.get_associated_check_result(kwargs)

    action.output = patch_badges(
        rep_info_check_result['full_output'], 'inconsistent-replicate-info', connection.ff_keys
    )
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching successful for inconsistent replicate info badges.'
    return action
