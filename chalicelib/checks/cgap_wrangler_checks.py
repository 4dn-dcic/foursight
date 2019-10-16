from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    action_function,
)
from ..run_result import CheckResult, ActionResult
from dcicutils import ff_utils
import requests
import json
import datetime
import time
import itertools


@check_function(item_type=None)
def suggestions_for_obsolete_items(connection, **kwargs):
    ''' Gets obsolete items currently Disorder and Phenotype and searches
        for the ontology term id as an alternative_id
        associated with live item(s).

        To restrict the check to one item type include as parameter.
    '''
    id_field_map = {'Disorder': 'disorder_id', 'Phenotype': 'hpo_id'}
    check = CheckResult(connection, 'suggestions_for_obsolete_items')
    check.action = "patch_suggested_replacements"
    check.fulloutput = {}
    chkstatus = None
    chkdesc = None
    brief = None
    obs_search = 'search/?status=obsolete'
    itype_str = '&type=Disorder&type=Phenotype'
    if kwargs.get('item_type'):
        itype_str = '&{}'.format(kwards.get('item_type'))
    obs_query = obs_search + itype_str
    altid_search = 'search/?status!=obsolete&type={}&alternative_ids={}'
    obs_items = ff_utils.search_metadata(obs_query, key=connection.ff_keys, page_limit=200, is_generator=True)
    errors = []
    skipped = {}
    suggestions = {}
    uid2info = {}
    for oi in obs_items:
        ouuid = oi.get('uuid')
        if not ouuid:
            errors.append(oi)
            continue
        at_type = oi.get('@type')[0]
        id_field = id_field_map.get(at_type)
        iid = oi.get(id_field)
        name_field = at_type.lower() + '_name'
        oiname = oi.get(name_field)
        if oiname:
            uid2info[ouuid] = {'name': oiname, 'term_id': iid}
        # check to see if suggested_replacements field exists even if empty
        # empty list means item has been checked before but no replacements found
        # need to keep track to patch to empty list in action
        if 'suggested_replacements' not in oi:
            suggestions.setdefault(ouuid, [])
        replacements = oi.get('suggested_replacements', [])
        repids = [r.get('uuid') for r in replacements]
        if not at_type or not iid:
            skipped[ouuid] = 'missing type or id'
            continue
        altid_query = altid_search.format(at_type, iid)
        rep_items = ff_utils.search_metadata(altid_query, key=connection.ff_keys)
        for ri in rep_items:
            ruid = ri.get('uuid')
            if ruid not in repids:
                suggestions.setdefault(ouuid, []).append(ruid)
                rname = ri.get(name_field)
                rid = ri.get(id_field)
                uid2info[ruid] = {'name': rname, 'term_id': rid}
    if errors:
        chkstatus = 'ERROR'
        chkdesc = 'One or more obsolete items are malformed or irretrivable'
        brief = errors
    elif skipped:
        chkstatus = 'WARN'
        checkdesc = 'One or more obsolete items are missing key info'
        brief = skipped
    if suggestions:
        if not chkstatus:
            chkstatus = 'WARN'
        if not chkdesc:
            checkdesc = 'Obsolete Items with Possible Replacements Found'
        if not brief:
            brief = {}
            for ot, reps in suggestions.items():
                if not reps:
                    val = 'NO SUGGESTED REPLACEMENTS'
                else:
                    val = []
                    for r in reps:
                        rinfo = uid2info[r]
                        val.append('SUGGEST {} - {}'.format(rinfo.get('term_id'), rinfo.get('name')))
                oinfo = uid2info[ot]
                brief['FOR {} - {}'.format(oinfo.get('term_id'), oinfo.get('name'))] = val
        check.full_output = suggestions

    check.status = chkstatus
    check.desc = chkdesc
    check.brief_output = brief
    return check


@action_function()
def patch_suggested_replacements(connection, **kwargs):
    action = ActionResult(connection, 'patch_suggested_replacements')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_failure': [], 'patch_success': []}
    my_key = connection.ff_keys
    items2patch = check_res.get(full_output, {})
    for item, suggested in items2patch.items():
        patch_data = {'suggested_replacements': suggested}
        try:
            ff_utils.patch_metadata(patch_data, obj_id=item, key=my_key)
        except Exception as e:
            action_logs['patch_failure'].append([item, str(e)])
        else:
            action_logs['patch_success'].append(item)
    action.output = action_logs
    action.status = 'DONE'
    if action_logs.get('patch_failure'):
        action.status = 'FAIL'
    return action


@check_function(id_list=None)
def check_status_mismatch_cgap_clinical(connection, **kwargs):
    STATUS_LEVEL = {
        'released': 10,
        'current': 10,
        'in public review': 8,
        'released to project': 6,
        'released to institution': 4,
        'in review': 2,
        'to be uploaded by workflow': 2,
        'uploading': 2,
        'uploaded': 2,
        'upload failed': 2,
        'obsolete': 1,
        'deleted': 0,
        'inactive': 0,
    }
    check = CheckResult(connection, 'check_status_mismatch_cgap')
    id_list = kwargs['id_list']

    MIN_CHUNK_SIZE = 200
    # embedded sub items should have an equal or greater level
    # than that of the item in which they are embedded
    id2links = {}
    id2status = {}
    id2item = {}
    stati2search = [s for s in STATUS_LEVEL.keys() if STATUS_LEVEL.get(s) >= 4]
    items2search = ['Case']
    item_search = 'search/?frame=object'
    for item in items2search:
        item_search += '&type={}'.format(item)
    for status in stati2search:
        item_search += '&status={}'.format(status)

    if id_list:
        itemids = re.split(',|\s+', id_list)
        itemids = [id for id in itemids if id]
    else:
        itemres = ff_utils.search_metadata(item_search, key=connection.ff_keys, page_limit=500)
        itemids = [item.get('uuid') for item in itemres]
    es_items = ff_utils.get_es_metadata(itemids, key=connection.ff_keys, chunk_size=200, is_generator=True)
    for es_item in es_items:
        label = es_item.get('object').get('display_title')
        desc = es_item.get('object').get('description')
        inst = es_item.get('embedded').get('institution').get('display_title')
        status = es_item.get('properties').get('status', 'in review')
        id2links[es_item.get('uuid')] = [li.get('uuid') for li in es_item.get('linked_uuids_embedded')]
        id2status[es_item.get('uuid')] = STATUS_LEVEL.get(status)
        id2item[es_item.get('uuid')] = {'label': label, 'status': status, 'institution': inst,
                                        'description': desc}

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
            linked2chk = ff_utils.get_es_metadata(list(linked2get.keys()), key=connection.ff_keys,
                                                  chunk_size=200, is_generator=True)
            for litem in linked2chk:
                luuid = litem.get('uuid')
                listatus = litem.get('properties').get('status', 'in review')
                llabel = litem.get('item_type')
                lstatus = STATUS_LEVEL.get(listatus)
                # add info to tracking dict
                id2status[luuid] = lstatus
                id2item[luuid] = {'label': llabel, 'status': listatus}
                for lfid in set(linked2get[luuid]):
                    if lstatus < id2status[lfid]:  # status mismatch so add to report
                        mismatches.setdefault(lfid, []).append(luuid)
            linked2get = {}  # reset the linked id dict
    if mismatches:
        brief_output = {}
        full_output = {}
        for eid, mids in mismatches.items():
            eset = id2item.get(eid)
            key = '{} | {} | {} | {}'.format(
                eid, eset.get('label'), eset.get('status'), eset.get('description'))
            brief_output.setdefault(eset.get('institution'), {}).update({key: len(mids)})
            for mid in mids:
                mitem = id2item.get(mid)
                val = '{} | {} | {}'.format(mid, mitem.get('label'), mitem.get('status'))
                full_output.setdefault(eset.get('institution'), {}).setdefault(key, []).append(val)
        check.status = 'WARN'
        check.summary = "MISMATCHED STATUSES FOUND"
        check.description = 'Viewable Items have linked items with unviewable status'
        check.brief_output = brief_output
        check.full_output = full_output
    else:
        check.status = 'PASS'
        check.summary = "NO MISMATCHES FOUND"
        check.description = 'all statuses present and correct'
    return check
