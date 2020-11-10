from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    action_function,
)
from ..run_result import CheckResult, ActionResult
from dcicutils import ff_utils
from dcicutils.env_utils import FF_PROD_BUCKET_ENV
import re
import requests
import json
import datetime
import time
import itertools
import random
from fuzzywuzzy import fuzz
import boto3
from .helpers import wrangler_utils
from collections import Counter

# use a random number to stagger checks
random_wait = 20


@check_function(cmp_to_last=False)
def workflow_run_has_deleted_input_file(connection, **kwargs):
    """Checks all wfrs that are not deleted, and have deleted input files
    There is an option to compare to the last, and only report new cases (cmp_to_last)
    The full output has 2 keys, because we report provenance wfrs but not run action on them
    problematic_provenance: stores uuid of deleted file, and the wfr that is not deleted
    problematic_wfr:        stores deleted file,  wfr to be deleted, and its downstream items (qcs and output files)
    """
    check = CheckResult(connection, 'workflow_run_has_deleted_input_file')
    check.status = "PASS"
    check.action = "patch_workflow_run_to_deleted"
    my_key = connection.ff_keys
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    search_query = 'search/?type=WorkflowRun&status!=deleted&input_files.value.status=deleted&limit=all'
    bad_wfrs = ff_utils.search_metadata(search_query, key=my_key)
    if kwargs.get('cmp_to_last', False):
        # filter out wfr uuids from last run if so desired
        prevchk = check.get_latest_result()
        if prevchk:
            prev_wfrs = prevchk.get('full_output', [])
            filtered = [b.get('uuid') for b in bad_wfrs if b.get('uuid') not in prev_wfrs]
            bad_wfrs = filtered
    if not bad_wfrs:
        check.summmary = check.description = "No live WorkflowRuns linked to deleted input Files"
        return check
    brief = str(len(bad_wfrs)) + " live WorkflowRuns linked to deleted input Files"
    # problematic_provenance stores uuid of deleted file, and the wfr that is not deleted
    # problematic_wfr stores deleted file,  wfr to be deleted, and its downstream items (qcs and output files)
    fulloutput = {'problematic_provenance': [], 'problematic_wfrs': []}
    no_of_items_to_delete = 0

    def fetch_wfr_associated(wfr_info):
        """Given wfr_uuid, find associated output files and qcs"""
        wfr_as_list = []
        wfr_as_list.append(wfr_info['uuid'])
        if wfr_info.get('output_files'):
            for o in wfr_info['output_files']:
                if o.get('value'):
                    wfr_as_list.append(o['value']['uuid'])
                if o.get('value_qc'):
                    wfr_as_list.append(o['value_qc']['uuid'])
        if wfr_info.get('output_quality_metrics'):
            for qc in wfr_info['output_quality_metrics']:
                if qc.get('value'):
                    wfr_as_list.append(qc['value']['uuid'])
        return list(set(wfr_as_list))

    for wfr in bad_wfrs:
        infiles = wfr.get('input_files', [])
        delfile = [f.get('value').get('uuid') for f in infiles if f.get('value').get('status') == 'deleted'][0]
        if wfr['display_title'].startswith('File Provenance Tracking'):
            fulloutput['problematic_provenance'].append([delfile, wfr['uuid']])
        else:
            del_list = fetch_wfr_associated(wfr)
            fulloutput['problematic_wfrs'].append([delfile, wfr['uuid'], del_list])
            no_of_items_to_delete += len(del_list)
    check.summary = "Live WorkflowRuns found linked to deleted Input Files"
    check.description = "{} live workflows were found linked to deleted input files - \
                         found {} items to delete, use action for cleanup".format(len(bad_wfrs), no_of_items_to_delete)
    if fulloutput.get('problematic_provenance'):
        brief += " ({} provenance tracking)"
    check.brief_output = brief
    check.full_output = fulloutput
    check.status = 'WARN'
    check.action_message = "Will attempt to patch %s workflow_runs with deleted inputs to status=deleted." % str(len(bad_wfrs))
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_workflow_run_to_deleted(connection, **kwargs):
    action = ActionResult(connection, 'patch_workflow_run_to_deleted')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_failure': [], 'patch_success': []}
    my_key = connection.ff_keys
    for a_case in check_res['full_output']['problematic_wfrs']:
        wfruid = a_case[1]
        del_list = a_case[2]
        patch_data = {'status': 'deleted'}
        for delete_me in del_list:
            try:
                ff_utils.patch_metadata(patch_data, obj_id=delete_me, key=my_key)
            except Exception as e:
                acc_and_error = [delete_me, str(e)]
                action_logs['patch_failure'].append(acc_and_error)
            else:
                action_logs['patch_success'].append(wfruid + " - " + delete_me)
    action.output = action_logs
    action.status = 'DONE'
    if action_logs.get('patch_failure'):
        action.status = 'FAIL'
    return action


# helper functions for biorxiv check
def get_biorxiv_meta(biorxiv_id, connection):
    ''' Attempts to get metadata for provided biorxiv id
        returns the error string if fails
    '''
    try:
        biorxiv = ff_utils.get_metadata(biorxiv_id, key=connection.ff_keys, add_on='frame=object')
    except Exception as e:
        return 'Problem getting biorxiv - msg: ' + str(e)
    else:
        if not biorxiv:
            return 'Biorxiv not found!'
    return biorxiv


def get_transfer_fields(biorxiv_meta):
    fields2transfer = [
        'lab', 'contributing_labs', 'award', 'categories', 'exp_sets_prod_in_pub',
        'exp_sets_used_in_pub', 'published_by', 'static_headers',
        'static_content'
    ]
    return {f: biorxiv_meta.get(f) for f in fields2transfer if biorxiv_meta.get(f) is not None}


@check_function(uuid_list=None, false_positives=None, add_to_result=None)
def biorxiv_is_now_published(connection, **kwargs):
    ''' To restrict the check to just certain biorxivs use a comma separated list
        of biorxiv uuids in uuid_list kwarg.  This is useful if you want to
        only perform the replacement on a subset of the potential matches - i.e.
        re-run the check with a uuid list and then perform the actions on the result
        of the restricted check.

        Known cases of incorrect associations are stored in the check result in
        the 'false_positive' field of full_output.  To add new entries to this field use the
        'false_positive' kwarg with format "rxiv_uuid1: number_part_only_of_PMID, rxiv_uuid2: ID ..."
         eg. fd3827e5-bc4c-4c03-bf22-919ee8f4351f:31010829 and to reset to empty use 'RESET'

        There are some examples of the title and author list being different enough so
        that the pubmid esearch query doesn't find the journal article.  In order to
        allow the replacement, movement of all the relevant fields and adding replacement static sections
        in the action - a parameter is provided to manually input a mapping between biorxiv (uuid)
        to journal article (PMID:ID) - to add that pairing to the result full_output. It will
        be acted on by the associated action format of input is uuid PMID:nnnnnn, uuid PMID:nnnnnn

        NOTE: because the data to transfer from biorxiv to pub is obtained from the check result
        it is important to run the check (again) before executing the action in case something has
        changed since the check was run
    '''
    check = CheckResult(connection, 'biorxiv_is_now_published')
    chkstatus = ''
    chkdesc = ''
    check.action = "add_pub_and_replace_biorxiv"
    fulloutput = {'biorxivs2check': {}, 'false_positives': {}, 'GEO datasets found': {}}
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # see if a 'manual' mapping was provided as a parameter
    fndcnt = 0
    if kwargs.get('add_to_result'):
        b2p = [pair.strip().split(' ') for pair in kwargs.get('add_to_result').split(',')]
        b2p = {b.strip(): p.strip() for b, p in b2p}
        # if there was a manual mapping need to report info to transfer
        for bid, pid in b2p.items():
            b_meta = get_biorxiv_meta(bid, connection)
            if isinstance(b_meta, str):
                check.status = "FAIL"
                check.description = "Problem retrieving metadata for input data - " + b_meta
                return check
            fulloutput['biorxivs2check'].setdefault(bid, {}).update({'new_pub_ids': [pid]})
            if b_meta.get('url'):
                fulloutput['biorxivs2check'][bid].setdefault('blink', b_meta.get('url'))
            fulloutput['biorxivs2check'][bid].setdefault('data2transfer', {}).update(get_transfer_fields(b_meta))
        fndcnt = len(b2p)
    search = 'search/?'
    if kwargs.get('uuid_list'):
        suffix = '&'.join(['uuid={}'.format(u) for u in [uid.strip() for uid in kwargs.get('uuid_list').split(',')]])
    else:
        suffix = 'journal=bioRxiv&type=Publication&status=current&limit=all'
    # run the check
    search_query = search + suffix
    biorxivs = ff_utils.search_metadata(search_query, key=connection.ff_keys)
    if not biorxivs and not fndcnt:
        check.status = "FAIL"
        check.description = "Could not retrieve biorxiv records from fourfront"
        return check

    # here is where we get any previous or current false positives
    last_result = check.get_primary_result()
    # if last one was fail, find an earlier check with non-FAIL status
    it = 0
    while last_result['status'] == 'ERROR' or not last_result['kwargs'].get('primary'):
        it += 1
        # this is a daily check, so look for checks with 12h iteration
        hours = it * 12
        last_result = check.get_closest_result(diff_hours=hours)
        # if this is going forever kill it
        if hours > 100:
            err_msg = 'Can not find a non-FAIL check in last 100 hours'
            check.brief_output = err_msg
            check.full_output = {}
            check.status = 'ERROR'
            return check
    last_result = last_result.get('full_output')
    try:
        false_pos = last_result.get('false_positives', {})
    except AttributeError:  # if check errored last result is a list of error rather than a dict
        false_pos = {}
    fp_input = kwargs.get('false_positives')
    if fp_input:
        fps = [fp.strip() for fp in fp_input.split(',')]
        for fp in fps:
            if fp == 'RESET':  # reset the saved dict to empty
                false_pos = {}
                continue
            id_vals = [i.strip() for i in fp.split(':')]
            false_pos.setdefault(id_vals[0], []).append(id_vals[1])
    fulloutput['false_positives'] = false_pos
    pubmed_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json'
    problems = {}
    for bx in biorxivs:
        title = bx.get('title')
        authors = bx.get('authors')
        buuid = bx.get('uuid')
        if not (title and authors):
            # problem with biorxiv record in ff
            problems.setdefault('missing metadata', []).append(buuid)
            if not chkstatus or chkstatus != 'WARN':
                chkstatus = 'WARN'
            msg = "some biorxiv records are missing metadata used for search\n"
            if msg not in chkdesc:
                chkdesc = chkdesc + msg
        # first search with title
        suffix = '&field=title&term={}'.format(title)
        title_query = pubmed_url + suffix
        time.sleep(1)
        do_author_search = False
        ids = []
        res = requests.get(title_query)
        if res.status_code == 200:
            result = res.json().get('esearchresult')
            if not result or not result.get('idlist'):
                do_author_search = True
            else:
                ids = result.get('idlist')
        else:
            do_author_search = True  # problem with request to pubmed

        if do_author_search and authors:
            author_string = '&term=' + '%20'.join(['{}[Author]'.format(a.split(' ')[-1]) for a in authors])
            author_query = pubmed_url + author_string
            time.sleep(1)
            res = requests.get(author_query)
            if res.status_code == 200:
                result = res.json().get('esearchresult')
                if result and result.get('idlist'):
                    ids = result.get('idlist')

        if buuid in false_pos:
            ids = [i for i in ids if i not in false_pos[buuid]]
        if ids:
            # we have possible article(s) - populate check_result
            fndcnt += 1
            fulloutput['biorxivs2check'].setdefault(buuid, {}).update({'new_pub_ids': ['PMID:' + id for id in ids]})
            if bx.get('url'):
                fulloutput['biorxivs2check'][buuid].setdefault('blink', bx.get('url'))
            # here we don't want the embedded search view so get frame=object
            bmeta = get_biorxiv_meta(buuid, connection)
            fulloutput['biorxivs2check'][buuid].setdefault('data2transfer', {}).update(get_transfer_fields(bmeta))
            # look for GEO datasets
            for id_ in ids:
                result = requests.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
                                      'elink.fcgi?dbfrom=pubmed&db=gds&id={}&retmode=json'.format(id_))
                if result.status_code != 200:
                    continue
                geo_ids = [num for link in json.loads(result.text).get('linksets', [])
                           for item in link.get('linksetdbs', []) for num in item.get('links', [])]
                geo_accs = []
                for geo_id in geo_ids:
                    geo_result = requests.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/'
                                              'efetch.fcgi?db=gds&id={}'.format(geo_id))
                    if geo_result.status_code == 200:
                        geo_accs.extend([item for item in geo_result.text.split() if item.startswith('GSE')])
                if geo_accs:
                    fulloutput['GEO datasets found']['PMID:' + id_] = geo_accs

    if fndcnt != 0:
        chkdesc = "Candidate Biorxivs to replace found\nNOTE: please re-run check directly prior to running action to ensure all metadata is up to date." + chkdesc
        if not chkstatus:
            chkstatus = 'WARN'
        check.allow_action = True
    else:
        chkdesc = "No Biorxivs to replace\n" + chkdesc
        if not chkstatus:
            chkstatus = 'PASS'
        check.allow_action = False

    check.status = chkstatus
    check.summary = check.description = chkdesc
    check.brief_output = fndcnt
    check.full_output = fulloutput

    return check


@action_function()
def add_pub_and_replace_biorxiv(connection, **kwargs):
    action = ActionResult(connection, 'add_pub_and_replace_biorxiv')
    action_log = {}
    biorxiv_check_result = action.get_associated_check_result(kwargs)
    check_output = biorxiv_check_result.get('full_output', {})
    to_replace = check_output.get('biorxivs2check', {})
    for buuid, transfer_info in to_replace.items():
        error = ''
        pmids = transfer_info.get('new_pub_ids', [])
        if len(pmids) != 1:
            pmstr = ', '.join(pmids)
            action_log[buuid] = '0 or multiple pmids {} - manual intervention needed!\n\tNOTE: to transfer to a single pub you can enter the biorxiv uuid PMID in add_to_result'.format(pmstr)
            continue

        pmid = pmids[0]
        # prepare a post/patch for transferring data
        existing_fields = {}
        fields_to_patch = {}
        post_metadata = transfer_info.get('data2transfer', {})
        post_metadata['ID'] = pmid
        post_metadata['status'] = 'current'
        if 'blink' in transfer_info:
            post_metadata['aka'] = transfer_info.get('blink')

        # first try to post the pub
        pub_upd_res = None
        pub = None
        try:
            pub_upd_res = ff_utils.post_metadata(post_metadata, 'publication', key=connection.ff_keys)
        except Exception as e:
            error = str(e)
        else:
            if pub_upd_res.get('status') != 'success':
                error = pub_upd_res.get('status')
        if error:
            if "'code': 422" in error:
                # there is a conflict-see if pub is already in portal
                pub_search_res = None
                error = ''  # reset error
                try:
                    search = 'search/?type=Publication&ID={}&frame=object'.format(post_metadata['ID'])
                    pub_search_res = ff_utils.search_metadata(search, key=connection.ff_keys)
                except Exception as e:
                    error = 'SEARCH failure for {} - msg: {}'.format(pmid, str(e))
                else:
                    if not pub_search_res or len(pub_search_res) != 1:
                        error = 'SEARCH for {} returned zero or multiple results'.format(pmid)
                if error:
                    action_log[buuid] = error
                    continue

                # a single pub with that pmid is found - try to patch it
                pub = pub_search_res[0]

                for f, v in post_metadata.items():
                    if pub.get(f):
                        if f == 'status' and pub.get(f) != v:
                            fields_to_patch[f] = v
                        if f != 'ID':
                            existing_fields[f] = pub.get(f)
                    else:
                        fields_to_patch[f] = v

                if fields_to_patch:
                    try:
                        puuid = pub.get('uuid')
                        pub_upd_res = ff_utils.patch_metadata(fields_to_patch, puuid, key=connection.ff_keys)
                    except Exception as e:
                        error = 'PATCH failure for {} msg: '.format(pmid, str(e))
                    else:
                        if pub_upd_res.get('status') != 'success':
                            error = 'PATCH failure for {} msg: '.format(pmid, pub_upd_res.get('status'))
                    if error:
                        action_log[buuid] = error
                        continue
                else:  # all the fields already exist on the item
                    msg = 'NOTHING TO AUTO PATCH - {} already has all the fields in the biorxiv - WARNING values may be different!'.format(pmid)
                    action_log[buuid] = {
                        'message': msg,
                        'existing': existing_fields,
                        'possibly_new': post_metadata
                    }
            else:
                error = 'POST failure for {} msg: {}'.format(pmid, error)
                action_log[buuid] = error
                continue
        else:
            pub = pub_upd_res['@graph'][0]

        # here we have successfully posted or patched a pub
        # generate a static header with link to new pub and set status of biorxiv to replaced
        if not pub:
            action_log[buuid] = 'NEW PUB INFO NOT AVAILABLE'
            continue

        header_alias = "static_header:replaced_biorxiv_{}_by_{}".format(buuid, pmid.replace(':', '_'))
        header_name = "static-header.replaced_item_{}".format(buuid)
        header_post = {
            "body": "This biorxiv set was replaced by [{0}]({2}{1}/).".format(pmid, pub.get('uuid'), connection.ff_server),
            "award": post_metadata.get('award'),
            "lab": post_metadata.get('lab'),
            "name": header_name,
            "section_type": "Item Page Header",
            "options": {"title_icon": "info", "default_open": True, "filetype": "md", "collapsible": False},
            "title": "Note: Replaced Biorxiv",
            "status": 'released',
            "aliases": [header_alias]
        }
        huuid = None
        try_search = False
        try:
            header_res = ff_utils.post_metadata(header_post, 'static_section', key=connection.ff_keys)
        except Exception as e:
            error = 'FAILED TO POST STATIC SECTION {} - msg: '.format(str(e))
            try_search = True
        else:
            try:
                huuid = header_res['@graph'][0].get('uuid')
            except (KeyError, AttributeError) as e:  # likely a conflict - search for existing section by name
                try_search = True
        if try_search:
            try:
                search = 'search/?type=UserContent&name={}&frame=object'.format(header_name)
                header_search_res = ff_utils.search_metadata(search, key=connection.ff_keys)
            except Exception as e:
                error = 'SEARCH failure for {} - msg: {}'.format(header_name, str(e))
            else:
                if header_search_res and len(header_search_res) == 1:
                    huuid = header_search_res[0].get('uuid')
                else:
                    error = 'PROBLEM WITH STATIC SECTION CREATION - manual intervention needed'
        if error:
            action_log[buuid] = error

        patch_json = {'status': 'replaced'}
        if huuid:  # need to see if other static content exists and add this one
            existing_content = post_metadata.get('static_content', [])
            existing_content.append({'content': huuid, 'location': 'header'})
            patch_json['static_content'] = existing_content
        try:
            replace_res = ff_utils.patch_metadata(patch_json, buuid, key=connection.ff_keys)
        except Exception as e:
            error = 'FAILED TO UPDATE STATUS FOR {} - msg: '.format(buuid, str(e))
        else:
            if replace_res.get('status') != 'success':
                error = 'FAILED TO UPDATE STATUS FOR {} - msg: '.format(buuid, replace_res.get('status'))

        # do we want to add a flag to indicate if it was post or patch
        if existing_fields:
            # report that it was an incomplete patch
            msg = 'PARTIAL PATCH'
            action_log[buuid] = {
                'message': msg,
                'existing': existing_fields,
                'possibly_new': fields_to_patch,
                'all_rxiv_data': post_metadata
            }
        else:
            action_log[buuid] = {'message': 'DATA TRANSFERED TO ' + pmid}
        if error:
            action_log[buuid].update({'error': error})
    action.status = 'DONE'
    action.output = action_log
    return action


@check_function()
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(':')] = int(split_str[1])
        ret[split_str[2].strip(':')] = int(split_str[3])
        return ret

    check = CheckResult(connection, 'item_counts_by_type')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    item_counts = {}
    warn_item_counts = {}
    req_location = ''.join([connection.ff_server, 'counts?format=json'])
    counts_res = ff_utils.authorized_request(req_location, auth=connection.ff_keys)
    if counts_res.status_code >= 400:
        check.status = 'ERROR'
        check.description = 'Error (bad status code %s) connecting to the counts endpoint at: %s.' % (counts_res.status_code, req_location)
        return check
    counts_json = json.loads(counts_res.text)
    for index in counts_json['db_es_compare']:
        counts = process_counts(counts_json['db_es_compare'][index])
        item_counts[index] = counts
        if counts['DB'] != counts['ES']:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json['db_es_total'])
    item_counts['ALL'] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = 'FAIL'
        check.summary = check.description = 'Error on fourfront health page'
    elif warn_item_counts:
        check.status = 'WARN'
        check.summary = check.description = 'DB and ES item counts are not equal'
        check.brief_output = warn_item_counts
    else:
        check.status = 'PASS'
        check.summary = check.description = 'DB and ES item counts are equal'
    check.full_output = item_counts
    return check


@check_function()
def change_in_item_counts(connection, **kwargs):
    # use this check to get the comparison
    check = CheckResult(connection, 'change_in_item_counts')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    counts_check = CheckResult(connection, 'item_counts_by_type')
    latest_check = counts_check.get_primary_result()
    # get_item_counts run closest to 10 mins
    prior_check = counts_check.get_closest_result(diff_hours=24)
    if not latest_check.get('full_output') or not prior_check.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no counts_check results to run this check with.'
        return check
    diff_counts = {}
    # drill into full_output
    latest = latest_check['full_output']
    prior = prior_check['full_output']
    # get any keys that are in prior but not latest
    prior_unique = list(set(prior.keys()) - set(latest.keys()))
    for index in latest:
        if index == 'ALL':
            continue
        if index not in prior:
            diff_counts[index] = {'DB': latest[index]['DB'], 'ES': 0}
        else:
            diff_DB = latest[index]['DB'] - prior[index]['DB']
            if diff_DB != 0:
                diff_counts[index] = {'DB': diff_DB, 'ES': 0}
    for index in prior_unique:
        diff_counts[index] = {'DB': -1 * prior[index]['DB'], 'ES': 0}

    # now do a metadata search to make sure they match
    # date_created endpoints for the FF search
    # XXX: We should revisit if we really think this search is necessary. - will 3-26-2020
    to_date = datetime.datetime.strptime(latest_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    from_date = datetime.datetime.strptime(prior_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    # tracking items and ontology terms must be explicitly searched for
    search_query = ''.join(['search/?type=Item&type=OntologyTerm&type=TrackingItem',
                            '&frame=object&date_created.from=',
                            from_date, '&date_created.to=', to_date])
    search_resp = ff_utils.search_metadata(search_query, key=connection.ff_keys)
    # add deleted/replaced items
    search_query += '&status=deleted&status=replaced'
    search_resp.extend(ff_utils.search_metadata(search_query, key=connection.ff_keys))
    for res in search_resp:

        # Stick with given type name in CamelCase since this is now what we get on the counts page
        _type = res['@type'][0]
        _entry = diff_counts.get(_type)
        if not _entry:
            diff_counts[_type] = _entry = {'DB': 0, 'ES': 0}
        if _type in diff_counts:
            _entry['ES'] += 1

    check.ff_link = ''.join([connection.ff_server, 'search/?type=Item&',
                             'type=OntologyTerm&type=TrackingItem&date_created.from=',
                             from_date, '&date_created.to=', to_date])
    check.brief_output = diff_counts

    # total created items from diff counts (exclude any negative counts)
    total_counts_db = sum([diff_counts[coll]['DB'] for coll in diff_counts if diff_counts[coll]['DB'] >= 0])
    # see if we have negative counts
    # allow negative counts, but make note of, for the following types
    purged_types = ['TrackingItem', 'HiglassViewConfig']
    negative_types = [tp for tp in diff_counts if (diff_counts[tp]['DB'] < 0 and tp not in purged_types)]
    inconsistent_types = [tp for tp in diff_counts if (diff_counts[tp]['DB'] != diff_counts[tp]['ES'] and tp not in purged_types)]
    if negative_types:
        negative_str = ', '.join(negative_types)
        check.status = 'FAIL'
        check.summary = 'DB counts decreased in the past day for %s' % negative_str
        check.description = ('Positive numbers represent an increase in counts. '
                             'Some DB counts have decreased!')
    elif inconsistent_types:
        check.status = 'WARN'
        check.summary = 'Change in DB counts does not match search result for new items'
        check.description = ('Positive numbers represent an increase in counts. '
                             'The change in counts does not match search result for new items.')
    else:
        check.status = 'PASS'
        check.summary = 'There are %s new items in the past day' % total_counts_db
        check.description = check.summary + '. Positive numbers represent an increase in counts.'
    check.description += ' Excluded types: %s' % ', '.join(purged_types)
    return check


@check_function(file_type=None, status=None, file_format=None, search_add_on=None)
def identify_files_without_filesize(connection, **kwargs):
    check = CheckResult(connection, 'identify_files_without_filesize')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # must set this to be the function name of the action
    check.action = "patch_file_size"
    check.allow_action = True
    default_filetype = 'File'
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object&file_size=No value'.format(filetype, stati)
    ff = kwargs.get('file_format')
    if ff is not None:
        ff = '&file_format.file_format=' + ff
        search_query += ff
    addon = kwargs.get('search_add_on')
    if addon is not None:
        if not addon.startswith('&'):
            addon = '&' + addon
        search_query += addon
    problem_files = []
    file_hits = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    if not file_hits:
        check.allow_action = False
        check.summary = 'All files have file size'
        check.description = 'All files have file size'
        check.status = 'PASS'
        return check

    for hit in file_hits:
        hit_dict = {
            'accession': hit.get('accession'),
            'uuid': hit.get('uuid'),
            '@type': hit.get('@type'),
            'upload_key': hit.get('upload_key')
        }
        problem_files.append(hit_dict)
    check.brief_output = '{} files with no file size'.format(len(problem_files))
    check.full_output = problem_files
    check.status = 'WARN'
    check.summary = 'File metadata found without file_size'
    status_str = 'pre-release/released/released to project/uploaded'
    if kwargs.get('status'):
        status_str = kwargs.get('status')
    type_str = ''
    if kwargs.get('file_type'):
        type_str = kwargs.get('file_type') + ' '
    ff_str = ''
    if kwargs.get('file_format'):
        ff_str = kwargs.get('file_format') + ' '
    check.description = "{cnt} {type}{ff}files that are {st} don't have file_size.".format(
        cnt=len(problem_files), type=type_str, st=status_str, ff=ff_str)
    check.action_message = "Will attempt to patch file_size for %s files." % str(len(problem_files))
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_file_size(connection, **kwargs):
    action = ActionResult(connection, 'patch_file_size')
    action_logs = {'s3_file_not_found': [], 'patch_failure': [], 'patch_success': []}
    # get the associated identify_files_without_filesize run result
    filesize_check_result = action.get_associated_check_result(kwargs)
    for hit in filesize_check_result.get('full_output', []):
        bucket = connection.ff_s3.outfile_bucket if 'FileProcessed' in hit['@type'] else connection.ff_s3.raw_file_bucket
        head_info = connection.ff_s3.does_key_exist(hit['upload_key'], bucket)
        if not head_info:
            action_logs['s3_file_not_found'].append(hit['accession'])
        else:
            patch_data = {'file_size': head_info['ContentLength']}
            try:
                ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys)
            except Exception as e:
                acc_and_error = '\n'.join([hit['accession'], str(e)])
                action_logs['patch_failure'].append(acc_and_error)
            else:
                action_logs['patch_success'].append(hit['accession'])
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(reset=False)
def new_or_updated_items(connection, **kwargs):
    ''' Currently restricted to experiment sets and experiments
        search query can be modified if desired

        keeps a running total of number of new/changed items from
        when the last time the 'reset' action was run
    '''
    class DictQuery(dict):
        def get(self, path, default=None):
            keys = path.split(".")
            val = None
            for key in keys:
                if val:
                    if isinstance(val, list):
                        val = [v.get(key, default) if v else None for v in val]
                    else:
                        val = val.get(key, default)
                else:
                    val = dict.get(self, key, default)
                if not val:
                    break
            return val

    seen = {}
    dcic = {}

    def get_non_dcic_user(user, seen, dcic):
        dciclab = "4DN DCIC, HMS"
        try:
            user = user.get('uuid')
        except AttributeError:
            pass
        if user in dcic:
            return None
        if user in seen and user not in dcic:
            return seen.get(user)

        user_item = ff_utils.get_metadata(user, key=connection.ff_keys)
        seen[user] = user_item.get('display_title')
        if user_item.get('lab').get('display_title') == dciclab:
            dcic[user] = True
            return None
        return user_item.get('display_title')

    check = CheckResult(connection, 'new_or_updated_items')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    rundate = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M')
    last_result = check.get_latest_result()
    if last_result is None or last_result.get('status') == 'ERROR' or kwargs.get('reset') is True:
        # initial set up when run on each environment - should produce 0 counts
        # maybe also use for reset?
        check.brief_output = {'reset_date': rundate}
        check.full_output = {'reset_date': rundate}
        check.status = 'PASS'
        check.summary = 'Counters reset to 0'
        return check

    days_since = 7
    last_check_date = last_result.get('uuid')
    last_reset_date = last_result.get('brief_output').get('reset_date')
    check.brief_output = {'reset_date': last_reset_date}
    check.full_output = {'reset_date': last_reset_date}
    days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=days_since)).strftime('%Y-%m-%dT%H:%M')
    label2date = {
        'since last check': last_check_date,
        'since last reset': last_reset_date,
        'in the last %d days' % days_since: days_ago
    }
    earliest_date = min([last_check_date, last_reset_date, days_ago])
    search = 'search/?status=in review by lab&type={type}'
    brief_output = {}
    full_output = {}
    warn = False
    # fields used for reporting
    item_flds = ['accession', 'lab.uuid', 'lab.display_title', 'submitted_by.uuid',
                 'last_modified.modified_by.uuid']
    # can add or remove item types here
    types2chk = ['ExperimentSet', 'Experiment']
    for itype in types2chk:
        chk_query = search.format(type=itype)
        item_results = ff_utils.search_metadata(chk_query, key=connection.ff_keys, page_limit=200)
        for item in item_results:
            submitter = None
            modifier = None
            created = item.get('date_created')
            modified = None
            if item.get('last_modified', None) is not None:
                modified = item.get('last_modified').get('date_modified')
                # check to see if modified and created are essentially the same and if so ignore modified
                minute_created = ':'.join(created.split(':')[0:2])
                minute_modified = ':'.join(modified.split(':')[0:2])
                if minute_created == minute_modified:
                    modified = None

            if created and created > earliest_date:
                submitter = get_non_dcic_user(item.get('submitted_by'), seen, dcic)
            if modified and modified > earliest_date:
                modifier = get_non_dcic_user(item.get('last_modified').get('modified_by'), seen, dcic)

            # now we're ready to see which bucket item goes into
            if submitter or modifier:
                # we've got an item newer or modified since earliest date
                item_info = {fld: DictQuery(item).get(fld) for fld in item_flds}
                labname = item_info.get('lab.display_title')
                labuuid = item_info.get('lab.uuid')
                if submitter:
                    brief_output.setdefault(submitter, {}).setdefault(labname, {}).setdefault(itype, {})
                    full_output.setdefault(submitter, {}).setdefault(labname, {}).setdefault(itype, {})
                    for label, date in label2date.items():
                        newlabel = 'New ' + label
                        brief_output[submitter][labname][itype].setdefault(newlabel, 0)
                        full_output[submitter][labname][itype].setdefault(newlabel, 'None')
                        if created > date:
                            warn = True
                            # newlabel = 'New ' + label
                            # brief_output[submitter][labname][itype].setdefault(newlabel, 0)
                            brief_output[submitter][labname][itype][newlabel] += 1
                            # full_output[submitter][labname][itype].setdefault(newlabel, {'search': '', 'accessions': []})
                            if full_output[submitter][labname][itype][newlabel] == 'None' or not full_output[submitter][labname][itype][newlabel].get('search'):
                                searchdate, _ = date.split('T')
                                newsearch = '{server}/search/?q=date_created:[{date} TO *]&type={itype}&lab.uuid={lab}&submitted_by.uuid={sub}&status=in review by lab'.format(
                                    server=connection.ff_server, date=searchdate, itype=itype, lab=labuuid, sub=item_info.get('submitted_by.uuid')
                                )
                                full_output[submitter][labname][itype][newlabel] = {'search': newsearch}
                            full_output[submitter][labname][itype][newlabel].setdefault('accessions', []).append(item_info['accession'])
                if modifier:
                    brief_output.setdefault(modifier, {}).setdefault(labname, {}).setdefault(itype, {})
                    full_output.setdefault(modifier, {}).setdefault(labname, {}).setdefault(itype, {})
                    for label, date in label2date.items():
                        modlabel = 'Modified ' + label
                        brief_output[modifier][labname][itype].setdefault(modlabel, 0)
                        full_output[modifier][labname][itype].setdefault(modlabel, 'None')
                        if modified > date:
                            warn = True
                            # modlabel = 'Modified ' + label
                            # brief_output[modifier][labname][itype].setdefault(modlabel, 0)
                            brief_output[modifier][labname][itype][modlabel] += 1
                            # full_output[modifier][labname][itype].setdefault(modlabel, {'search': '', 'accessions': []})
                            if full_output[modifier][labname][itype][modlabel] == 'None' or not full_output[modifier][labname][itype][modlabel].get('search'):
                                searchdate, _ = date.split('T')
                                modsearch = ('{server}search/?q=last_modified.date_modified:[{date} TO *]'
                                             '&type={itype}&lab.uuid={lab}&last_modified.modified_by.uuid={mod}status=in review by lab').format(
                                    server=connection.ff_server, date=searchdate, itype=itype, lab=labuuid, mod=item_info.get('last_modified.modified_by.uuid')
                                )
                                full_output[modifier][labname][itype][modlabel] = {'search': modsearch}
                            full_output[modifier][labname][itype][modlabel].setdefault('accessions', []).append(item_info['accession'])
    check.brief_output.update(brief_output)
    check.full_output.update(full_output)
    if warn is True:
        check.status = 'WARN'
        check.summary = 'In review Experiments or ExperimentSets submitted or modified'
        description = "Experiments or ExperimentSets with status='in review by lab' have been submitted or modified by non-DCIC users since last reset or in the past %d days." % days_since
        check.description = description
    else:
        check.status = 'PASS'
        check.summary = 'No newly submitted or modified Experiments or ExperimentSets since last reset'
    return check


@check_function()
def clean_up_webdev_wfrs(connection, **kwargs):

    def patch_wfr_and_log(wfr, full_output):
        uuid = wfr['uuid']
        patch_json = {'uuid': uuid, 'status': 'deleted'}
        # no need to patch again
        if uuid in full_output['success']:
            return
        try:
            ff_utils.patch_metadata(patch_json, uuid, key=connection.ff_keys)
        except Exception as exc:
            # log something about str(exc)
            full_output['failure'].append('%s. %s' % (uuid, str(exc)))
        else:
            # successful patch
            full_output['success'].append(uuid)

    check = CheckResult(connection, 'clean_up_webdev_wfrs')
    check.full_output = {'success': [], 'failure': []}
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # input for test pseudo hi-c-processing-bam
    response = ff_utils.get_metadata('68f38e45-8c66-41e2-99ab-b0b2fcd20d45',
                                     key=connection.ff_keys)
    wfrlist = response['workflow_run_inputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    wfrlist = response['workflow_run_outputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    # input for test md5 and bwa-mem
    response = ff_utils.get_metadata('f4864029-a8ad-4bb8-93e7-5108f462ccaa',
                                     key=connection.ff_keys)
    wfrlist = response['workflow_run_inputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    # input for test md5 and bwa-mem
    response = ff_utils.get_metadata('f4864029-a8ad-4bb8-93e7-5108f462ccaa',
                                     key=connection.ff_keys)
    wfrlist = response['workflow_run_inputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    if check.full_output['failure']:
        check.status = 'WARN'
        check.summary = 'One or more WFR patches failed'
    else:
        check.status = 'PASS'
        if check.full_output['success']:
            check.summary = 'All WFR patches successful'
        else:
            check.summary = 'No WFR patches run'

    return check


@check_function()
def validate_entrez_geneids(connection, **kwargs):
    ''' query ncbi to see if geneids are valid
    '''
    check = CheckResult(connection, 'validate_entrez_geneids')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    problems = {}
    timeouts = 0
    search_query = 'search/?type=Gene&limit=all&field=geneid'
    genes = ff_utils.search_metadata(search_query, key=connection.ff_keys)
    if not genes:
        check.status = "FAIL"
        check.description = "Could not retrieve gene records from fourfront"
        return check
    geneids = [g.get('geneid') for g in genes]

    query = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=gene&id={id}"
    for gid in geneids:
        if timeouts > 5:
            check.status = "FAIL"
            check.description = "Too many ncbi timeouts. Maybe they're down."
            return check
        gquery = query.format(id=gid)
        # make 3 attempts to query gene at ncbi
        for count in range(3):
            resp = requests.get(gquery)
            if resp.status_code == 200:
                break
            if resp.status_code == 429:
                time.sleep(0.334)
                continue
            if count == 2:
                timeouts += 1
                problems[gid] = 'ncbi timeout'
        try:
            rtxt = resp.text
        except AttributeError:
            problems[gid] = 'empty response'
        else:
            if rtxt.startswith('Error'):
                problems[gid] = 'not a valid geneid'
    if problems:
        check.summary = "{} problematic entrez gene ids.".format(len(problems))
        check.brief_output = problems
        check.description = "Problematic Gene IDs found"
        check.status = "WARN"
    else:
        check.status = "PASS"
        check.description = "GENE IDs are all valid"
    return check


@check_function(scope='all')
def users_with_pending_lab(connection, **kwargs):
    """Define comma seperated emails in scope
    if you want to work on a subset of all the results"""
    check = CheckResult(connection, 'users_with_pending_lab')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    check.action = 'finalize_user_pending_labs'
    check.full_output = []
    check.status = 'PASS'
    cached_items = {}  # store labs/PIs for performance
    mismatch_users = []
    # do not look for deleted/replaced users
    scope = kwargs.get('scope')
    search_q = '/search/?type=User&pending_lab!=No+value&frame=object'
    # want to see all results or a subset defined by the scope
    if scope == 'all':
        pass
    else:
        emails = [mail.strip() for mail in scope.split(',')]
        for an_email in emails:
            search_q += '&email=' + an_email
    search_res = ff_utils.search_metadata(search_q, key=connection.ff_keys)
    for res in search_res:
        user_fields = ['uuid', 'email', 'pending_lab', 'lab', 'title', 'job_title']
        user_append = {k: res.get(k) for k in user_fields}
        check.full_output.append(user_append)
        # Fail if we have a pending lab and lab that do not match
        if user_append['lab'] and user_append['pending_lab'] != user_append['lab']:
            check.status = 'FAIL'
            mismatch_users.append(user_append['uuid'])
            continue
        # cache the lab and PI contact info
        if user_append['pending_lab'] not in cached_items:
            to_cache = {}
            pending_meta = ff_utils.get_metadata(user_append['pending_lab'], key=connection.ff_keys,
                                                 add_on='frame=object')
            to_cache['lab_title'] = pending_meta['display_title']
            if 'pi' in pending_meta:
                pi_meta = ff_utils.get_metadata(pending_meta['pi'], key=connection.ff_keys,
                                                add_on='frame=object')
                to_cache['lab_PI_email'] = pi_meta['email']
                to_cache['lab_PI_title'] = pi_meta['title']
                to_cache['lab_PI_viewing_groups'] = pi_meta['viewing_groups']
            cached_items[user_append['pending_lab']] = to_cache
        # now use the cache to fill fields
        for lab_field in ['lab_title', 'lab_PI_email', 'lab_PI_title', 'lab_PI_viewing_groups']:
            user_append[lab_field] = cached_items[user_append['pending_lab']].get(lab_field)

    if check.full_output:
        check.summary = 'Users found with pending_lab.'
        if check.status == 'PASS':
            check.status = 'WARN'
            check.description = check.summary + ' Run the action to add lab and remove pending_lab'
            check.allow_action = True
            check.action_message = 'Will attempt to patch lab and remove pending_lab for %s users' % len(check.full_output)
        if check.status == 'FAIL':
            check.summary += '. Mismatches found for pending_lab and existing lab'
            check.description = check.summary + '. Resolve conflicts for mismatching users before running action. See brief_output'
            check.brief_output = mismatch_users
    else:
        check.summary = 'No users found with pending_lab'
    return check


@action_function()
def finalize_user_pending_labs(connection, **kwargs):
    action = ActionResult(connection, 'finalize_user_pending_labs')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_failure': [], 'patch_success': []}
    for user in check_res.get('full_output', []):
        patch_data = {'lab': user['pending_lab']}
        if user.get('lab_PI_viewing_groups'):
            patch_data['viewing_groups'] = user['lab_PI_viewing_groups']
        # patch lab and delete pending_lab in one request
        try:
            ff_utils.patch_metadata(patch_data, obj_id=user['uuid'], key=connection.ff_keys,
                                    add_on='delete_fields=pending_lab')
        except Exception as e:
            action_logs['patch_failure'].append({user['uuid']: str(e)})
        else:
            action_logs['patch_success'].append(user['uuid'])
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(emails=None, ignore_current=False, reset_ignore=False)
def users_with_doppelganger(connection, **kwargs):
    """ Find users that share emails or have very similar names
    Args:
        emails: comma seperated emails to run the check on, i.e. when you want to ignore some of the results
        ignore_current: if there are accepted catches, put them to emails, and set ignore_current to true,
                        they will not show up next time.
        if there are caught cases, which are not problematic, you can add them to ignore list
        reset_ignore: you can reset the ignore list, and restart it, useful if you added something by mistake
    Result:
     full_output : contains two lists, one for problematic cases, and the other one for results to skip (ignore list)
    """
    check = CheckResult(connection, 'users_with_doppelganger')
    check.description = 'Reports duplicate users, and number of items they created (user1/user2)'
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # do we want to add current results to ignore list
    ignore_current = False
    if kwargs.get('ignore_current'):
        ignore_current = True
    # do we want to reset the ignore list
    reset = False
    if kwargs.get('reset_ignore'):
        reset = True
    # GET THE IGNORE LIST FROM LAST CHECKS IF NOT RESET_IGNORE
    if reset:
        ignored_cases = []
    else:
        last_result = check.get_primary_result()
        # if last one was fail, find an earlier check with non-FAIL status
        it = 0
        while last_result['status'] == 'ERROR' or not last_result['kwargs'].get('primary'):
            it += 1
            # this is a daily check, so look for checks with 12h iteration
            hours = it * 12
            last_result = check.get_closest_result(diff_hours=hours)
            # if this is going forever kill it
            if hours > 100:
                err_msg = 'Can not find a non-FAIL check in last 100 hours'
                check.brief_output = err_msg
                check.full_output = {}
                check.status = 'ERROR'
                return check
        # remove cases previously ignored
        ignored_cases = last_result['full_output'].get('ignore', [])

    # ignore contains nested list with 2 elements, 2 user @id values that should be ignored
    check.full_output = {'result': [], 'ignore': []}
    check.brief_output = []
    check.status = 'PASS'
    query = ('/search/?type=User&sort=display_title'
             '&field=display_title&field=contact_email&field=preferred_email&field=email')
    # if check was limited to certain emails
    if kwargs.get('emails'):
        emails = kwargs['emails'].split(',')
        for an_email in emails:
            an_email = an_email.strip()
            if an_email:
                query += '&email=' + an_email.strip()
    # get users
    all_users = ff_utils.search_metadata(query, key=connection.ff_keys)
    # combine all emails for each user
    for a_user in all_users:
        mail_fields = ['email', 'contact_email', 'preferred_email']
        user_mails = []
        for f in mail_fields:
            if a_user.get(f):
                user_mails.append(a_user[f].lower())
        a_user['all_mails'] = list(set(user_mails))

    # go through each combination
    combs = itertools.combinations(all_users, 2)
    cases = []
    for comb in combs:
        us1 = comb[0]
        us2 = comb[1]
        # is there a common email between the 2 users
        common_mail = list(set(us1['all_mails']) & set(us2['all_mails']))
        if common_mail:
            msg = '{} and {} share mail(s) {}'.format(
                us1['display_title'],
                us2['display_title'],
                str(common_mail))
            log = {'user1': [us1['display_title'], us1['@id'], us1['email']],
                   'user2': [us2['display_title'], us2['@id'], us2['email']],
                   'log': 'has shared email(s) {}'.format(str(common_mail)),
                   'brief': msg}
            cases.append(log)
        # if not, compare names
        else:
            score = fuzz.token_sort_ratio(us1['display_title'], us2['display_title'])
            if score > 85:
                msg = '{} and {} are similar-{}'.format(
                    us1['display_title'],
                    us2['display_title'],
                    str(score))
                log = {'user1': [us1['display_title'], us1['@id'], us1['email']],
                       'user2': [us2['display_title'], us2['@id'], us2['email']],
                       'log': 'has similar names ({}/100)'.format(str(score)),
                       'brief': msg}
                cases.append(log)

    # are the ignored ones getting out of control
    if len(ignored_cases) > 100:
        fail_msg = 'Number of ignored cases is very high, time for maintainace'
        check.brief_output = fail_msg
        check.full_output = {'result': [fail_msg, ],  'ignore': ignored_cases}
        check.status = 'FAIL'
        return check
    # remove ignored cases from all cases
    if ignored_cases:
        for an_ignored_case in ignored_cases:
            cases = [i for i in cases if i['user1'] not in an_ignored_case and i['user2'] not in an_ignored_case]
    # if ignore_current, add cases to ignored ones
    if ignore_current:
        for a_case in cases:
            ignored_cases.append([a_case['user1'], a_case['user2']])
        cases = []

    # add if they have any items referencing them
    if cases:
        for a_case in cases:
            us1_info = ff_utils.get_metadata(a_case['user1'][1] + '@@links', key=connection.ff_keys)
            item_count_1 = len(us1_info['uuids_linking_to'])
            us2_info = ff_utils.get_metadata(a_case['user2'][1] + '@@links', key=connection.ff_keys)
            item_count_2 = len(us2_info['uuids_linking_to'])
            add_on = ' ({}/{})'.format(item_count_1, item_count_2)
            a_case['log'] = a_case['log'] + add_on
            a_case['brief'] = a_case['brief'] + add_on

    check.full_output = {'result': cases,  'ignore': ignored_cases}
    if cases:
        check.summary = 'Some user accounts need attention.'
        check.brief_output = [i['brief'] for i in cases]
        check.status = 'WARN'
    else:
        check.summary = 'No user account conflicts'
        check.brief_output = []
    return check


@check_function()
def check_assay_classification_short_names(connection, **kwargs):
    check = CheckResult(connection, 'check_assay_classification_short_names')
    check.action = 'patch_assay_subclass_short'
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    subclass_dict = {
        "replication timing": "Replication timing",
        "proximity to cellular component": "Proximity-seq",
        "dna binding": "DNA binding",
        "open chromatin": "Open Chromatin",
        "open chromatin - single cell": "Open Chromatin",
        "dna-dna pairwise interactions": "Hi-C",
        "dna-dna pairwise interactions - single cell": "Hi-C (single cell)",
        "dna-dna multi-way interactions": "Hi-C (multi-contact)",
        "dna-dna multi-way interactions of selected loci": "3/4/5-C (multi-contact)",
        "dna-dna pairwise interactions of enriched regions": "IP-based 3C",
        "dna-dna pairwise interactions of selected loci": "3/4/5-C",
        "ligation-free 3c": "Ligation-free 3C",
        "transcription": "Transcription",
        "transcription - single cell": "Transcription",
        "rna-dna pairwise interactions": "RNA-DNA HiC",
        "fixed sample dna localization": "DNA FISH",
        "fixed sample rna localization": "RNA FISH",
        "single particle tracking": "SPT",
        "context-dependent reporter expression": "Reporter Expression",
        "scanning electron microscopy": "SEM",
        "transmission electron microscopy": "TEM",
        "immunofluorescence": "Immunofluorescence",
        "synthetic condensation": "OptoDroplet",
        "capture hi-c": "Enrichment Hi-C"
    }
    exptypes = ff_utils.search_metadata('search/?type=ExperimentType&frame=object',
                                        key=connection.ff_keys)
    auto_patch = {}
    manual = {}
    for exptype in exptypes:
        value = ''
        if exptype.get('assay_classification', '').lower() in subclass_dict:
            value = subclass_dict[exptype['assay_classification'].lower()]
        elif exptype.get('title', '').lower() in subclass_dict:
            value = subclass_dict[exptype['title'].lower()]
        elif exptype.get('assay_subclassification', '').lower() in subclass_dict:
            value = subclass_dict[exptype['assay_subclassification'].lower()]
        else:
            manual[exptype['@id']] = {
                'classification': exptype['assay_classification'],
                'subclassification': exptype['assay_subclassification'],
                'current subclass_short': exptype.get('assay_subclass_short'),
                'new subclass_short': 'N/A - Attention needed'
            }
        if value and exptype.get('assay_subclass_short') != value:
            auto_patch[exptype['@id']] = {
                'classification': exptype['assay_classification'],
                'subclassification': exptype['assay_subclassification'],
                'current subclass_short': exptype.get('assay_subclass_short'),
                'new subclass_short': value
            }
            check.allow_action = True
    check.full_output = {'Manual patching needed': manual, 'Patch by action': auto_patch}
    check.brief_output = {'Manual patching needed': list(manual.keys()), 'Patch by action': list(auto_patch.keys())}
    if auto_patch or manual:
        check.status = 'WARN'
        check.summary = 'Experiment Type classifications need patching'
        check.description = '{} experiment types need assay_subclass_short patched'.format(
            len(manual.keys()) + len(auto_patch.keys())
        )
        if manual:
            check.summary += ' - some manual patching needed'
    else:
        check.status = 'PASS'
        check.summary = 'Experiment Type classifications all set'
        check.description = 'No experiment types need assay_subclass_short patched'
    return check


@action_function()
def patch_assay_subclass_short(connection, **kwargs):
    action = ActionResult(connection, 'patch_assay_subclass_short')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_success': [], 'patch_failure': []}
    for k, v in check_res['full_output']['Patch by action'].items():
        try:
            ff_utils.patch_metadata({'assay_subclass_short': v['new subclass_short']}, k, key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({k: str(e)})
        else:
            action_logs['patch_success'].append(k)
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


def semver2int(semver):
    v = [num for num in semver.lstrip('v').split('.')]
    for i in range(1, len(v)):
        if len(v[i]) == 1:
            v[i] = '0' + v[i]
    return float(''.join([v[0] + '.'] + v[1:]))


@check_function()
def check_for_ontology_updates(connection, **kwargs):
    '''
    Checks for updates in one of the three main ontologies that the 4DN data portal uses:
    EFO, UBERON, and OBI.
    EFO: checks github repo for new releases and compares release tag. Release tag is a
    semantic version number starting with 'v'.
    OBI: checks github repo for new releases and compares release tag. Release tag is a 'v'
    plus the release date.
    UBERON: github site doesn't have official 'releases' (and website isn't properly updated),
    so checks for commits that have a commit message containing 'new release'

    If version numbers to compare against aren't specified in the UI, it will use the ones
    from the previous primary check result.
    '''
    check = CheckResult(connection, 'check_for_ontology_updates')
    check.summary = ''
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    ontologies = ff_utils.search_metadata(
        'search/?type=Ontology&frame=object',
        key=connection.ff_keys
    )
    ontologies = [o for o in ontologies if o['ontology_prefix'] != '4DN']
    versions = {
        o['ontology_prefix']: {
            'current': o.get('current_ontology_version'),
            'needs_update': False
        } for o in ontologies
    }
    for o in ontologies:
        # UBERON needs different behavior
        if o['ontology_prefix'] == 'UBERON':
            uberon = requests.get('http://svn.code.sf.net/p/obo/svn/uberon/releases/')
            ub_release = uberon._content.decode('utf-8').split('</li>\n  <li>')[-1]
            versions['UBERON']['latest'] = ub_release[ub_release.index('>') + 1: ub_release.index('</a>')].rstrip('/')
        # instead of repos etc, check download url for ontology header to get version
        elif o.get('download_url'):
            owl = requests.get(o['download_url'], headers={"Range": "bytes=0-2000"})
            if owl.status_code == 404:
                versions[o['ontology_prefix']]['latest'] = 'WARN: 404 at download_url'
                check.summary = '404 at download_url'
                check.description = 'One or more ontologies has a download_url with a 404 error.'
                check.description += ' Please update ontology item or try again later.'
                check.status = 'WARN'
                continue
            if 'versionIRI' in owl.text:
                idx = owl.text.index('versionIRI')
                vline = owl.text[idx:idx+150]
                if 'releases'in vline:
                    vline = vline.split('/')
                    v = vline[vline.index('releases')+1]
                    versions[o['ontology_prefix']]['latest'] = v
                    continue
                else:
                    # looks for date string in versionIRI line
                    match = re.search('(20)?([0-9]{2})-[0-9]{2}-(20)?[0-9]{2}', vline)
                    if match:
                        v = match.group()
                        versions[o['ontology_prefix']]['latest'] = v
                        continue
            # SO removed version info from versionIRI, use date field instead
            if 'oboInOwl:date' in owl.text:
                idx = owl.text.index('>', owl.text.index('oboInOwl:date'))
                vline = owl.text[idx+1:owl.text.index('<', idx)]
                v = vline.split()[0]
                versions[o['ontology_prefix']]['latest'] = datetime.datetime.strptime(v, '%d:%m:%Y').strftime('%Y-%m-%d')
    check.brief_output = []
    for k, v in versions.items():
        if v.get('latest') and '404' in v['latest']:
            check.brief_output.append('{} - 404'.format(k))
        elif not v['current']:
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        elif k == 'EFO' and semver2int(v['latest']) > semver2int(v['current']):
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        elif k != 'EFO' and v['latest'] > v['current']:
            v['needs_update'] = True
            check.brief_output.append('{} needs update'.format(k))
        else:
            check.brief_output.append('{} OK'.format(k))
    check.full_output = versions
    num = ''.join(check.brief_output).count('update')
    if '404' not in check.summary:
        if num:
            check.summary = 'Ontology updates available'
            check.description = '{} ontologies need update'.format(num)
            check.status = 'WARN'
        else:
            check.summary = 'Ontologies up-to-date'
            check.description = 'No ontology updates needed'
            check.status = 'PASS'
        if num == 1 & versions['SO']['needs_update']:
            check.status = 'PASS'
    return check


@check_function()
def states_files_without_higlass_defaults(connection, **kwargs):
    check = CheckResult(connection, 'states_files_without_higlass_defaults')
    check.action = 'patch_states_files_higlass_defaults'
    check.full_output = {'to_add': {}, 'problematic_files': {}}
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    query = '/search/?file_type=chromatin states&type=File'
    res = ff_utils.search_metadata(query, key=connection.ff_keys)
    for a_res in res:
        if not a_res.get('higlass_defaults'):
            if not a_res.get('tags'):
                check.full_output['problematic_files'][a_res['accession']] = 'missing state tag'
            else:
                check.full_output['to_add'][a_res['accession']] = a_res["tags"]

    if check.full_output['to_add']:
        check.status = 'WARN'
        check.summary = 'Ready to patch higlass_defaults'
        check.description = 'Ready to patch higlass_defaults'
        check.allow_action = True
        check.action_message = 'Will patch higlass_defaults to %s items' % (len(check.full_output['to_add']))
    elif check.full_output['problematic_files']:
        check.status = 'WARN'
        check.summary = 'There are some files without states tags'
    else:
        check.status = 'PASS'
        check.summary = 'higlass_defaults are all set'
    return check


@action_function()
def patch_states_files_higlass_defaults(connection, **kwargs):
    action = ActionResult(connection, 'patch_states_files_higlass_defaults')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_success': [], 'patch_failure': [], 'missing_ref_file': []}
    total_patches = check_res['full_output']['to_add']

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('elasticbeanstalk-%s-files' % FF_PROD_BUCKET_ENV)

    query = '/search/?type=FileReference'
    all_ref_files = ff_utils.search_metadata(query, key=connection.ff_keys)
    ref_files_tags = {}
    for ref_file in all_ref_files:
        if ref_file.get('tags'):
            for ref_file_tag in ref_file.get('tags'):
                if 'states' in ref_file_tag:
                    ref_files_tags[ref_file_tag] = {'uuid': ref_file['uuid'], 'accession': ref_file['accession']}

    for item, tag in total_patches.items():
        if ref_files_tags.get(tag[0]):
            buck_obj = ref_files_tags[tag[0]]['uuid'] + '/' + ref_files_tags[tag[0]]['accession'] + '.txt'
            obj = bucket.Object(buck_obj)
            body = obj.get()['Body'].read().decode('utf8')
            lines = body.split()
            states_colors = [item for num, item in enumerate(lines) if num % 2 != 0]
            patch = {'higlass_defaults': {'colorScale': states_colors}}
            try:
                ff_utils.patch_metadata(patch, item, key=connection.ff_keys)
            except Exception as e:
                action_logs['patch_failure'].append({item: str(e)})
            else:
                action_logs['patch_success'].append(item)
        else:
            action_logs['missing_ref_file'].append({item: 'missing rows_info reference file'})

    if action_logs['patch_failure'] or action_logs['missing_ref_file']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def check_for_strandedness_consistency(connection, **kwargs):
    check = CheckResult(connection, 'check_for_strandedness_consistency')
    check.action = 'patch_strandedness_consistency_info'
    check.full_output = {'to_patch': {}, 'problematic': {}}
    check.brief_output = []
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # Build the query (RNA-seq experiments for now)
    query = '/search/?experiment_type.display_title=RNA-seq&type=ExperimentSeq'

    # The search
    res = ff_utils.search_metadata(query, key=connection.ff_keys)
    # experiments that need to be patched
    missing_consistent_tag = []
    problematic = {'fastqs_zero_count_both_strands': [], 'fastqs_unmatch_strandedness': [], 'inconsistent_strandedness': []}
    target_experiments = []  # the experiments that we are interested in (fastqs with beta actin count tag)

    # Filtering the experiments target experiments
    for a_res in res:
        if a_res.get("strandedness"):
            strandedness_meta = a_res['strandedness']
        else:
            strandedness_meta = 'missing'

        exp_info = {'meta': a_res, 'files': [], 'tag': strandedness_meta}

        # verify that the files in the experiment have the beta-actin count info
        for a_re_file in a_res['files']:
            if a_re_file['file_format']['display_title'] == 'fastq':
                file_meta = ff_utils.get_metadata(a_re_file['accession'], connection.ff_keys)
                file_meta_keys = file_meta.keys()
                if 'beta_actin_sense_count' in file_meta_keys and 'beta_actin_antisense_count' in file_meta_keys:
                    ready = True
                    if file_meta.get('related_files'):
                        paired = True
                    else:
                        paired = False

                    file_info = {'accession': file_meta['accession'],
                                 'sense_count': file_meta['beta_actin_sense_count'],
                                 'antisense_count': file_meta['beta_actin_antisense_count'],
                                 'paired': paired}
                    exp_info['files'].append(file_info)

                else:
                    ready = False
        if ready:
            target_experiments.append(exp_info)

    # Calculates if the beta-actin count is consistent with the metadata strandedness asignment.
    if target_experiments:
        problm = False
        for target_exp in target_experiments:
            if target_exp['meta'].get('tags'):
                tags = target_exp['meta']['tags']
            else:
                tags = []
            if 'strandedness_verified' not in tags:
                #  Calculate forward, reversed or unstranded
                strandedness_report = wrangler_utils.calculate_rna_strandedness(target_exp['files'])
                if "unknown" in strandedness_report['calculated_strandedness']:
                    problematic['fastqs_unmatch_strandedness'].append({'exp': target_exp['meta']['accession'],
                                                                       'strandedness_info': strandedness_report})
                    problm = True
                elif strandedness_report['calculated_strandedness'] == "zero":
                    problematic['fastqs_zero_count_both_strands'].append({'exp': target_exp['meta']['accession'],
                                                                          'strandedness_info': strandedness_report})
                    problm = True
                elif target_exp['tag'] != strandedness_report['calculated_strandedness']:
                    problematic['inconsistent_strandedness'].append({'exp': target_exp['meta']['accession'],
                                                                     'strandedness_metadata': target_exp['tag'],
                                                                     'calculated_strandedness': strandedness_report['calculated_strandedness'],
                                                                     'files': strandedness_report['files']})
                    problm = True
                else:
                    missing_consistent_tag.append(target_exp['meta']['accession'])
                    problm = True

    if problm:
        check.status = 'WARN'
        check.description = 'Problematic experiments need to be addressed'
        msg = str(len(missing_consistent_tag) + len(problematic['fastqs_unmatch_strandedness']) + len(problematic['fastqs_zero_count_both_strands']) +
                  len(problematic['inconsistent_strandedness'])) + ' experiment(s) need to be addressed'
        check.brief_output.append(msg)

        if problematic['fastqs_zero_count_both_strands']:
            check.full_output['problematic']['fastqs_zero_count_both_strands'] = problematic['fastqs_zero_count_both_strands']
        if problematic['fastqs_unmatch_strandedness']:
            check.full_output['problematic']['fastqs_unmatch_strandedness'] = problematic['fastqs_unmatch_strandedness']
        if problematic['inconsistent_strandedness']:
            check.full_output['problematic']['inconsistent_strandedness'] = problematic['inconsistent_strandedness']
        if missing_consistent_tag:
            check.full_output['to_patch']['strandedness_verified'] = missing_consistent_tag
            check.summary = 'Some experiments are missing verified strandedness tag'
            check.allow_action = True
            check.description = 'Ready to patch verified strandedness tag'

    else:
        check.status = 'PASS'
        check.summary = 'All good!'
    return check


@action_function()
def patch_strandedness_consistency_info(connection, **kwargs):
    """Start rna_strandness runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'patch_strandedness_consistency_info')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_success': [], 'patch_failure': []}
    total_patches = check_res['full_output']['to_patch']
    for key, item in total_patches.items():
        for i in item:
            tags = {'tags': []}
            meta = ff_utils.get_metadata(i, key=connection.ff_keys)
            if meta.get('tags'):
                tags['tags'] = [tg for tg in meta['tags']]
                tags['tags'].append(key)

            else:
                tags = {'tags': [key]}
            try:
                ff_utils.patch_metadata(tags, i, key=connection.ff_keys)
            except Exception as e:
                action_logs['patch_failure'].append({i: str(e)})
            else:
                action_logs['patch_success'].append(i)

    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def check_suggested_enum_values(connection, **kwargs):
    """On our schemas we have have a list of suggested fields for
    suggested_enum tagged fields. A value that is not listed in this list
    can be accepted, and with this check we will find all values for
    each suggested enum field that is not in this list.
    There are 2 functions below:

    - find_suggested_enum
    This functions takes properties for a item type (taken from /profiles/)
    and goes field by field, looks for suggested enum lists, and is also
    recursive for taking care of sub-embedded objects (tagged as type=object).
    Additionally, it also takes ignored enum lists (enums which are not
    suggested, but are ignored in the subsequent search).

    * after running this function, we construct a search url for each field,
    where we exclude all values listed under suggested_enum (and ignored_enum)
    from the search: i.e. if it was FileProcessed field 'my_field' with options
    [val1, val2], url would be:
    /search/?type=FileProcessed&my_field!=val1&my_field!=val2&my_field!=No value

    - extract value
    Once we have the search result for a field, we disect it
    (again for subembbeded items or lists) to extract the field value, and =
    count occurences of each new value. (i.e. val3:10, val4:15)

    *deleted items are not considered by this check
    """
    check = CheckResult(connection, 'check_suggested_enum_values')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # must set this to be the function name of the action
    check.action = "add_suggested_enum_values"

    def find_suggested_enum(properties, parent='', is_submember=False):
        """Filter schema propteries for fields with suggested enums.
        This functions takes properties for a item type (taken from /profiles/)
        and goes field by field, looks for suggested enum lists, and is also
        recursive for taking care of sub-embedded objects (tagged as
        type=object). It also looks fore ignored enum lists.
        """
        def is_subobject(field):
            if field.get('type') == 'object':
                return True
            try:
                return field['items']['type'] == 'object'
            except:
                return False

        def dotted_field_name(field_name, parent_name=None):
            if parent_name:
                return "%s.%s" % (parent_name, field_name)
            else:
                return field_name

        def get_field_type(field):
            field_type = field.get('type', '')
            if field_type == 'string':
                if field.get('linkTo', ''):
                    return "Item:" + field.get('linkTo')
                # if multiple objects are linked by "anyOf"
                if field.get('anyOf', ''):
                    links = list(filter(None, [d.get('linkTo', '') for d in field.get('anyOf')]))
                    if links:
                        return "Item:" + ' or '.join(links)
                # if not object return string
                return 'string'
            elif field_type == 'array':
                return 'array of ' + get_field_type(field.get('items'))
            return field_type

        fields = []
        for name, props in properties.items():
            options = []
            # focus on suggested_enum ones
            if 'suggested_enum' not in str(props):
                continue
            # skip calculated
            if props.get('calculatedProperty'):
                continue
            is_array = False
            if is_subobject(props) and name != 'attachment':
                is_array = get_field_type(props).startswith('array')
                obj_props = {}
                if is_array:
                    obj_props = props['items']['properties']
                else:
                    obj_props = props['properties']
                fields.extend(find_suggested_enum(obj_props, name, is_array))
            else:
                field_name = dotted_field_name(name, parent)
                field_type = get_field_type(props)
                # check props here
                if 'suggested_enum' in props:
                    options = props['suggested_enum']
                    if 'ignored_enum' in props:
                        options.extend(props['ignored_enum'])
                # if array of string with enum
                if is_submember or field_type.startswith('array'):
                    sub_props = props.get('items', '')
                    if 'suggested_enum' in sub_props:
                        options = sub_props['suggested_enum']
                        if 'ignored_enum' in sub_props:
                            options.extend(sub_props['ignored_enum'])
                fields.append((field_name, options))
        return(fields)

    def extract_value(field_name, item, options=[]):
        """Given a json, find the values for a given field.
        Once we have the search result for a field, we disect it
        (again for subembbeded items or lists) to extract the field value(s)
        """
        # let's exclude also empty new_values
        options.append('')
        new_vals = []
        if '.' in field_name:
            part1, part2 = field_name.split('.')
            val1 = item.get(part1)
            if isinstance(val1, list):
                for an_item in val1:
                    if an_item.get(part2):
                        new_vals.append(an_item[part2])
            else:
                if val1.get(part2):
                    new_vals.append(val1[part2])
        else:
            val1 = item.get(field_name)
            if val1:
                if isinstance(val1, list):
                    new_vals.extend(val1)
                else:
                    new_vals.append(val1)
        # are these linkTo items
        if new_vals:
            if isinstance(new_vals[0], dict):
                new_vals = [i['display_title'] for i in new_vals]
        new_vals = [i for i in new_vals if i not in options]
        return new_vals

    outputs = []
    # Get Schemas
    schemas = ff_utils.get_metadata('/profiles/', key=connection.ff_keys)
    sug_en_cases = {}
    for an_item_type in schemas:
        properties = schemas[an_item_type]['properties']
        sug_en_fields = find_suggested_enum(properties)
        if sug_en_fields:
            sug_en_cases[an_item_type] = sug_en_fields

    for item_type in sug_en_cases:
        for i in sug_en_cases[item_type]:
            extension = ""
            field_name = i[0]
            field_option = i[1]
            # create queries - we might need multiple since there is a url length limit
            # Experimental - limit seems to be between 5260-5340
            # all queries are appended by filter for No value
            character_limit = 2000
            extensions = []
            extension = ''
            for case in field_option:
                if len(extension) < character_limit:
                    extension += '&' + field_name + '!=' + case
                else:
                    # time to finalize, add no value
                    extension += '&' + field_name + '!=' + 'No value'
                    extensions.append(extension)
                    # reset extension
                    extension = '&' + field_name + '!=' + case
            # add the leftover extension - there should be always one
            if extension:
                extension += '&' + field_name + '!=' + 'No value'
                extensions.append(extension)

            # only return this field
            f_ex = '&field=' + field_name

            common_responses = []
            for an_ext in extensions:
                q = "/search/?type={it}{ex}{f_ex}".format(it=item_type, ex=an_ext, f_ex=f_ex)
                responses = ff_utils.search_metadata(q, connection.ff_keys)
                # if this is the first response, assign this as the first common response
                if not common_responses:
                    common_responses = responses
                # if it is the subsequent responses, filter the commons ones with the new requests (intersection)
                else:
                    filter_ids = [i['@id'] for i in responses]
                    common_responses = [i for i in common_responses if i['@id'] in filter_ids]
                # let's check if we depleted common_responses
                if not common_responses:
                    break

            odds = []
            for response in common_responses:
                odds.extend(extract_value(field_name, response, field_option))
            if len(odds) > 0:
                outputs.append(
                    {
                        'item_type': item_type,
                        'field': field_name,
                        'new_values': dict(Counter(odds))
                    })
    if not outputs:
        check.allow_action = False
        check.brief_output = []
        check.full_output = []
        check.status = 'PASS'
        check.summary = 'No new values for suggested enum fields'
        check.description = 'No new values for suggested enum fields'
    else:
        b_out = []
        for res in outputs:
            b_out.append(res['item_type'] + ': ' + res['field'])
        check.allow_action = False
        check.brief_output = b_out
        check.full_output = outputs
        check.status = 'WARN'
        check.summary = 'Suggested enum fields have new values'
        check.description = 'Suggested enum fields have new values'
    return check


@action_function()
def add_suggested_enum_values(connection, **kwargs):
    """No action is added yet, this is a placeholder for
    automated pr that adds the new values."""
    # TODO: for linkTo items, the current values are @ids, and might need a change
    action = ActionResult(connection, 'add_suggested_enum_values')
    action_logs = {}
    # check_result = action.get_associated_check_result(kwargs)
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(days_back=30)
def check_external_references_uri(connection, **kwargs):
    '''
    Check if external_references.uri is missing while external_references.ref
    is present.
    '''
    check = CheckResult(connection, 'check_external_references_uri')

    days_back = kwargs.get('days_back')
    from_date_query, from_text = wrangler_utils.last_modified_from(days_back)

    search = ('search/?type=Item&external_references.ref%21=No+value' +
              '&field=external_references' + from_date_query)
    result = ff_utils.search_metadata(search, key=connection.ff_keys, is_generator=True)
    items = []
    for res in result:
        bad_refs = [er.get('ref') for er in res.get('external_references', []) if not er.get('uri')]
        if bad_refs:
            items.append({'@id': res['@id'], 'refs': bad_refs})
    names = [ref.split(':')[0] for item in items for ref in item['refs']]
    name_counts = [{na: names.count(na)} for na in set(names)]

    if items:
        check.status = 'WARN'
        check.summary = 'external_references.uri is missing'
        check.description = '%s items %sare missing uri' % (len(items), from_text)
    else:
        check.status = 'PASS'
        check.summary = 'All external_references uri are present'
        check.description = 'All dbxrefs %sare formatted properly' % from_text
    check.brief_output = name_counts
    check.full_output = items
    return check


@check_function(days_back=30)
def check_opf_lab_different_than_experiment(connection, **kwargs):
    '''
    Check if other processed files have lab (generating lab) that is different
    than the lab of that generated the experiment. In this case, the
    experimental lab needs to be added to the opf (contributing lab).
    '''
    check = CheckResult(connection, 'check_opf_lab_different_than_experiment')
    check.action = 'add_contributing_lab_opf'

    # check only recently modified files, to reduce the number of items
    days_back = kwargs.get('days_back')
    from_date_query, from_text = wrangler_utils.last_modified_from(days_back)

    search = ('search/?type=FileProcessed' +
              '&track_and_facet_info.experiment_bucket%21=No+value' +
              '&track_and_facet_info.experiment_bucket%21=processed+file' +
              '&field=experiment_sets&field=experiments' +
              '&field=lab&field=contributing_labs' + from_date_query)
    result = ff_utils.search_metadata(search, key=connection.ff_keys)

    opf = {'to_patch': [], 'problematic': []}
    exp_set_uuids = []  # Exp or ExpSet uuid list
    for res in result:
        if res.get('experiments'):
            if len(res['experiments']) != 1:  # this should not happen
                opf['problematic'].append({
                    '@id': res['@id'],
                    'experiments': [exp['uuid'] for exp in res['experiments']]})
                continue
            exp_or_set = res['experiments'][0]
        elif res.get('experiment_sets'):
            if len(res['experiment_sets']) != 1:  # this should not happen
                opf['problematic'].append({
                    '@id': res['@id'],
                    'experiment_sets': [es['uuid'] for es in res['experiment_sets']]})
                continue
            exp_or_set = res['experiment_sets'][0]
        else:  # this should not happen
            opf['problematic'].append({'@id': res['@id']})
            continue
        res['exp_set_uuid'] = exp_or_set['uuid']
        if res['exp_set_uuid'] not in exp_set_uuids:
            exp_set_uuids.append(res['exp_set_uuid'])

    # get lab of Exp/ExpSet
    result_exp_set = ff_utils.get_es_metadata(exp_set_uuids, sources=['uuid', 'properties.lab'], key=connection.ff_keys)
    uuid_2_lab = {}  # map file uuid to Exp/Set lab
    for item in result_exp_set:
        uuid_2_lab[item['uuid']] = item['properties']['lab']

    # evaluate contributing lab
    for res in result:
        if res['@id'] not in [pr['@id'] for pr in opf['problematic']]:
            contr_lab = []
            exp_set_lab = uuid_2_lab[res['exp_set_uuid']]
            if exp_set_lab == res['lab']['uuid']:
                continue
            elif res.get('contributing_labs'):
                contr_lab = [lab['uuid'] for lab in res['contributing_labs']]
                if exp_set_lab in contr_lab:
                    continue
            contr_lab.append(exp_set_lab)
            opf['to_patch'].append({
                '@id': res['@id'],
                'contributing_labs': contr_lab,
                'lab': res['lab']['display_title']})

    if opf['to_patch'] or opf['problematic']:
        check.status = 'WARN'
        check.summary = 'Supplementary files need attention'
        check.description = '%s files %sneed patching' % (len(opf['to_patch']), from_text)
        if opf['problematic']:
            check.description += ' and %s files have problems with experiments or sets' % len(opf['problematic'])
        if opf['to_patch']:
            check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'All supplementary files have correct contributing labs'
        check.description = 'All files %sare good' % from_text
    check.brief_output = {'to_patch': len(opf['to_patch']), 'problematic': len(opf['problematic'])}
    check.full_output = opf
    return check


@action_function()
def add_contributing_lab_opf(connection, **kwargs):
    '''
    Add contributing lab (the experimental lab that owns the experiment/set) to
    the other processed files (supplementary) analyzed by a different lab.
    '''
    action = ActionResult(connection, 'add_contributing_lab_opf')
    check_res = action.get_associated_check_result(kwargs)
    files_to_patch = check_res['full_output']['to_patch']
    action_logs = {'patch_success': [], 'patch_failure': []}
    for a_file in files_to_patch:
        patch_body = {'contributing_labs': a_file['contributing_labs']}
        try:
            ff_utils.patch_metadata(patch_body, a_file['@id'], key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({a_file['@id']: str(e)})
        else:
            action_logs['patch_success'].append(a_file['@id'])
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def grouped_with_file_relation_consistency(connection, **kwargs):
    ''' Check if "grouped with" file relationships are reciprocal and complete.
        While other types of file relationships are automatically updated on
        the related file, "grouped with" ones need to be explicitly (manually)
        patched on the related file. This check ensures that there are no
        related files that lack the reciprocal relationship, or that lack some
        of the group relationships (for groups larger than 2 files).
    '''
    check = CheckResult(connection, 'grouped_with_file_relation_consistency')
    check.action = 'add_grouped_with_file_relation'
    search = 'search/?type=File&related_files.relationship_type=grouped+with&field=related_files'
    files = ff_utils.search_metadata(search, key=connection.ff_keys, is_generator=True)

    file2all = {}  # map all existing relations
    file2grp = {}  # map "group with" existing relations
    for f in files:
        for rel in f['related_files']:
            rel_type = rel['relationship_type']
            rel_file = rel['file']['@id']
            file2all.setdefault(f['@id'], []).append(
                {"relationship_type": rel_type, "file": rel_file})
            if rel_type == "grouped with":
                file2grp.setdefault(f['@id'], []).append(rel_file)

    # list groups of related items
    groups = []
    newgroups = [set(rel).union({file}) for file, rel in file2grp.items()]

    # Check if any pair of groups in the list has a common file (intersection).
    # In that case, they are parts of the same group: merge them.
    # Repeat until all groups are disjoint (not intersecting).
    while len(groups) != len(newgroups):
        groups, newgroups = newgroups, []
        for a_group in groups:
            for each_group in newgroups:
                if not a_group.isdisjoint(each_group):
                    each_group.update(a_group)
                    break
            else:
                newgroups.append(a_group)

    # find missing relations
    missing = {}
    for a_group in newgroups:
        pairs = [(a, b) for a in a_group for b in a_group if a != b]
        for (a_file, related) in pairs:
            if related not in file2grp.get(a_file, []):
                missing.setdefault(a_file, []).append(related)

    if missing:
        # add existing relations to patch related_files
        to_patch = {}
        for f, r in missing.items():
            to_patch[f] = file2all.get(f, [])
            to_patch[f].extend([{"relationship_type": "grouped with", "file": rel_f} for rel_f in r])
        check.brief_output = missing
        check.full_output = to_patch
        check.status = 'WARN'
        check.summary = 'File relationships are missing'
        check.description = "{} files are missing 'grouped with' relationships".format(len(missing))
        check.allow_action = True
        check.action_message = ("DO NOT RUN if relations need to be removed! "
                                "This action will attempt to patch {} items by adding the missing 'grouped with' relations".format(len(to_patch)))
    else:
        check.status = 'PASS'
        check.summary = check.description = "All 'grouped with' file relationships are consistent"
    return check


@action_function()
def add_grouped_with_file_relation(connection, **kwargs):
    action = ActionResult(connection, 'add_grouped_with_file_relation')
    check_res = action.get_associated_check_result(kwargs)
    files_to_patch = check_res['full_output']
    action_logs = {'patch_success': [], 'patch_failure': []}
    for a_file, related_list in files_to_patch.items():
        patch_body = {"related_files": related_list}
        try:
            ff_utils.patch_metadata(patch_body, a_file, key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({a_file: str(e)})
        else:
            action_logs['patch_success'].append(a_file)
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(days_back=1)
def check_hic_summary_tables(connection, **kwargs):
    ''' Check for recently modified Hi-C Experiment Sets that are released.
        If any result is found, update the summary tables.
    '''
    check = CheckResult(connection, 'check_hic_summary_tables')
    check.action = 'patch_hic_summary_tables'
    query = ('search/?type=ExperimentSetReplicate&status=released' +
             '&experiments_in_set.experiment_type.assay_subclass_short=Hi-C')

    # search if there is any new expset
    from_date_query, from_text = wrangler_utils.last_modified_from(kwargs.get('days_back'))
    new_sets = ff_utils.search_metadata(query + from_date_query + '&field=accession', key=connection.ff_keys)

    if len(new_sets) == 0:  # no update needed
        check.status = 'PASS'
        check.summary = check.description = "No update needed for Hi-C summary tables"
        return check

    else:
        check.status = 'WARN'
        check.summary = 'New Hi-C datasets found'
        # collect ALL metadata to patch
        expsets = ff_utils.search_metadata(query, key=connection.ff_keys)

        def _add_set_to_row(row, expset, dsg):
            ''' Add ExpSet metadata to the table row for dsg'''

            row.setdefault('Data Set', {'text': dsg})
            row['Data Set'].setdefault('ds_list', set()).add(expset.get('dataset_label'))

            row.setdefault('Study', set()).add(expset.get('study'))
            row.setdefault('Class', set()).add(expset.get('study_group'))
            row.setdefault('Project', set()).add(expset['award']['project'])
            row.setdefault('Lab', set()).add(expset['lab']['display_title'])

            exp_type = expset['experiments_in_set'][0]['experiment_type']['display_title']
            row['Replicate Sets'] = row.get('Replicate Sets', dict())
            row['Replicate Sets'][exp_type] = row['Replicate Sets'].get(exp_type, 0) + 1

            biosample = expset['experiments_in_set'][0]['biosample']
            row.setdefault('Species', set()).add(biosample['biosource'][0]['individual']['organism']['name'])

            if biosample['biosource'][0].get('cell_line'):
                biosource = biosample['biosource'][0]['cell_line']['display_title']
            else:
                biosource = biosample['biosource_summary']
            row.setdefault('Biosources', set()).add(biosource)

            journal_mapping = {
                'Science (New York, N.Y.)': 'Science',
                'Genome biology': 'Genome Biol',
                'Nature biotechnology': 'Nat Biotechnol',
                'Nature genetics': 'Nat Genet',
                'Nature communications': 'Nat Commun',
                'Proceedings of the National Academy of Sciences of the United States of America': 'PNAS',
                'The Journal of biological chemistry': 'J Biol Chem',
                'The EMBO journal': 'EMBO J',
                'The Journal of cell biology': 'J Cell Biol',
                'Nature cell biology': 'Nat Cell Biol',
                'Molecular cell': 'Mol Cell'
            }
            pub = expset.get('produced_in_pub')
            if pub:
                publication = [
                    {'text': pub['short_attribution'], 'link': pub['@id']},
                    {'text': '(' + journal_mapping.get(pub['journal'], pub['journal']) + ')', 'link': pub['url']}]
                previous_pubs = [i['text'] for i in row.get('Publication', []) if row.get('Publication')]
                if publication[0]['text'] not in previous_pubs:
                    row.setdefault('Publication', []).extend(publication)

            return row

        def _row_cleanup(row):
            '''Summarize various fields in row'''
            (row['Study'],) = row['Study']
            (row['Class'],) = row['Class']

            dsg_link = '&'.join(["dataset_label=" + ds for ds in row['Data Set']['ds_list']])
            dsg_link = "/browse/?" + dsg_link.replace("+", "%2B").replace("/", "%2F").replace(" ", "+")
            row['Data Set']['link'] = dsg_link

            row['Replicate Sets'] = "<br>".join(
                [str(count) + " " + exp_type for exp_type, count in row['Replicate Sets'].items()])

            return row

        # build the table
        table = {}
        problematic = {}
        for a_set in expsets:
            # make sure dataset and study group are present
            if (a_set.get('dataset_label') is None) or (a_set.get('study_group') is None):
                problematic.setdefault('missing_info', []).append(a_set['@id'])
                continue
            # create/update row in the table
            dsg = a_set.get('dataset_group', a_set['dataset_label'])
            table[dsg] = _add_set_to_row(table.get(dsg, {}), a_set, dsg)

        # consolidate the table
        for dsg, row in table.items():
            if (len(row['Study']) == 1) and (len(row['Class']) == 1):
                table[dsg] = _row_cleanup(row)
            else:
                problematic.setdefault('multiple_info', []).append(dsg)
                table.pop(dsg)

        # split table into studygroup-specific output tables
        output = {}
        study_groups = list({row['Class'] for row in table.values()})
        for st_grp in study_groups:
            table_stg = {dsg: row for dsg, row in table.items() if row['Class'] == st_grp}

            keys = ['Data Set', 'Project', 'Replicate Sets', 'Species', 'Biosources', 'Publication', 'Study', 'Lab']
            if st_grp == "Single Time Point and Condition":
                keys.remove('Study')

            # make markdown table
            name = "data-highlights.hic." + st_grp.lower().replace(" ", "-") + ".md"
            default_col_widths = "[-1,100,-1,100,-1,-1,-1,-1]"
            if "Study" not in keys:
                default_col_widths = "[-1,100,-1,120,250,-1,170]"
            output[st_grp] = {
                'alias': "4dn-dcic-lab:" + name,
                'body': wrangler_utils.md_table_maker(table_stg, keys, name, default_col_widths)}

    check.description = "Hi-C summary tables need to be updated."
    if problematic:
        check.full_output = problematic
        if problematic.get('missing_info'):
            check.description += ' Dataset or study group are missing.'
        if problematic.get('multiple_info'):
            check.description += ' Multiple study or study groups found for the same dataset group.'
        check.description += ' Will NOT patch until these problems are resolved. See full output for details.'
    else:
        check.brief_output = [s['accession'] for s in new_sets]
        check.full_output = output
        check.allow_action = True
        check.action_message = 'Will attempt to patch {} static sections'.format(len(output))
    return check


@check_function()
def sync_users_oh_status(connection, **kwargs):
    from oauth2client.service_account import ServiceAccountCredentials
    import gspread
    import pandas as pd
    """
    Use replace function to replace `sync_users_oh` and your check name to have a quick setup
    Keyword arguments:
    """
    check = CheckResult(connection, 'sync_users_oh_status')
    my_auth = connection.ff_keys
    check.action = "sync_users_oh_start"
    check.description = "add description"
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'
    check.allow_action = False

    # get skipped users from this static section
    # if you want to skip more users, append their display titles to this static section as new line
    skip_user_static_section_uuid = '56986f99-8ebc-4d01-828d-db78b45c0840'
    skip_users = ff_utils.get_metadata(skip_user_static_section_uuid, my_auth)['content'].split('\n')
    skip_lab_display_title = ['Peter Park, HARVARD', 'DCIC Testing Lab', '4DN Viewing Lab']
    # Collect information from data portal
    all_users = ff_utils.search_metadata('/search/?type=User', key=my_auth)
    # skip bots and external devs
    all_users = [i for i in all_users if i['display_title'] not in skip_users]
    all_users_with_lab = [i for i in all_users if i.get('lab')]
    all_labs = ff_utils.search_metadata('/search/?type=Lab', key=my_auth)
    all_labs = [i for i in all_labs if i['display_title'] not in skip_lab_display_title]
    all_grants = ff_utils.search_metadata('/search/?type=Award', key=my_auth)
    # print some details
    print(len(all_labs), '- all labs in scope')
    print(len(all_grants), '- all grantsin scope')
    print(len(all_users), '- all usersin scope')
    print(len(all_users_with_lab), '- all users in scope with lab')

    # narrow users to 4DN users
    fdn_users = []
    for a_user in all_users_with_lab:
        labs = []
        awards = []
        labs = a_user.get('submits_for', [])
        labs.append(a_user['lab'])
        # skip users from test labs
        if a_user['lab']['display_title'] in skip_lab_display_title:
            continue
        for a_lab in labs:
            awards.extend(i['uuid'] for i in a_lab.get('awards', []))
        awards = [i['viewing_group'] for i in all_grants if i['uuid'] in awards]
        if '4DN' in awards or 'NOFIC' in awards:
            fdn_users.append(a_user)
    print(len(fdn_users), 'fdn users')

    # GET KEY FROM S3 To Access
    # TODO: encrypt the key same as foursight key and use same function to fetch it
    s3 = boto3.resource('s3')
    obj = s3.Object('elasticbeanstalk-fourfront-webprod-system', 'DCICjupgoogle.json')
    cont = obj.get()['Body'].read().decode()
    key_dict = json.loads(cont)
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, SCOPES)
    gc = gspread.authorize(creds)
    # Get the google sheet information
    book_id = '1zPfPjm1-QT8XdYtE2CSRA83KOhHfiRWX6rRl8E1ARSw'
    sheet_name = 'AllMembers_Testing_Updates'
    book = gc.open_by_key(book_id)
    worksheet = book.worksheet(sheet_name)
    table = worksheet.get_all_values()
    ##Convert table data into an ordered dictionary
    df = pd.DataFrame(table[1:], columns=table[0])
    user_list = df.to_dict(orient='records', into=OrderedDict)





    return check

    query_base = '/search/?type=NotMyType'
    q = query_base
    # print(q)
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        check.status = 'PASS'
        check.summary = 'All Good!'
        check.brief_output = ['All Good!']
        check.full_output = {}
        return check

    for a_res in res:
        # do something
        pass

    check.summary = ""
    check.full_output = {}
    check.status = 'WARN'
    check.allow_action = True
    return check


@action_function()
def sync_users_oh_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'sync_users_oh_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    sync_users_oh_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    # do something
    for a_res in sync_users_oh_check_result:
        assert my_auth
        assert my_env
        break
    return action


@action_function()
def patch_hic_summary_tables(connection, **kwargs):
    ''' Update the Hi-C summary tables
    '''
    action = ActionResult(connection, 'patch_hic_summary_tables')
    check_res = action.get_associated_check_result(kwargs)
    sections_to_patch = check_res['full_output']
    action_logs = {'patch_success': [], 'patch_failure': []}
    for item in sections_to_patch.values():
        try:
            ff_utils.patch_metadata({"body": item['body']}, item['alias'], key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({item['alias']: str(e)})
        else:
            action_logs['patch_success'].append(item['alias'])
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def replace_name_status(connection, **kwargs):
    """
    Use replace function to replace `replace_name` and your check/action name to have a quick setup
    Keyword arguments:
    """
    check = CheckResult(connection, 'replace_name_status')
    my_auth = connection.ff_keys
    check.action = "replace_name_start"
    check.description = "add description"
    check.brief_output = []
    check.summary = ""
    check.full_output = {}
    check.status = 'PASS'
    check.allow_action = False

    # check indexing queue
    env = connection.ff_env
    indexing_queue = ff_utils.stuff_in_queues(env, check_secondary=True)

    # if you need to check for indexing queue
    if indexing_queue:
        check.status = 'PASS'  # maybe use warn?
        check.brief_output = ['Waiting for indexing queue to clear']
        check.summary = 'Waiting for indexing queue to clear'
        check.full_output = {}
        return check

    query_base = '/search/?type=NotMyType'
    q = query_base
    # print(q)
    res = ff_utils.search_metadata(q, my_auth)
    # check if anything in scope
    if not res:
        check.status = 'PASS'
        check.summary = 'All Good!'
        check.brief_output = ['All Good!']
        check.full_output = {}
        return check

    for a_res in res:
        # do something
        pass

    check.summary = ""
    check.full_output = {}
    check.status = 'WARN'
    check.allow_action = True
    return check


@action_function()
def replace_name_start(connection, **kwargs):
    """Start runs by sending compiled input_json to run_workflow endpoint"""
    action = ActionResult(connection, 'replace_name_start')
    my_auth = connection.ff_keys
    my_env = connection.ff_env
    replace_name_check_result = action.get_associated_check_result(kwargs).get('full_output', {})
    # do something
    for a_res in replace_name_check_result:
        assert my_auth
        assert my_env
        break
    return action
