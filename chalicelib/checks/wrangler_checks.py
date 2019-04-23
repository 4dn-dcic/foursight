from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import requests
import json
import datetime
import time
import itertools
from fuzzywuzzy import fuzz


@check_function(cmp_to_last=False)
def workflow_run_has_deleted_input_file(connection, **kwargs):
    check = init_check_res(connection, 'workflow_run_has_deleted_input_file')
    chkstatus = ''
    check.status = "PASS"
    check.action = "patch_workflow_run_to_deleted"
    # run the check
    search_query = 'search/?type=WorkflowRun&status!=deleted&input_files.value.status=deleted&limit=all'
    bad_wfrs = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

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
    fulloutput = {}
    for wfr in bad_wfrs:
        infiles = wfr.get('input_files', [])
        wfruuid = wfr.get('uuid', '')
        delfiles = [f.get('value').get('uuid') for f in infiles if f.get('value').get('status') == 'deleted']
        fulloutput[wfruuid] = delfiles
    check.summary = "Live WorkflowRuns found linked to deleted Input Files"
    check.description = "%s live workflows were found linked to deleted input files - \
                         you can delete the workflows using the linked action" % len(bad_wfrs)
    check.brief_output = brief
    check.full_output = fulloutput
    check.status = 'WARN'
    check.action_message = "Will attempt to patch %s workflow_runs with deleted inputs to status=deleted." % str(len(bad_wfrs))
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_workflow_run_to_deleted(connection, **kwargs):
    action = init_action_res(connection, 'patch_workflow_run_to_deleted')
    action_logs = {'patch_failure': [], 'patch_success': []}
    check_res = action.get_associated_check_result(kwargs)
    for wfruid in check_res['full_output']:
        patch_data = {'status': 'deleted'}
        try:
            ff_utils.patch_metadata(patch_data, obj_id=wfruid, key=connection.ff_keys, ff_env=connection.ff_env)
        except Exception as e:
            acc_and_error = '\n'.join([wfruid, str(e)])
            action_logs['patch_failure'].append(acc_and_error)
        else:
            action_logs['patch_success'].append(wfruid)
    action.status = 'DONE'
    action.output = action_logs
    return action


@check_function()
def biorxiv_is_now_published(connection, **kwargs):
    check = init_check_res(connection, 'biorxiv_is_now_published')
    chkstatus = ''
    chkdesc = ''
    check.action = "add_pub_and_replace_biorxiv"
    # run the check
    search_query = 'search/?journal=bioRxiv&type=Publication&status=current&limit=all'
    biorxivs = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
    if not biorxivs:
        check.status = "FAIL"
        check.description = "Could not retrieve biorxiv records from fourfront"
        return check

    pubmed_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&field=title&'
    problems = {}
    fndcnt = 0
    fulloutput = {}
    for bx in biorxivs:
        title = bx.get('title')
        buuid = bx.get('uuid')
        if not title:
            # problem with biorxiv record in ff
            print("Biorxiv %s lacks a title" % buuid)
            if problems.get('notitle'):
                problems['notitle'].append(buuid)
            else:
                problems['notitle'] = [buuid]
            if not chkstatus or chkstatus != 'WARN':
                chkstatus = 'WARN'
            if "some biorxiv records do not have a title\n" not in chkdesc:
                chkdesc = chkdesc + "some biorxiv records do not have a title\n"
            continue
        term = 'term=%s' % title
        pubmed_query = pubmed_url + term
        time.sleep(1)
        res = requests.get(pubmed_query)
        if res.status_code != 200:
            print("We got a status code other than 200 for %s" % buuid)
            # problem with request to pubmed
            if problems.get('eutilsq'):
                problems['eutilsq'].append(buuid)
            else:
                problems['eutilsq'] = [buuid]
            if not chkstatus or chkstatus != 'WARN':
                chkstatus = 'WARN'
            if "problem with eutils query for some records\n" not in chkdesc:
                chkdesc = chkdesc + "problem with eutils query for some records\n"
            continue
        result = res.json().get('esearchresult')
        if not result or result.get('idlist') is None:
            # problem with format of results returned by esearch
            if not chkstatus or chkstatus != 'WARN':
                chkstatus = 'WARN'
            if problems.get('pubmedresult'):
                problems['pubmedresult'].append(buuid)
            else:
                problems['pubmedresult'] = [buuid]
            if "problem with results format for some records\n" not in chkdesc:
                chkdesc = chkdesc + "problem with results format for some records\n"
            continue
        ids = result.get('idlist')
        if ids:
            # we have possible article(s) - populate check_result
            fndcnt += 1
            fulloutput[buuid] = ['PMID:' + id for id in ids]

    if fndcnt != 0:
        chkdesc = "Candidate Biorxivs to replace found\n" + chkdesc
        if not chkstatus:
            chkstatus = 'WARN'
    else:
        chkdesc = "No Biorxivs to replace\n" + chkdesc
        if not chkstatus:
            chkstatus = 'PASS'

    check.status = chkstatus
    check.summary = check.description = chkdesc
    check.brief_output = fndcnt
    check.full_output = fulloutput
    check.allow_action = True
    return check


@action_function()
def add_pub_and_replace_biorxiv(connection, **kwargs):
    action = init_action_res(connection, 'add_pub_2_replace_biorxiv')
    action_log = {}
    biorxiv_check_result = action.get_associated_check_result(kwargs)
    to_replace = biorxiv_check_result.get('full_output', {})
    for buuid, pmids in to_replace.items():
        error = ''
        if len(pmids) > 1:
            pmstr = ', '.join(pmids)
            action_log[buuid] = 'multiple pmids {} - manual intervention needed!'.format(pmstr)
            continue

        pmid = pmids[0]
        biorxiv = None
        # get biorxiv info
        try:
            biorxiv = ff_utils.get_metadata(buuid, key=connection.ff_keys, add_on='frame=object')
        except Exception as e:
            error = 'Problem getting biorxiv - msg: ' + str(e)
        else:
            if not biorxiv:
                error = 'Biorxiv not found!'
        if error:
            action_log[buuid] = error
            continue

        # prepare a post/patch for transferring data
        existing_fields = {}
        fields_to_patch = {}
        fields2transfer = [
            'lab', 'award', 'categories', 'exp_sets_prod_in_pub',
            'exp_sets_used_in_pub', 'published_by'
        ]
        post_metadata = {f: biorxiv.get(f) for f in fields2transfer if biorxiv.get(f) is not None}
        post_metadata['ID'] = pmid
        if 'url' in biorxiv:
            post_metadata['aka'] = biorxiv.get('url')

        # first try to post the pub
        pub_upd_res = None
        try:
            pub_upd_res = ff_utils.post_metadata(post_metadata, 'publication', key=connection.ff_keys)
        except Exception as e:
            error = str(e)
        else:
            if pub_upd_res.get('status') != 'success':
                error = pub_upd_res.get('status')
        if error:
            if "'code': 409" in error:
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
                    if f in pub and pub.get(f):
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

        # here we have successfully posted or patched a pub
        # set status of biorxiv to replaced
        try:
            replace_res = ff_utils.patch_metadata({'status': 'replaced'}, buuid, key=connection.ff_keys)
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

    check = init_check_res(connection, 'item_counts_by_type')
    # run the check
    item_counts = {}
    warn_item_counts = {}
    req_location = ''.join([connection.ff_server, 'counts?format=json'])
    counts_res = ff_utils.authorized_request(req_location, ff_env=connection.ff_env)
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
    from ..utils import convert_camel_to_snake
    # use this check to get the comparison
    check = init_check_res(connection, 'change_in_item_counts')
    counts_check = init_check_res(connection, 'item_counts_by_type')
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
    to_date = datetime.datetime.strptime(latest_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    from_date = datetime.datetime.strptime(prior_check['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d+%H:%M')
    # tracking items and ontology terms must be explicitly searched for
    search_query = ''.join(['search/?type=Item&type=OntologyTerm&type=TrackingItem',
                            '&frame=object&date_created.from=',
                            from_date, '&date_created.to=', to_date])
    search_resp = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
    # add deleted/replaced items
    search_query += '&status=deleted&status=replaced'
    search_resp.extend(ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env))
    search_output = []
    for res in search_resp:
        # convert type to index name. e.g. ExperimentSet --> experiment_set
        res_index = convert_camel_to_snake(res['@type'][0])
        if res_index in diff_counts:
            diff_counts[res_index]['ES'] = diff_counts[res_index]['ES'] + 1 if 'ES' in diff_counts[res_index] else 1
        else:
            # db entry wasn't already present for this index
            diff_counts[res_index] = {'DB': 0, 'ES': 1}

    check.ff_link = ''.join([connection.ff_server, 'search/?type=Item&',
                             'type=OntologyTerm&type=TrackingItem&date_created.from=',
                             from_date, '&date_created.to=', to_date])
    check.brief_output = diff_counts

    # total created items from diff counts (exclude any negative counts)
    total_counts_db = sum([diff_counts[coll]['DB'] for coll in diff_counts if diff_counts[coll]['DB'] >= 0])
    # see if we have negative counts
    # allow negative counts, but make note of, for the following types
    purged_types = ['tracking_item']
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
    check = init_check_res(connection, 'identify_files_without_filesize')
    # must set this to be the function name of the action
    check.action = "patch_file_size"
    default_filetype = 'File'
    default_stati = 'released%20to%20project&status=released&status=uploaded&status=pre-release'
    filetype = kwargs.get('file_type') or default_filetype
    stati = 'status=' + (kwargs.get('status') or default_stati)
    search_query = 'search/?type={}&{}&frame=object'.format(filetype, stati)
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
    file_hits = ff_utils.search_metadata(search_query, ff_env=connection.ff_env, page_limit=200)
    for hit in file_hits:
        if hit.get('file_size') is None:
            hit_dict = {
                'accession': hit.get('accession'),
                'uuid': hit.get('uuid'),
                '@type': hit.get('@type'),
                'upload_key': hit.get('upload_key')
            }
            problem_files.append(hit_dict)
    check.brief_output = '{} files with no file size'.format(len(problem_files))
    check.full_output = problem_files
    if problem_files:
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
    else:
        check.status = 'PASS'
    return check


@action_function()
def patch_file_size(connection, **kwargs):
    action = init_action_res(connection, 'patch_file_size')
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
                ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys, ff_env=connection.ff_env)
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

        user_item = ff_utils.get_metadata(user, ff_env=connection.ff_env)
        seen[user] = user_item.get('display_title')
        if user_item.get('lab').get('display_title') == dciclab:
            dcic[user] = True
            return None
        return user_item.get('display_title')

    check = init_check_res(connection, 'new_or_updated_items')
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
        item_results = ff_utils.search_metadata(chk_query, ff_env=connection.ff_env, page_limit=200)
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
                                modsearch = '{server}search/?q=last_modified.date_modified:[{date} TO *]&type={itype}&lab.uuid={lab}&last_modified.modified_by.uuid={mod}status=in review by lab'.format(
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
            ff_utils.patch_metadata(patch_json, uuid, key=connection.ff_keys,
                                    ff_env=connection.ff_env)
        except Exception as exc:
            # log something about str(exc)
            full_output['failure'].append('%s. %s' % (uuid, str(exc)))
        else:
            # successful patch
            full_output['success'].append(uuid)

    check = init_check_res(connection, 'clean_up_webdev_wfrs')
    check.full_output = {'success': [], 'failure': []}

    # input for test pseudo hi-c-processing-bam
    response = ff_utils.get_metadata('68f38e45-8c66-41e2-99ab-b0b2fcd20d45',
                                     key=connection.ff_keys, ff_env=connection.ff_env)
    wfrlist = response['workflow_run_inputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    wfrlist = response['workflow_run_outputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    # input for test md5 and bwa-mem
    response = ff_utils.get_metadata('f4864029-a8ad-4bb8-93e7-5108f462ccaa',
                                     key=connection.ff_keys, ff_env=connection.ff_env)
    wfrlist = response['workflow_run_inputs']
    for entry in wfrlist:
        patch_wfr_and_log(entry, check.full_output)

    # input for test md5 and bwa-mem
    response = ff_utils.get_metadata('f4864029-a8ad-4bb8-93e7-5108f462ccaa',
                                     key=connection.ff_keys, ff_env=connection.ff_env)
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
    check = init_check_res(connection, 'validate_entrez_geneids')
    problems = {}
    timeouts = 0
    search_query = 'search/?type=Gene&limit=all&field=geneid'
    genes = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
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
    check = init_check_res(connection, 'users_with_pending_lab')
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
    search_res = ff_utils.search_metadata(search_q, key=connection.ff_keys, ff_env=connection.ff_env)
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
                                                 ff_env=connection.ff_env, add_on='frame=object')
            to_cache['lab_title'] = pending_meta['display_title']
            if 'pi' in pending_meta:
                pi_meta = ff_utils.get_metadata(pending_meta['pi'], key=connection.ff_keys,
                                                ff_env=connection.ff_env, add_on='frame=object')
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
    action = init_action_res(connection, 'finalize_user_pending_labs')
    check_res = action.get_associated_check_result(kwargs)
    action_logs = {'patch_failure': [], 'patch_success': []}
    for user in check_res.get('full_output', []):
        patch_data = {'lab': user['pending_lab']}
        if user.get('lab_PI_viewing_groups'):
            patch_data['viewing_groups'] = user['lab_PI_viewing_groups']
        # patch lab and delete pending_lab in one request
        try:
            ff_utils.patch_metadata(patch_data, obj_id=user['uuid'], key=connection.ff_keys,
                                    ff_env=connection.ff_env, add_on='delete_fields=pending_lab')
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
    check = init_check_res(connection, 'users_with_doppelganger')
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
                msg = '{} and {} are similar ({}/100)'.format(
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

    check.full_output = {'result': cases,  'ignore': ignored_cases}
    if cases:
        check.summary = 'Some user accounts need attention.'
        check.brief_output = [i['brief'] for i in cases]
        check.status = 'WARN'
    else:
        check.summary = 'No user account conflicts'
        check.brief_output = []
    return check
