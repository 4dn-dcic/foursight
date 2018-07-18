from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import requests
import sys
import json
import datetime
import time
import boto3


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
    # get latest results
    wfr_w_del_check = init_check_res(connection, 'workflow_run_has_deleted_input_file')
    check_res = wfr_w_del_check.get_result_by_uuid(kwargs['called_by'])
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
    return check


@check_function(confirm_on_higlass=False, filetype='all')
def files_not_registered_with_higlass(connection, **kwargs):
    """
    Used to check registration of files on higlass and also register them
    through the patch_file_higlass_uid action.
    If confirm_on_higlass is True, check each file by making a request to the
    higlass server. Otherwise, just look to see if a higlass_uid is present in
    the metadata.
    The filetype arg allows you to specify which filetypes to operate on.
    Must be one of: 'all', 'mcool', 'bg', or 'bw'
    """
    check = init_check_res(connection, 'files_not_registered_with_higlass')
    check.status = "FAIL"
    check.description = "not able to get data from fourfront"
    # keep track of mcool, bg, and bw files separately
    valid_types = ['mcool', 'bg', 'bw']
    files_to_be_reg = {}
    not_found_upload_key = []
    not_found_s3 = []
    if kwargs['filetype'] != 'all' and kwargs['filetype'] not in valid_types:
        check.description = check.summary = "Filetype must be one of: %s" % (valid_types + ['all'])
        return check
    reg_filetypes = valid_types if kwargs['filetype'] == 'all' else [kwargs['filetype']]
    check.action = "patch_file_higlass_uid"
    higlass_key = connection.ff_s3.get_higlass_key()
    # run the check
    for ftype in reg_filetypes:
        files_to_be_reg[ftype] = []
        search_query = 'search/?file_format=%s&type=FileProcessed' % ftype
        possibly_reg = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for procfile in possibly_reg:
            file_info = {
                'accession': procfile['accession'],
                'uuid': procfile['uuid'],
                'file_format': procfile['file_format'],
                'higlass_uid': procfile.get('higlass_uid')
            }
            # bg files use an bw file from extra files to register
            # mcool and bw files use themselves
            if ftype == 'bg':
                for extra in procfile.get('extra_files', []):
                    if extra['file_format'] == 'bw' and 'upload_key' in extra:
                        file_info['upload_key'] = extra['upload_key']
                        break
                if 'upload_key' not in file_info:
                    not_found_upload_key.append(file_info)
                    continue
            else:
                if 'upload_key' in procfile:
                    file_info['upload_key'] = procfile['upload_key']
                else:
                    not_found_upload_key.append(file_info['accession'])
                    continue
            # make sure file exists on s3
            if not connection.ff_s3.does_key_exist(file_info['upload_key']):
                not_found_s3.append(file_info)
                continue
            # check for higlass_uid and, if confirm_on_higlass is True, check higlass.4dnucleome.org
            if file_info.get('higlass_uid'):
                if kwargs['confirm_on_higlass'] is True:
                    higlass_get = higlass_key['server'] + '/api/v1/tileset_info/?d=%s' % file_info['higlass_uid']
                    hg_res = requests.get(higlass_get)
                    # what should I check from the response?
                    if hg_res.status_code >= 400:
                        files_to_be_reg[ftype].append(file_info)
                    elif 'error' in hg_res.json().get(file_info['higlass_uid'], {}):
                        files_to_be_reg[ftype].append(file_info)
            else:
                files_to_be_reg[ftype].append(file_info)

    check.full_output = {'files_not_registered': files_to_be_reg,
                         'files_without_upload_key': not_found_upload_key,
                         'files_not_found_on_s3': not_found_s3}
    if not_found_upload_key:
        check.status = "FAIL"
        check.summary = check.description = "Error getting upload_key from files"
    elif not_found_s3:
        check.status = 'PASS'
        check.summary = check.description = "Not all files are uploaded"
    else:
        check.status = 'PASS'
    file_count = sum([len(files_to_be_reg[ft]) for ft in files_to_be_reg])
    if check.summary:
        check.summary += '. %s files ready for registration' % file_count
        check.description += '. %s files ready for registration. Run with confirm_on_higlass=True to check against the higlass server' % file_count
    else:
        check.summary = '%s files ready for registration' % file_count
        check.description = check.summary + '. Run with confirm_on_higlass=True to check against the higlass server'
    check.action_message = "Will attempt to patch higlass_uid for %s files." % file_count
    check.allow_action = True  # allows the action to be run
    return check


@action_function()
def patch_file_higlass_uid(connection, **kwargs):
    action = init_action_res(connection, 'patch_file_higlass_uid')
    action_logs = {'patch_failure': [], 'patch_success': [],
                   'registration_failure': [], 'registration_success': 0}
    # get latest results
    higlass_check = init_check_res(connection, 'files_not_registered_with_higlass')
    if kwargs.get('called_by', None):
        higlass_check_result = higlass_check.get_result_by_uuid(kwargs['called_by'])
    else:
        higlass_check_result = higlass_check.get_primary_result()

    higlass_key = connection.ff_s3.get_higlass_key()
    authentication = (higlass_key['key'], higlass_key['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}
    to_be_registered = higlass_check_result.get('full_output', {}).get('files_not_registered')
    for ftype, hits in to_be_registered.items():
        for hit in hits:
            payload = {"filepath": connection.ff_s3.outfile_bucket + "/" + hit['upload_key']}
            if ftype == 'mcool':
                payload['filetype'] = 'cooler'
                payload['datatype'] = 'matrix'
            elif ftype in ['bg', 'bw']:
                payload['filetype'] = 'bigwig'
                payload['datatype'] = 'vector'
            # register with previous higlass_uid if already there
            if 'higlass_uid' in hit:
                payload['uuid'] = hit['higlass_uid']
            res = requests.post(higlass_key['server'] + '/api/v1/link_tile/',
                                data=json.dumps(payload), auth=authentication,
                                headers=headers)
            # update the metadata file as well, if uid wasn't already present or changed
            if res.status_code == 201:
                action_logs['registration_success'] += 1
                res_uuid = res.json()['uuid']
                if 'higlass_uid' not in hit or hit['higlass_uid'] != res_uuid:
                    patch_data = {'higlass_uid': res_uuid}
                    try:
                        ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys, ff_env=connection.ff_env)
                    except Exception as e:
                        acc_and_error = '\n'.join([hit['accession'], str(e)])
                        action_logs['patch_failure'].append(acc_and_error)
                    else:
                        action_logs['patch_success'].append(hit['accession'])
            else:
                action_logs['registration_failure'].append(hit['accession'])
    action.status = 'DONE'
    action.output = action_logs
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
    # use this check to get the comparison
    check = init_check_res(connection, 'change_in_item_counts')
    counts_check = init_check_res(connection, 'item_counts_by_type')
    latest = counts_check.get_primary_result()
    # get_item_counts run closest to 10 mins
    prior = counts_check.get_closest_result(diff_hours=24)
    if not latest.get('full_output') or not prior.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no counts_check results to run this check with.'
        return check
    diff_counts = {}
    # drill into full_output
    latest = latest['full_output']
    prior = prior['full_output']
    # get any keys that are in prior but not latest
    prior_unique = list(set(prior.keys()) - set(latest.keys()))
    for index in latest:
        if index == 'ALL':
            continue
        if index not in prior:
            diff_counts[index] = latest[index]['DB']
        else:
            diff_DB = latest[index]['DB'] - prior[index]['DB']
            if diff_DB != 0:
                diff_counts[index] = diff_DB
    for index in prior_unique:
        diff_counts[index] = -1 * prior[index]['DB']
    check.brief_output = diff_counts

    # now do a metadata search to make sure they match
    date_str = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H\:%M')
    search_query = ''.join(['search/?type=Item&frame=object&q=date_created:>=', date_str])
    search_resp = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
    # add deleted/replaced items
    search_query += '&status=deleted&status=replaced'
    search_resp.extend(ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env))
    search_output = []
    for res in search_resp:
        search_output.append({
            'uuid': res.get('uuid'),
            '@id': res.get('@id'),
            'date_created': res.get('date_created'),
            'status': res.get('status')
        })
    check.ff_link = ''.join([connection.ff_server, 'search/?type=Item&q=date_created:>=', date_str])
    check.full_output = search_output

    # total created items from diff counts (exclude any negative counts)
    total_counts = sum([diff_counts[coll] for coll in diff_counts if diff_counts[coll] >= 0])
    # see if we have negative counts
    negative_counts = any([diff_counts[coll] < 0 for coll in diff_counts])
    if negative_counts:
        check.status = 'FAIL'
        check.summary = 'One or more item counts has decreased in the past day'
        check.description = ('Positive numbers represent an increase in counts. '
                             'Some counts have decreased!')
    elif total_counts != len(search_output):
        check.status = 'WARN'
        check.summary = 'Change in DB counts does not match search result for new items'
        check.description = ('Positive numbers represent an increase in counts. '
                             'The change in counts does not match search result for new items.')
    else:
        check.status = 'PASS'
        check.summary = 'There are %s new items in the past day' % total_counts
        check.description = check.summary + '. Positive numbers represent an increase in counts.'
    return check


@check_function(search_add_on=None)
def identify_files_without_filesize(connection, **kwargs):
    check = init_check_res(connection, 'identify_files_without_filesize')
    # must set this to be the function name of the action
    check.action = "patch_file_size"
    search_query = ('search/?type=File&status=released%20to%20project'
                    '&status=released&status=uploaded&frame=object')
    if kwargs.get('search_add_on'):
        search_query = ''.join([search_query, kwargs['search_add_on']])
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
    check.full_output = problem_files
    if problem_files:
        check.status = 'WARN'
        check.summary = 'File metadata found without file_size'
        check.description = "One or more files that are released/released to project/uploaded don't have file_size."
        check.action_message = "Will attempt to patch file_size for %s files." % str(len(problem_files))
        check.allow_action = True # allows the action to be run
    else:
        check.status = 'PASS'
    return check


@action_function()
def patch_file_size(connection, **kwargs):
    action = init_action_res(connection, 'patch_file_size')
    action_logs = {'s3_file_not_found': [], 'patch_failure': [], 'patch_success': []}
    # get latest results from identify_files_without_filesize
    filesize_check = init_check_res(connection, 'identify_files_without_filesize')
    filesize_check_result = filesize_check.get_result_by_uuid(kwargs['called_by'])
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


@check_function()
def biosource_cell_line_value(connection, **kwargs):
    '''
    checks cell line biosources to make sure they have an associated ontology term
    '''
    check = init_check_res(connection, 'biosource_cell_line_value')

    cell_line_types = ["primary cell", "primary cell line", "immortalized cell line",
                       "in vitro differentiated cells", "induced pluripotent stem cell line",
                       "stem cell", "stem cell derived cell line"]
    biosources = ff_utils.search_metadata('search/?type=Biosource', ff_env=connection.ff_env, page_limit=200)
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
                # detail = 'In Biosource {}'.format(value['@id']) + \
                #      ' - Missing Required cell_line field value for biosource type  ' + \
                #      '{}'.format(bstype)
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

    ext = ff_utils.search_metadata('search/?award.project=External&type=ExperimentSet',
                                   ff_env=connection.ff_env, page_limit=200)
    no_pub = []
    for expset in ext:
        if not expset.get('publications_of_set') and not expset.get('produced_in_pub'):
            # add to Output
            no_pub.append({'uuid': expset['uuid'],
                           '@id': expset['@id'],
                           'description': expset.get('description'),
                           'lab': expset['lab'].get('display_title'),
                           'error': 'Missing attribution to a publication'})
    if no_pub:
        check.status = 'WARN'
        check.summary = 'External experiment sets found without associated publication'
        check.description = '{} external experiment sets are missing attribution to a publication.'.format(len(no_pub))
    else:
        check.status = 'PASS'
        check.summary = 'No external experiment sets are missing publication'
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

    opf_expsets = ff_utils.search_metadata('search/?type=ExperimentSet&other_processed_files.files.uuid%21=No+value',
                                           ff_env=connection.ff_env, page_limit=100)
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
                                           ff_env=connection.ff_env, page_limit=100)
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

# @check_function()
# def proc_files_without_contributing_labs(connection, **kwargs):
#     check = init_check_res(connection, 'proc_files_without_contributing_labs')
#     pfile_search = ('search/?lab.display_title=4DN+DCIC+Lab%2C+HMS'
#                     '&status%21=to+be+uploaded+by+workflow&type=FileProcessed')
#     pfiles = ff_utils.search_metadata(pfile_search, ff_env=connection.ff_env)
#     pfile_dict = {pf['@id']: [c_lab['@id'] for c_lab in pf['contributing_labs'] if
#                               pf.get('contributing_labs')] for pf in pfiles}
