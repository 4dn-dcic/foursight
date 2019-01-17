from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import re
import requests
import datetime
import json


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
        'restricted': 3,
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
    es_items = ff_utils.get_es_metadata(itemids, key=ffkey, chunk_size=200, is_generator=True)
    for es_item in es_items:
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
                ignore = id2item.get(iid).get('to_ignore')
                if ignore is not None and lid in ignore:
                    continue
                else:
                    mismatches.setdefault(iid, []).append(lid)

        if len(linked2get) > MIN_CHUNK_SIZE or i + 1 == len(itemids):  # only query es when we have more than a set number of ids (500)
            linked2chk = ff_utils.get_es_metadata(list(linked2get.keys()), key=ffkey, chunk_size=200, is_generator=True)
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
    for pfinfo in item.get('properties').get('other_processed_files', []):
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
