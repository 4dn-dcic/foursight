from dcicutils import ff_utils
import re
import requests
import datetime
from .helpers import wrangler_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


STATUS_LEVEL = {
    'released': 4,
    'archived': 4,
    'current': 4,
    'revoked': 4,
    'released to project': 3,
    'pre-release': 2,
    'restricted': 4,
    'planned': 2,
    'archived to project': 3,
    'in review by lab': 1,
    'released to lab': 1,
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


@check_function()
def biosource_cell_line_value(connection, **kwargs):
    '''
    checks cell line biosources to make sure they have an associated ontology term
    '''
    check = CheckResult(connection, 'biosource_cell_line_value')

    cell_line_types = ["primary cell", "primary cell line", "immortalized cell line",
                       "induced pluripotent stem cell", "stem cell", "stem cell derived cell line"]
    biosources = ff_utils.search_metadata(
        'search/?type=Biosource&cell_line.display_title=No+value&frame=object' +
        ''.join(['&biosource_type=' + c for c in cell_line_types]),
        key=connection.ff_keys)
    missing = []
    for biosource in biosources:
        missing.append({'uuid': biosource['uuid'],
                        '@id': biosource['@id'],
                        'biosource_type': biosource.get('biosource_type'),
                        'description': biosource.get('description'),
                        'error': 'Missing cell_line OntologyTerm'})
    check.full_output = missing
    check.brief_output = [item['@id'] for item in missing]
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
    check = CheckResult(connection, 'external_expsets_without_pub')

    ext = ff_utils.search_metadata('search/?award.project=External&type=ExperimentSet&frame=object',
                                   key=connection.ff_keys, page_limit=50)
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
    check = CheckResult(connection, 'expset_opfsets_unique_titles')

    opf_expsets = ff_utils.search_metadata('search/?type=ExperimentSet&other_processed_files.files.uuid%21=No+value&frame=object',
                                           key=connection.ff_keys, page_limit=50)
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
    check = CheckResult(connection, 'expset_opf_unique_files_in_experiments')

    opf_expsets = ff_utils.search_metadata('search/?type=ExperimentSet&other_processed_files.files.uuid%21=No+value',
                                           key=connection.ff_keys, page_limit=25)
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


@check_function(days_back_as_string='14')
def expset_opf_unique_files(connection, **kwargs):
    '''
    Checks Experiments and Experiment Sets with other_processed_files and looks
    if any opf is also present within the raw, processed or reference files.
    '''
    check = CheckResult(connection, 'expset_opf_unique_files')
    days_back = kwargs.get('days_back_as_string')
    from_date_query, from_text = wrangler_utils.last_modified_from(days_back)
    # opfs can be on Exps or ExpSets: search ExpSets for each case and merge results
    opf_query = ('other_processed_files.files.uuid%21=No+value' +
                 from_date_query +
                 '&field=experiments_in_set&field=processed_files&field=other_processed_files')
    opf_sets = ff_utils.search_metadata(
        'search/?type=ExperimentSet&' + opf_query, key=connection.ff_keys)
    opf_exps = ff_utils.search_metadata(
        'search/?type=ExperimentSet&experiments_in_set.' + opf_query, key=connection.ff_keys)
    # merge
    es_list = [expset['@id'] for expset in opf_sets]
    for expset in opf_exps:
        if expset['@id'] not in es_list:
            opf_sets.append(expset)

    errors = {}
    for expset in opf_sets:
        # skip sets without experiments
        if not expset.get('experiments_in_set'):
            continue

        opfs_id = []  # list opfs @id in the ExpSet and Exps
        all_files = []  # list all raw, processed and reference files in the ExpSet
        if expset.get('other_processed_files'):
            opfs_id.extend([f['@id'] for grp in expset['other_processed_files'] for f in grp.get('files', [])])
        for exp in expset['experiments_in_set']:
            if exp.get('other_processed_files'):
                opfs_id.extend([f['@id'] for grp in exp['other_processed_files'] for f in grp.get('files', [])])
            all_files.extend([f['@id'] for f in exp.get('files', [])])
            all_files.extend([f['@id'] for f in exp.get('processed_files', [])])
            all_files.extend([f['@id'] for f in exp.get('reference_files', [])])
        all_files.extend([f['@id'] for f in expset.get('processed_files', [])])

        # compare opfs and files lists to find duplicates
        for opf in opfs_id:
            if opf in all_files:
                errors.setdefault(expset['@id'], []).append(opf)
    if errors:
        check.status = 'WARN'
        check.summary = '{} exp sets found with files that are also other_processed_files'.format(len(errors))
        check.description = ('{} Experiment Sets {}found that have other_processed_files'
                             ' which are also present in raw, processed or reference files'.format(len(errors), from_text))
    else:
        check.status = 'PASS'
        check.summary = 'No exp sets found with files that are also other_processed_files'
        check.description = ('No Experiment Sets {}found that have other_processed_files'
                             ' which are also present in raw, processed or reference files'.format(from_text))
    check.full_output = errors

    return check


@check_function()
def paired_end_info_consistent(connection, **kwargs):
    '''
    Check that fastqs with a paired_end number have a paired_with related_file, and vice versa
    '''
    check = CheckResult(connection, 'paired_end_info_consistent')

    search1 = 'search/?type=FileFastq&file_format.file_format=fastq&related_files.relationship_type=paired+with&paired_end=No+value'
    search2 = 'search/?type=FileFastq&file_format.file_format=fastq&related_files.relationship_type!=paired+with&paired_end%21=No+value'

    results1 = ff_utils.search_metadata(search1 + '&frame=object', key=connection.ff_keys)
    results2 = ff_utils.search_metadata(search2 + '&frame=object', key=connection.ff_keys)

    results = {'paired with file missing paired_end number':
               [result1['@id'] for result1 in results1],
               'file with paired_end number missing "paired with" related_file':
               [result2['@id'] for result2 in results2]}

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
    check.full_output = results
    check.brief_output = [item for val in results.values() for item in val]
    return check


@check_function()
def workflow_properties(connection, **kwargs):
    check = CheckResult(connection, 'workflow_properties')

    workflows = ff_utils.search_metadata('search/?type=Workflow&category!=provenance&frame=object',
                                         key=connection.ff_keys)
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
    check = CheckResult(connection, 'page_children_routes')

    page_search = 'search/?type=Page&format=json&children.name%21=No+value'
    results = ff_utils.search_metadata(page_search, key=connection.ff_keys)
    problem_routes = {}
    for result in results:
        if result['name'] != 'resources/data-collections':
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
    check = CheckResult(connection, 'check_help_page_urls')
    server = connection.ff_keys['server']
    help_results = ff_utils.search_metadata(
        'search/?type=StaticSection&q=help&status=released&field=body&field=options',
        key=connection.ff_keys
    )
    resource_results = ff_utils.search_metadata(
        'search/?type=StaticSection&q=resources&status=released&field=body&field=options',
        key=connection.ff_keys
    )
    results = help_results
    results = results + [item for item in resource_results if item['@id'] not in [res['@id'] for res in results]]
    sections_w_broken_links = {}
    addl_exceptions = {}
    timeouts = {}
    for result in results:
        broken_links = []
        body = result.get('body', '')
        urls = []
        if result.get('options', {}).get('filetype') == 'md':
            # look for markdown links - e.g. [text](link)
            links = re.findall(r'\[[^\]]+\]\([^\)]+\)', body)
            for link in links:
                # test only link part of match (not text part, even if it looks like a link)
                idx = link.index(']')
                url = link[link.index('(', idx)+1:-1]
                urls.append(url)
                # remove these from body so body can be checked for other types of links
                body = body[:body.index(link)] + body[body.index(link)+len(link):]
        # looks for links starting with http (full) or / (relative) inside parentheses or brackets
        urls += re.findall(r'[\(\[=]["]*(http[^\s\)\]"]+|/[^\s\)\]"]+)[\)\]"]', body)
        for url in urls:
            if url.startswith('mailto'):
                continue
            elif 'biorxiv.org' in url.lower():
                # biorxiv does not allow programmatic requests - skip
                continue
            if url.startswith('#'):  # section of static page
                url = result['@id'] + url
            if url.startswith('/'):  # fill in appropriate url for relative link
                url = server + url
            if url.startswith(server.rstrip('/') + '/search/') or url.startswith(server.rstrip('/') + '/browse/'):
                continue
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                request = requests.get(url.replace('&amp;', '&'), timeout=2, headers=headers)
                if request.status_code == 403 and 'doi.org' in url:
                    # requests to doi.org that get redirected to biorxiv fail with 403
                    addl_exceptions.setdefault(result['@id'], {})
                    addl_exceptions[result['@id']][url] = str(403)
                elif request.status_code not in [200, 412]:
                    broken_links.append((url, request.status_code))
            except requests.exceptions.Timeout:
                timeouts.setdefault(result['@id'], [])
                timeouts[result['@id']].append(url)
            except requests.exceptions.SSLError:
                continue
            except Exception as e:
                addl_exceptions.setdefault(result['@id'], {})
                addl_exceptions[result['@id']][url] = str(e)
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
    check.full_output = {
        'broken links': sections_w_broken_links,
        'timed out requests': timeouts,
        'additional exceptions': addl_exceptions
    }
    return check


@check_function(run_on_uuid_list=None, add_res_to_ignore=False, reset_ignore=False)
def check_search_urls(connection, **kwargs):
    '''Check for URLs in static sections that link to a search or browse page
    and return no results. Keeps a list of items to ignore.

    Args:
    - run_on_uuid_list: if present, run the check only on these items (list of
      uuids, comma separated).
    - add_res_to_ignore: if True, add all results to the ignore list and they
      will not show up next time (default is False). Use in combination with run_on_uuid_list.
    - reset_ignore: if True, empty the ignore list (default is False).

    Results:
    - full_output.result: static sections have URLs to search/browse with 0 results.
    - full_output.ignore: static sections in the ignore list are not checked by
      current and future runs of the check. Can be modified by check kwargs.
    '''
    check = CheckResult(connection, 'check_search_urls')

    # get items to ignore from previous check result, unless reset_ignore is True
    if kwargs.get('reset_ignore') is True:
        ignored_sections = []
    else:
        last_result = check.get_primary_result()
        # if last one was fail, find an earlier check with non-FAIL status
        it = 0
        while last_result['status'] == 'ERROR' or not last_result['kwargs'].get('primary'):
            it += 1
            hours = it * 7 * 24  # this is a weekly check, so look for checks with 7 days iteration
            last_result = check.get_closest_result(diff_hours=hours)
            if it > 4:
                check.summary = 'Cannot find a non-fail primary check in the past 4 weeks'
                check.status = 'ERROR'
                return check
        # remove cases previously ignored
        ignored_sections = last_result['full_output'].get('ignore', [])

    query = 'search/?type=StaticSection&status%21=deleted&status%21=draft'
    # if check is limited to certain uuids
    if kwargs.get('run_on_uuid_list'):
        uuids = kwargs['run_on_uuid_list'].split(',')
        query += ''.join(['&uuid=' + u.strip() for u in uuids])
    results = ff_utils.search_metadata(query + '&field=body&field=uuid',
                                       key=connection.ff_keys)
    problematic_sections = {}
    results_filtered = [r for r in results if r['uuid'] not in ignored_sections]
    for result in results_filtered:
        body = result.get('body', '')
        # search links for search or browse pages, either explicit or relative
        urls = re.findall(r'[\(\[=]["]*(?:[^\s\)\]"]+(?:4dnucleome|elasticbeanstalk)[^\s\)\]"]+|/)((?:browse|search)/\?[^\s\)\]"]+)[\)\]"]', body)
        if urls:
            for url in urls:
                url = url.replace('&amp;', '&')  # replace HTML &amp;
                url = re.sub(r'&limit=[^&]*|limit=[^&]*&?', '', url)  # remove limit if present
                q_results = ff_utils.search_metadata(url + '&limit=1&field=uuid', key=connection.ff_keys)
                if len(q_results) == 0:
                    problematic_sections.setdefault(result['uuid'], [])
                    problematic_sections[result['uuid']].append(url)

    # move results to ignored if add_res_to_ignore == True
    if problematic_sections and kwargs.get('add_res_to_ignore') is True:
        ignored_sections.extend([uuid for uuid in problematic_sections.keys() if uuid not in ignored_sections])
        problematic_sections = {}

    if problematic_sections:
        check.status = 'WARN'
        check.summary = 'Empty search links found'
        check.description = ('{} static sections currently have empty search links.'
                             ''.format(len(problematic_sections.keys())))
    else:
        check.status = 'PASS'
        check.summary = 'No empty search links found'
        check.description = check.summary
    check.full_output = {'result': problematic_sections, 'ignore': ignored_sections}
    return check


@check_function(id_list=None)
def check_status_mismatch(connection, **kwargs):
    check = CheckResult(connection, 'check_status_mismatch')
    id_list = kwargs['id_list']

    MIN_CHUNK_SIZE = 200
    # embedded sub items should have an equal or greater level
    # than that of the item in which they are embedded
    id2links = {}
    id2status = {}
    id2item = {}
    stati2search = ['released', 'released_to_project']
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
        itemres = ff_utils.search_metadata(item_search, key=connection.ff_keys, page_limit=500)
        itemids = [item.get('uuid') for item in itemres]
    es_items = ff_utils.get_es_metadata(itemids, key=connection.ff_keys, chunk_size=200, is_generator=True)
    for es_item in es_items:
        label = es_item.get('embedded').get('display_title')
        desc = es_item.get('object').get('description')
        lab = es_item.get('embedded').get('lab').get('display_title')
        status = es_item.get('properties').get('status', 'in review by lab')
        opfs = _get_all_other_processed_files(es_item)
        id2links[es_item.get('uuid')] = [li.get('uuid') for li in es_item.get('linked_uuids_embedded')]
        id2status[es_item.get('uuid')] = STATUS_LEVEL.get(status)
        id2item[es_item.get('uuid')] = {'label': label, 'status': status, 'lab': lab,
                                        'description': desc, 'to_ignore': list(set(opfs))}

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
            linked2chk = ff_utils.get_es_metadata(list(linked2get.keys()), key=connection.ff_keys,
                                                  chunk_size=200, is_generator=True)
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
            key = '{} | {} | {} | {}'.format(
                eid, eset.get('label'), eset.get('status'), eset.get('description'))
            brief_output.setdefault(eset.get('lab'), {}).update({key: len(mids)})
            for mid in mids:
                mitem = id2item.get(mid)
                val = '{} | {} | {}'.format(mid, mitem.get('label'), mitem.get('status'))
                full_output.setdefault(eset.get('lab'), {}).setdefault(key, []).append(val)
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


@check_function(id_list=None)
def check_opf_status_mismatch(connection, **kwargs):
    '''
    Check to make sure that collections of other_processed_files don't have
    status mismatches. Specifically, checks that (1) all files in an
    other_processed_files collection have the same status; and (2) the status of
    the experiment set is on the same status level or higher than the status of
    files in the other_processed_files collection (e.g., if the other_processed_files
    were released when the experiment set is in review by lab.)
    '''
    check = CheckResult(connection, 'check_opf_status_mismatch')

    opf_set = ('search/?type=ExperimentSet&other_processed_files.title%21=No+value&field=status'
               '&field=other_processed_files&field=experiments_in_set.other_processed_files')
    opf_exp = ('search/?type=ExperimentSet&other_processed_files.title=No+value'
               '&experiments_in_set.other_processed_files.title%21=No+value'
               '&field=experiments_in_set.other_processed_files&field=status')
    opf_set_results = ff_utils.search_metadata(opf_set, key=connection.ff_keys)
    opf_exp_results = ff_utils.search_metadata(opf_exp, key=connection.ff_keys)
    results = opf_set_results + opf_exp_results
    # extract file uuids
    files = []
    for result in results:
        if result.get('other_processed_files'):
            for case in result['other_processed_files']:
                files.extend([i['uuid'] for i in case['files']])
                if case.get('higlass_view_config'):
                    files.append(case['higlass_view_config'].get('uuid'))
        if result.get('experiments_in_set'):
            for exp in result['experiments_in_set']:
                for case in exp['other_processed_files']:
                    files.extend([i['uuid'] for i in case['files']])
    # get metadata for files, to collect status
    resp = ff_utils.get_es_metadata(list(set(files)),
                                    sources=['links.quality_metric', 'object.status', 'uuid'],
                                    key=connection.ff_keys)
    opf_status_dict = {item['uuid']: item['object']['status'] for item in resp if item['uuid'] in files}
    opf_linked_dict = {
        item['uuid']: item.get('links', {}).get('quality_metric', []) for item in resp if item['uuid'] in files
    }
    quality_metrics = [uuid for item in resp for uuid in item.get('links', {}).get('quality_metric', [])]
    qm_resp = ff_utils.get_es_metadata(list(set(quality_metrics)),
                                       sources=['uuid', 'object.status'],
                                       key=connection.ff_keys)
    opf_other_dict = {item['uuid']: item['object']['status'] for item in qm_resp if item not in files}
    check.full_output = {}
    for result in results:
        hg_dict = {item['title']: item.get('higlass_view_config', {}).get('uuid')
                   for item in result.get('other_processed_files', [])}
        titles = [item['title'] for item in result.get('other_processed_files', [])]
        titles.extend([item['title'] for exp in result.get('experiments_in_set', [])
                       for item in exp.get('other_processed_files', [])])
        titles = list(set(titles))
        problem_dict = {}
        for title in titles:
            file_list = [item for fileset in result.get('other_processed_files', [])
                         for item in fileset['files'] if fileset['title'] == title]
            file_list.extend([item for exp in result.get('experiments_in_set', [])
                              for fileset in exp['other_processed_files']
                              for item in fileset['files'] if fileset['title'] == title])
            statuses = set([opf_status_dict[f['uuid']] for f in file_list])
            # import pdb; pdb.set_trace()
            if not statuses:
                # to account for empty sections that may not yet contain files
                pass
            elif len(statuses) > 1:  # status mismatch in opf collection
                scores = set([STATUS_LEVEL.get(status, 0) for status in list(statuses)])
                if len(scores) > 1:
                    problem_dict[title] = {f['@id']: {'status': opf_status_dict[f['uuid']]} for f in file_list}
                    if hg_dict.get(title):
                        problem_dict[title][hg_dict[title]] = {'status': opf_status_dict[hg_dict[title]]}
            elif hg_dict.get(title) and STATUS_LEVEL[list(statuses)[0]] != STATUS_LEVEL[opf_status_dict[hg_dict[title]]]:
                if not (list(statuses)[0] == 'pre-release' and opf_status_dict[hg_dict[title]] == 'released to lab'):
                    problem_dict[title] = {'files': list(statuses)[0],
                                           'higlass_view_config': opf_status_dict[hg_dict[title]]}
            elif STATUS_LEVEL[result['status']] < STATUS_LEVEL[list(statuses)[0]]:
                problem_dict[title] = {result['@id']: result['status'], title: list(statuses)[0]}
            for f in file_list:
                if opf_linked_dict.get(f['uuid']):
                    for qm in opf_linked_dict[f['uuid']]:
                        if (STATUS_LEVEL[opf_other_dict[qm]] != STATUS_LEVEL[opf_status_dict[f['uuid']]]):
                            if title not in problem_dict:
                                problem_dict[title] = {}
                            if f['@id'] not in problem_dict[title]:
                                problem_dict[title][f['@id']] = {}
                            problem_dict[title][f['@id']]['quality_metric'] = {
                                'uuid': opf_linked_dict[f['uuid']], 'status': opf_other_dict[qm]
                            }
        if problem_dict:
            check.full_output[result['@id']] = problem_dict
    if check.full_output:
        check.brief_output = list(check.full_output.keys())
        check.status = 'WARN'
        check.summary = 'Other processed files with status mismatches found'
        check.description = ('{} Experiment Sets found with status mismatches in '
                             'other processed files'.format(len(check.brief_output)))
    else:
        check.status = "PASS"
        check.summary = 'All other processed files have matching statuses'
        check.description = 'No Experiment Sets found with status mismatches in other processed files'
    return check


@check_function()
def check_validation_errors(connection, **kwargs):
    '''
    Counts number of items in fourfront with schema validation errors,
    returns link to search if found.
    '''
    check = CheckResult(connection, 'check_validation_errors')

    search_url = 'search/?validation_errors.name!=No+value&type=Item'
    results = ff_utils.search_metadata(search_url + '&field=@id', key=connection.ff_keys)
    if results:
        types = {item for result in results for item in result['@type'] if item != 'Item'}
        check.status = 'WARN'
        check.summary = 'Validation errors found'
        check.description = ('{} items found with validation errors, comprising the following '
                             'item types: {}. \nFor search results see link below.'.format(
                                 len(results), ', '.join(list(types))))
        check.ff_link = connection.ff_server + search_url
    else:
        check.status = 'PASS'
        check.summary = 'No validation errors'
        check.description = 'No validation errors found.'
    return check


def _get_all_other_processed_files(item):
    toignore = []

    def _get_items_in_opf_collection(opf_collection):
        '''helper function to get all relevant items within an opf collection'''
        items_list = []
        for pf in opf_collection.get('files', []):
            items_list.append(pf['uuid'])
            items_list.extend([sc['content']['uuid'] for sc in pf.get('static_content', [])])
            if pf.get('quality_metric'):
                items_list.append(pf['quality_metric']['uuid'])
                items_list.extend([qc_item['value']['uuid'] for qc_item in pf['quality_metric'].get('qc_list', [])])
        hgv = opf_collection.get('higlass_view_config')
        if hgv:
            items_list.append(hgv['uuid'])
        return items_list

    # get directly linked other processed files
    for opf_expset in item.get('embedded').get('other_processed_files', []):
        toignore.extend(_get_items_in_opf_collection(opf_expset))

    # experiment sets can also have linked opfs from experiment
    for exp in item.get('embedded').get('experiments_in_set', []):
        for opf_exp in exp.get('other_processed_files', []):
            toignore.extend(_get_items_in_opf_collection(opf_exp))

    return toignore


@check_function()
def check_bio_feature_organism_name(connection, **kwargs):
    '''
    Attempts to identify an organism to add to the organism_name field in BioFeature items
    checks the linked genes or the genomic regions and then description
    '''
    check = CheckResult(connection, 'check_bio_feature_organism_name')
    check.action = "patch_bio_feature_organism_name"

    # create some mappings
    organism_search = 'search/?type=Organism'
    organisms = ff_utils.search_metadata(organism_search, key=connection.ff_keys)
    orgn2name = {o.get('@id'): o.get('name') for o in organisms}
    # add special cases
    orgn2name['unspecified'] = 'unspecified'
    orgn2name['multiple organisms'] = 'multiple organisms'
    genome2orgn = {o.get('genome_assembly'): o.get('@id') for o in organisms if 'genome_assembly' in o}
    gene_search = 'search/?type=Gene'
    genes = ff_utils.search_metadata(gene_search, key=connection.ff_keys)
    gene2org = {g.get('@id'): g.get('organism').get('@id') for g in genes}
    # get all BioFeatures
    biofeat_search = 'search/?type=BioFeature'
    biofeatures = ff_utils.search_metadata(biofeat_search, key=connection.ff_keys)

    matches = 0
    name_trumps_guess = 0
    mismatches = 0
    to_patch = {}
    brief_report = []
    to_report = {'name_trumps_guess': {}, 'lost_and_found': {}, 'orphans': {}, 'mismatches': {}}
    for biofeat in biofeatures:
        linked_orgn_name = None
        orgn_name = biofeat.get('organism_name')
        biogenes = biofeat.get('relevant_genes')
        if biogenes is not None:
            borgns = [gene2org.get(g.get('@id')) for g in biogenes if '@id' in g]
            linked_orgn_name = _get_orgname_from_atid_list(borgns, orgn2name)
        if not linked_orgn_name:  # didn't get it from genes - try genomic regions
            gen_regions = biofeat.get('genome_location')
            if gen_regions is not None:
                grorgns = []
                for genreg in gen_regions:
                    assembly_in_dt = False
                    gr_dt = genreg.get('display_title')
                    for ga, orgn in genome2orgn.items():
                        if ga in gr_dt:
                            grorgns.append(orgn)
                            assembly_in_dt = True
                            break
                    if not assembly_in_dt:
                        gr_res = ff_utils.get_es_metadata([genreg.get('uuid')],
                                                          key=connection.ff_keys, sources=['properties.genome_assembly'])
                        try:
                            gr_ass = gr_res[0].get('properties').get('genome_assembly')
                        except AttributeError:
                            gr_ass = None
                        if gr_ass is not None:
                            for ga, orgn in genome2orgn.items():
                                if ga == gr_ass:
                                    grorgns.append(orgn)
                linked_orgn_name = _get_orgname_from_atid_list(grorgns, orgn2name)
        if not linked_orgn_name:  # and finally try Description
            desc = biofeat.get('description')
            if desc is not None:
                for o in orgn2name.values():
                    if o in desc.lower():
                        linked_orgn_name = o
                        break

        # we've done our best now check and create output
        bfuuid = biofeat.get('uuid')
        bfname = biofeat.get('display_title')
        if not orgn_name:
            if linked_orgn_name:
                to_patch[bfuuid] = {'organism_name': linked_orgn_name}
                brief_report.append('{} MISSING ORGANISM - PATCH TO {}'.format(bfname, linked_orgn_name))
                to_report['lost_and_found'].update({bfuuid: linked_orgn_name})
            else:
                brief_report.append('{} MISSING ORGANISM - NO GUESS'.format(bfname))
                to_report['orphans'].update({bfuuid: None})
        else:
            if linked_orgn_name:
                if orgn_name != linked_orgn_name:
                    if linked_orgn_name == 'unspecified' or orgn_name == 'engineered reagent':
                        # unspecified here means an organism or multiple coule not be found from linked genes or other criteria
                        # for engineered reagent may find a linked name depending on what is linked to bio_feature
                        # usually want to keep the given 'engineered reagent' label but warrants occasional review
                        name_trumps_guess += 1
                        to_report['name_trumps_guess'].update({bfuuid: (orgn_name, linked_orgn_name)})
                    elif orgn_name == 'unspecified':  # patch if a specific name is found
                        to_patch[bfuuid] = {'organism_name': linked_orgn_name}
                        to_report['mismatches'].update({bfuuid: (orgn_name, linked_orgn_name)})
                        brief_report.append('{}: CURRENT {} GUESS {} - WILL PATCH!'.format(bfname, orgn_name, linked_orgn_name))
                    else:
                        mismatches += 1
                        to_report['mismatches'].update({bfuuid: (orgn_name, linked_orgn_name)})
                        brief_report.append('{}: CURRENT {} GUESS {}'.format(bfname, orgn_name, linked_orgn_name))
                else:
                    matches += 1
            else:
                to_report['name_trumps_guess'].update({bfuuid: (orgn_name, None)})
                name_trumps_guess += 1
    brief_report.sort()
    cnt_rep = [
        'MATCHES: {}'.format(matches),
        'MISMATCHES TO CHECK: {}'.format(mismatches),
        'OK MISMATCHES: {}'.format(name_trumps_guess)
    ]

    check.brief_output = cnt_rep + brief_report
    check.full_output = {}
    if brief_report:
        check.summary = 'Found BioFeatures with organism_name that needs attention'
        check.status = 'WARN'
        check.allow_action = True
    else:
        check.status = 'PASS'
        check.summary = 'BioFeature organism_name looks good'
    if to_report:
        to_report.update({'counts': cnt_rep})
        check.full_output.update({'info': to_report})
    if to_patch:
        check.full_output.update({'to_patch': to_patch})
    return check


def _get_orgname_from_atid_list(atids, orgn2name):
    org_atid = [x for x in list(set(atids)) if x is not None]
    if not org_atid:
        org_atid = 'unspecified'
    elif len(org_atid) == 1:
        org_atid = org_atid[0]
    else:
        org_atid = 'multiple organisms'
    return orgn2name.get(org_atid)


@action_function()
def patch_bio_feature_organism_name(connection, **kwargs):
    action = ActionResult(connection, 'patch_bio_feature_organism_name')
    action_logs = {'patch_failure': [], 'patch_success': []}
    check_res = action.get_associated_check_result(kwargs)
    output = check_res.get('full_output')
    patches = output.get('to_patch')
    if patches:
        for uid, val in patches.items():
            try:
                res = ff_utils.patch_metadata(val, uid, key=connection.ff_keys)
            except:
                action_logs['patch_failure'].append(uid)
            else:
                if res.get('status') == 'success':
                    action_logs['patch_success'].append(uid)
                else:
                    action_logs['patch_failure'].append(uid)
        action.status = 'DONE'
        action.output = action_logs
        return action


@check_function()
def check_fastq_read_id(connection, **kwargs):
    '''
        Reports if there are uploaded fastq files with integer read ids
    '''
    check = CheckResult(connection, 'check_fastq_read_id')
    check.description = 'Reports fastq files that have integer read ids uploaded after 2020-04-13'
    check.summary = 'No fastq files with integer ids'
    check.full_output = {}
    check.status = 'PASS'
    query = '/search/?date_created.from=2020-04-13&file_format.file_format=fastq&status=uploaded&type=FileFastq'
    res = ff_utils.search_metadata(query, key=connection.ff_keys)
    if not res:
        return check
    target_files = {}
    for a_re in res:
        if a_re.get('file_first_line'):
            read_id = a_re['file_first_line'].split(' ')[0][1:]
            if read_id.isnumeric():
                if a_re.get('experiments'):
                    exp = a_re['experiments'][0]['@id']
                    exp_title = a_re['experiments'][0]['display_title']
                else:
                    exp = 'No experiment associated'
                    exp_title = ''

                if exp not in target_files:
                    target_files[exp] = {'title': exp_title, 'files': []}
                target_files[exp]['files'].append(a_re['accession'])

    if target_files:
        check.status = 'WARN'
        check.summary = '%s fastq files have integer read ids' % (sum([len(v['files']) for i, v in target_files.items()]))
        check.full_output = target_files

    return check


@check_function()
def released_protected_data_files(connection, **kwargs):
    '''
    Check if fastq or bam files from IndividualHuman with protected_data=True
    have a visible status
    '''
    check = CheckResult(connection, 'released_protected_data_files')
    visible_statuses = ['released to project', 'released', 'archived to project', 'archived', 'replaced']
    formats = ['fastq', 'bam']
    query = 'search/?type=File'
    query += ''.join(['&file_format.file_format=' + f for f in formats])
    query += '&experiments.biosample.biosource.individual.protected_data=true'
    query += ''.join(['&status=' + s for s in visible_statuses])
    query += '&field=file_format&field=status&field=open_data_url'
    res = ff_utils.search_metadata(query, key=connection.ff_keys)
    files = {'visible': []}
    files_with_open_data_url = False
    for a_file in res:
        file_report = {
            '@id': a_file['@id'],
            'file_format': a_file['file_format']['file_format'],
            'file_status': a_file['status']}
        if a_file.get('open_data_url'):
            files_with_open_data_url = True
            file_report['open_data_url'] = a_file['open_data_url']
        files['visible'].append(file_report)
    if files['visible']:
        check.status = 'WARN'
        check.summary = 'Found visible sequence files that should be restricted'
        check.description = '%s fastq or bam files from restricted individuals found with status: %s' % (len(files['visible']), str(visible_statuses).strip('[]'))
        check.action_message = 'Will attempt to patch %s files to status=restricted' % len(files['visible'])
        check.allow_action = True
        if files_with_open_data_url:
            check.description += '\nNOTE: some files are in AWS Open Data bucket and should be moved manually'
    else:
        check.status = 'PASS'
        check.summary = 'No unrestricted fastq or bam files found from individuals with protected_data'
        check.description = 'No fastq or bam files from restricted individuals found with status: %s' % str(visible_statuses).strip('[]')
    check.brief_output = {'visible': '%s files' % len(files['visible'])}
    check.full_output = files
    check.action = 'restrict_files'
    return check


@check_function()
def released_output_from_restricted_input(connection, **kwargs):
    '''
    Check if fastq or bam files produced by workflows with restricted input
    files (typically because deriving from HeLa cells) have a visible status.
    In addition, check if any fastq or bam processed file (with visible
    status) is not output of a workflow ('unlinked'). If this happens, the
    check cannot ensure that all processed files are analyzed.
    '''
    check = CheckResult(connection, 'released_output_from_restricted_input')
    visible_statuses = ['released to project', 'released', 'archived to project', 'archived', 'replaced']
    formats = ['fastq', 'bam']
    query_wfr = 'search/?type=WorkflowRun'
    query_wfr += '&input_files.value.status=restricted'
    query_wfr += ''.join(['&output_files.value.status=' + s for s in visible_statuses])
    query_wfr += ''.join(['&output_files.value.file_format.display_title=' + f for f in formats])
    query_wfr += '&field=output_files'
    res_wfr = ff_utils.search_metadata(query_wfr, key=connection.ff_keys)
    # this returns wfrs that have AT LEAST one output file with these values
    files = {'visible': [], 'unlinked': []}
    files_with_open_data_url = False
    for a_wfr in res_wfr:
        for a_file in a_wfr.get('output_files', []):
            if a_file.get('value'):
                format = a_file['value']['file_format']['display_title']
                status = a_file['value']['status']
                if format in formats and status in visible_statuses:
                    file_report = {
                        '@id': a_file['value']['@id'],
                        'file_format': format,
                        'file_status': status}
                    if a_file['value'].get('open_data_url'):
                        files_with_open_data_url = True
                        file_report['open_data_url'] = a_file['value']['open_data_url']
                    files['visible'].append(file_report)
    # search for visible fastq or bam processed files that are not output of any workflow
    query_pf = 'search/?type=FileProcessed&workflow_run_outputs.workflow.title=No+value'
    query_pf += ''.join(['&status=' + st for st in visible_statuses])
    query_pf += ''.join(['&file_format.file_format=' + f for f in formats])
    query_pf += '&field=file_format&field=status&field=open_data_url'
    res_pf = ff_utils.search_metadata(query_pf, key=connection.ff_keys)
    for a_file in res_pf:
        file_report = {
            '@id': a_file['@id'],
            'file_format': a_file['file_format']['display_title'],
            'file_status': a_file['status']}
        if a_file.get('open_data_url'):
            files_with_open_data_url = True
            file_report['open_data_url'] = a_file['open_data_url']
        files['unlinked'].append(file_report)

    if files['visible'] or files['unlinked']:
        check.status = 'WARN'
        check.summary = "Problematic processed files found"
        check.description = "Found %s problematic FASTQ or BAM processed files: 'visible' files should be restricted" % (len(files['visible']) + len(files['unlinked']))
        if files['unlinked']:
            check.description += "; 'unlinked' files are not output of any workflow, therefore cannot be analyzed (consider linking)"
        if files['visible']:
            check.allow_action = True
            check.action_message = "Will attempt to patch %s 'visible' files to status=restricted" % len(files['visible'])
        if files_with_open_data_url:
            check.description += '\nNOTE: some files are in AWS Open Data bucket and should be moved manually'
    else:
        check.status = 'PASS'
        check.summary = "No problematic processed files found"
        check.description = "No visible output files found from wfr with restricted files as input"
    check.full_output = files
    check.brief_output = {'visible': '%s files' % len(files['visible']),
                          'unlinked': '%s files' % len(files['unlinked'])}
    check.action = 'restrict_files'
    return check


@action_function()
def restrict_files(connection, **kwargs):
    '''
    Patch the status of visible sequence files to "restricted"
    '''
    action = ActionResult(connection, 'restrict_files')
    check_res = action.get_associated_check_result(kwargs)
    files_to_patch = check_res['full_output']['visible']
    action_logs = {'patch_success': [], 'patch_failure': []}
    patch = {'status': 'restricted'}
    for a_file in files_to_patch:
        try:
            ff_utils.patch_metadata(patch, a_file['uuid'], key=connection.ff_keys)
        except Exception as e:
            action_logs['patch_failure'].append({a_file['uuid']: str(e)})
        else:
            action_logs['patch_success'].append(a_file['uuid'])
    if action_logs['patch_failure']:
        action.status = 'FAIL'
    else:
        action.status = 'DONE'
    action.output = action_logs
    return action


@check_function(days_delay=14)
def external_submission_but_missing_dbxrefs(connection, **kwargs):
    ''' Check if items with external_submission also have dbxrefs.
    When exporting metadata for submission to an external repository,
    external_submission is patched. After some time (delay), the corresponding
    dbxref should also have been received and patched.
    '''
    check = CheckResult(connection, 'external_submission_but_missing_dbxrefs')
    delay = kwargs.get('days_delay')
    try:
        delay = int(delay)
    except (ValueError, TypeError):
        delay = 14
    date_now = datetime.datetime.now(datetime.timezone.utc)
    days_diff = datetime.timedelta(days=delay)
    to_date = datetime.datetime.strftime(date_now - days_diff, "%Y-%m-%d %H:%M")

    query = ('search/?type=ExperimentSet&type=Experiment&type=Biosample&type=FileFastq' +
             '&dbxrefs=No+value&external_submission.date_exported.to=' + to_date)
    items = ff_utils.search_metadata(query + '&field=dbxrefs&field=external_submission', key=connection.ff_keys)
    grouped_results = {}
    if items:
        for i in items:
            grouped_results.setdefault(i['@type'][0], []).append(i['@id'])
        check.brief_output = {i_type: len(ids) for i_type, ids in grouped_results.items()}
        check.full_output = grouped_results
        check.status = 'WARN'
        check.summary = 'Items missing dbxrefs found'
        check.description = '{} items exported for external submission more than {} days ago but still without dbxrefs'.format(len(items), delay)

    else:
        check.status = 'PASS'
        check.summary = 'No items missing dbxrefs found'
        check.description = 'All items exported for external submission more than {} days ago have dbxrefs'.format(delay)

    return check
