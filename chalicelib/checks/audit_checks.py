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

    ext = ff_utils.search_metadata('search/?award.project=External&type=ExperimentSet&frame=object',
                                   ff_env=connection.ff_env, page_limit=50)
    no_pub = []
    for expset in ext:
        if not expset.get('publications_of_set') and not expset.get('produced_in_pub'):
            # add to Output
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
def repsets_have_bio_reps(connection, **kwargs):
    check = init_check_res(connection, 'repsets_have_bio_reps')

    results = ff_utils.search_metadata('search/?type=ExperimentSetReplicate&frame=object',
                                       ff_env=connection.ff_env, page_limit=50)

    audits = {'single_experiment': [], 'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []}
    by_exp = {}
    for result in results: # maybe also create dict by experiment
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
                exp_audits.append('single experiment')
            # check for technical replicates only
            elif len(rep_dict.keys()) == 1:
                audits['single_biorep'].append('{} contains only a single biological replicate'.format(result['@id']))
                exp_audits.append('single bio replicate')
            # check if bio rep numbers not in sequence
            elif sorted(list(rep_dict.keys())) != list(range(min(rep_dict.keys()), max(rep_dict.keys()) + 1)):
                audits['biorep_nums'].append('Biological replicate numbers of {} are not in sequence:'
                                             ' {}'.format(result['@id'], str(sorted(list(rep_dict.keys())))))
                exp_audits.append('bio replicate numbers not in sequence')
        # check if tech rep numbers not in sequence
            else:
                for key, val in rep_dict.items():
                    if sorted(val) != list(range(min(val), max(val) + 1)):
                        audits['techrep_nums'].append('Technical replicates of Bio Rep {} in {} are not in '
                                                      'sequence {}'.format(key, result['@id'], str(sorted(val))))
                        exp_audits.append('tech replicates of bio replicate {}'
                                          ' not in sequence'.format(key))
        if exp_audits:
            by_exp[result['@id']] = exp_audits

    if by_exp:
        check.status = 'WARN'
        check.summary = 'Replicate experiment sets found with replicate number issues'
        check.description = '{} replicate experiment sets found with replicate number issues'.format(len(by_exp.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'No replicate experiment sets found with replicate number issues'
        check.description = '0 replicate experiment sets found with replicate number issues'
    check.full_output = audits
    check.brief_output = by_exp
    return check


@check_function()
def tier1_metadata_present(connection, **kwargs):
    check = init_check_res(connection, 'tier1_metadata_present')

    results = ff_utils.search_metadata('search/?biosource.cell_line_tier=Tier+1&type=Biosample',
                                       ff_env=connection.ff_env)
    missing = {'cell_culture_details': [], 'culture_start_date': [],
               'culture_duration': [], 'morphology_image': []}
    for result in results:
        if len(result.get('biosource')) != 1:
            continue
        elif not result.get('cell_culture_details'):
            missing['cell_culture_details'].append(result['@id'])
        else:
            for item in ['culture_start_date', 'culture_duration', 'morphology_image']:
                if not result['cell_culture_details'].get(item):
                    missing[item].append(result['@id'])
    flagged = list(set([bs for val in missing.values() for bs in val]))
    if flagged:
        check.status = 'WARN'
        check.summary = 'Tier 1 biosamples found missing required metadata'
        check.description = '{} tier 1 biosamples found missing required metadata'.format(len(flagged))
    else:
        check.status = 'PASS'
        check.summary = 'All Tier 1 biosamples have required metadata'
        check.description = '0 tier 1 biosamples found missing required metadata'
    check.full_output = missing
    check.brief_output = flagged
    return check


@check_function()
def exp_has_raw_files(connection, **kwargs):
    check = init_check_res(connection, 'exp_has_raw_files')

    no_files = ff_utils.search_metadata('search/?type=Experiment&%40type%21=ExperimentMic&files.uuid=No+value',
                                        ff_env=connection.ff_env)
    bad_status = ff_utils.search_metadata('search/?status=uploading&status=archived&status=deleted&type=FileFastq&experiments.uuid%21=No+value',
                                          ff_env=connection.ff_env)
    bad_status_ids = [item['@id'] for item in bad_status]
    exps = list(set([exp['@id'] for fastq in bad_status for exp in fastq.get('experiments') if fastq.get('experiments')]))
    missing_files = []
    for expt in exps:
        result = ff_utils.get_metadata(expt, ff_env=connection.ff_env)
        raw_files = False
        if result.get('files'):
            for fastq in result.get('files'):
                if fastq['@id'] not in bad_status_ids:
                    raw_files = True
                    break
        if not raw_files:
            missing_files.append(expt)
    if missing_files:
        check.status = 'WARN'
        check.summary = 'Experiments missing raw files found'
        check.description = '{} sequencing experiments are missing raw files'.format(len(missing_files))
    else:
        check.status = 'PASS'
        check.summary = 'No experiments missing raw files'
        check.description = '0 sequencing experiments are missing raw files'
    check.full_output = missing_files
    return check


@check_function()
def paired_end_info_consistent(connection, **kwargs):
    check = init_check_res(connection, 'paired_end_info_consistent')

    search1 = 'search/?type=FileFastq&related_files.relationship_type=paired+with&paired_end=No+value'
    search2 = 'search/?type=FileFastq&related_files.relationship_type!=paired+with&paired_end%21=No+value'

    results1 = ff_utils.search_metadata(search1 + '&frame=object', ff_env=connection.ff_env)
    results2 = ff_utils.search_metadata(search2 + '&frame=object', ff_env=connection.ff_env)
    # for result1 in results1:

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
