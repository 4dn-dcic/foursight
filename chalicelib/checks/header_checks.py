from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils

# generic CHECK function used to add a static headers to items of some search result
def find_items_for_header_processing(connection, check, header, add_search=None,
                                     remove_search=None, append=True):
    """
    (add_search) and remove them from others (remove_search).
    Args are:
    - connection (FS connection)
    - check (required; check object initialized by init_check_res)
    - headers @id (required)
    - add_search search query
    - remove_search search query
    Meant to be used for CHECKS
    """
    # sets the full_output of the check!
    check.full_output = {'static_section': header, 'to_add': {}, 'to_remove': {}}
    # this GET will fail if the static header does not exist
    header_res = ff_utils.get_metadata(header, key=connection.ff_keys, ff_env=connection.ff_env)
    # add entries keyed by item uuid with value of the static headers
    if add_search:
        search_res_add = ff_utils.search_metadata(add_search,
                                                  key=connection.ff_keys,
                                                  ff_env=connection.ff_env)
        for search_res in search_res_add:
            curr_headers = search_res.get('static_headers', [])
            # handle case where frame != object
            if curr_headers and isinstance(curr_headers[0], dict):
                curr_headers = [obj['@id'] for obj in curr_headers]
            if header not in curr_headers:
                curr_headers = curr_headers + [header] if append else [header] + curr_headers
                check.full_output['to_add'][search_res['@id']] = curr_headers

    if remove_search:
        search_res_remove = ff_utils.search_metadata(remove_search,
                                                     key=connection.ff_keys,
                                                     ff_env=connection.ff_env)
        for search_res in search_res_remove:
            curr_headers = search_res.get('static_headers', [])
            # handle case where frame != object
            if curr_headers and isinstance(curr_headers[0], dict):
                curr_headers = [obj['@id'] for obj in curr_headers]
            if header in curr_headers:
                curr_headers.remove(header)
                check.full_output['to_remove'][search_res['@id']] = curr_headers

    if check.full_output['to_add'] or check.full_output['to_remove']:
        check.status = 'WARN'
        check.summary = 'Ready to add and/or remove static header'
        check.description = 'Ready to add and/or remove static header: %s' % header
        check.allow_action = True
        check.action_message = 'Will add static header to %s items and remove it from %s items' % (len(check.full_output['to_add']), len(check.full_output['to_remove']))
    else:
        check.status = 'PASS'
        check.summary = 'Static header is all set'


# generic ACTION function used along to add/remove static headers from the
# information obtained from find_items_for_header_processing
def patch_items_with_headers(connection, action, headers_check, called_by):
    """
    Arguments are:
    - the connection (FS connection)
    - the action (from init_action_res)
    - the check from the associated header check (from init_check_res)
    - the check uuid used to call the action (equal to kwargs['called_by']).
    Takes care of patching info on Fourfront and also populating fields on the
    action
    """
    action_logs = {'patch_failure': [], 'patch_success': []}
    # get latest results from prepare_static_headers
    headers_check_result = headers_check.get_result_by_uuid(called_by)
    # the dictionaries can be combined
    total_patches = headers_check_result['full_output']['to_add']
    total_patches.update(headers_check_result['full_output']['to_remove'])
    for item, headers in total_patches.items():
        # if all headers are deleted, use ff_utils.delete_field
        if headers == []:
            try:
                ff_utils.delete_field(item, 'static_headers', key=connection.ff_keys, ff_env=connection.ff_env)
            except Exception as e:
                patch_error = '\n'.join([item, str(e)])
                action_logs['patch_failure'].append(patch_error)
            else:
                action_logs['patch_success'].append(item)
        else:
            patch_data = {'static_headers': headers}
            try:
                ff_utils.patch_metadata(patch_data, obj_id=item, key=connection.ff_keys, ff_env=connection.ff_env)
            except Exception as e:
                patch_error = '\n'.join([item, str(e)])
                action_logs['patch_failure'].append(patch_error)
            else:
                action_logs['patch_success'].append(item)
    action.status = 'DONE'
    action.output = action_logs


@check_function(
    add_search='',
    remove_search='',
    header_at_id=''
)
def prepare_static_headers(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers')
    check.action = 'patch_static_headers'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# Data Use Guidelines
@check_function(
    add_search='/search/?type=ExperimentSet&award.project=4DN&publications_of_set.display_title=No%20value&frame=object',
    remove_search='/search/?type=ExperimentSet&award.project=4DN&publications_of_set.display_title!=No%20value&frame=object',
    header_at_id='/static-sections/621e8359-3885-40ce-965d-91894aa7b758/'
)
def prepare_static_headers_data_use_guidelines(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_data_use_guidelines')
    check.action = 'patch_static_headers_data_use_guidelines'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'], append=False)
    return check


@action_function()
def patch_static_headers_data_use_guidelines(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_data_use_guidelines')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_data_use_guidelines')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# InSitu Hi-C experiment Sets
@check_function(
    add_search='/search/?experiments_in_set.experiment_type=in+situ+Hi-C&type=ExperimentSet&frame=object',
    remove_search='/search/?experiments_in_set.experiment_type!=in+situ+Hi-C&type=ExperimentSet&frame=object',
    header_at_id='/static-sections/298554ad-20e2-4449-a752-ac190123dab7/'
)
def prepare_static_headers_inSitu_HiC(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_inSitu_HiC')
    check.action = 'patch_static_headers_inSitu_HiC'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers_inSitu_HiC(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_inSitu_HiC')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_inSitu_HiC')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# Dilution Hi-C Experiment Sets
@check_function(
    add_search='/search/?experiments_in_set.experiment_type=dilution+Hi-C&type=ExperimentSet&frame=object',
    remove_search='/search/?experiments_in_set.experiment_type!=dilution+Hi-C&type=ExperimentSet&frame=object',
    header_at_id='/static-sections/7627f4eb-9f2d-4171-9e9b-87ab800ab5cd/'
)
def prepare_static_headers_dilution_HiC(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_dilution_HiC')
    check.action = 'patch_static_headers_dilution_HiC'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers_dilution_HiC(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_dilution_HiC')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_dilution_HiC')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# FISH
@check_function(
    add_search='/search/?experiments_in_set.experiment_type=DNA+FISH&type=ExperimentSet&frame=object',
    remove_search='/search/?experiments_in_set.experiment_type!=DNA+FISH&type=ExperimentSet&frame=object',
    header_at_id='/static-sections/911424f9-21c7-49fc-b1df-865dd64ae91e/'
)
def prepare_static_headers_FISH(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_FISH')
    check.action = 'patch_static_headers_FISH'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers_FISH(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_FISH')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_FISH')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# SPT
@check_function(
    add_search='/search/?experiments_in_set.experiment_type=SPT&type=ExperimentSet&frame=object',
    remove_search='/search/?experiments_in_set.experiment_type!=SPT&type=ExperimentSet&frame=object',
    header_at_id='/static-sections/6a313162-e70c-4fbe-93c5-bc78f5faf0c7/'
)
def prepare_static_headers_SPT(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_SPT')
    check.action = 'patch_static_headers_SPT'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers_SPT(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_SPT')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_SPT')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action


# SPRITE
@check_function(
    add_search='/search/?experiments_in_set.experiment_type=DNA+SPRITE&type=ExperimentSet&frame=object',
    remove_search='/search/?experiments_in_set.experiment_type!=DNA+SPRITE&type=ExperimentSet&frame=object',
    header_at_id='/static-sections/205f35ec-92cd-4c02-bd35-b0d38dd72a90/'
)
def prepare_static_headers_SPRITE(connection, **kwargs):
    check = init_check_res(connection, 'prepare_static_headers_SPRITE')
    check.action = 'patch_static_headers_SPRITE'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'])
    return check


@action_function()
def patch_static_headers_SPRITE(connection, **kwargs):
    action = init_action_res(connection, 'patch_static_headers_SPRITE')
    # get latest results from prepare_static_headers
    headers_check = init_check_res(connection, 'prepare_static_headers_SPRITE')
    patch_items_with_headers(connection, action, headers_check, kwargs['called_by'])
    return action
