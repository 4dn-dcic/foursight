from dcicutils import ff_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


# generic CHECK function used to add a static headers to items of some search result
def find_items_for_header_processing(connection, check, header, add_search=None,
                                     remove_search=None, append=True):
    """
    (add_search) and remove them from others (remove_search).
    Args are:
    - connection (FS connection)
    - check (required; check object initialized by CheckResult)
    - headers @id (required)
    - add_search search query
    - remove_search search query
    Meant to be used for CHECKS
    """
    # sets the full_output of the check!
    check.full_output = {'static_section': header, 'to_add': {}, 'to_remove': {}}
    # this GET will fail if the static header does not exist
    header_res = ff_utils.get_metadata(header, key=connection.ff_keys)
    # add entries keyed by item uuid with value of the static headers
    if add_search:
        search_res_add = ff_utils.search_metadata(add_search, key=connection.ff_keys)
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
                                                     key=connection.ff_keys)
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
def patch_items_with_headers(connection, action, kwargs):
    """
    Arguments are:
    - the connection (FS connection)
    - the action (from ActionResult)
    - kwargs (from the action function)
    Takes care of patching info on Fourfront and also populating fields on the
    action
    """
    action_logs = {'patch_failure': [], 'patch_success': []}
    # get latest results from prepare_static_headers
    headers_check_result = action.get_associated_check_result(kwargs)
    # the dictionaries can be combined
    total_patches = headers_check_result['full_output']['to_add']
    total_patches.update(headers_check_result['full_output']['to_remove'])
    for item, headers in total_patches.items():
        # if all headers are deleted, use ff_utils.delete_field
        if headers == []:
            try:
                ff_utils.delete_field(item, 'static_headers', key=connection.ff_keys)
            except Exception as e:
                patch_error = '\n'.join([item, str(e)])
                action_logs['patch_failure'].append(patch_error)
            else:
                action_logs['patch_success'].append(item)
        else:
            patch_data = {'static_headers': headers}
            try:
                ff_utils.patch_metadata(patch_data, obj_id=item, key=connection.ff_keys)
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
    header_at_id='',
    append=True,
    action="patch_static_headers"
)
def prepare_static_headers(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers')
    check.action = 'patch_static_headers'
    find_items_for_header_processing(connection, check, kwargs['header_at_id'],
                                     kwargs['add_search'], kwargs['remove_search'], append=kwargs['append'])
    return check


@action_function()
def patch_static_headers(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# Data Use Guidelines
@check_function(action="patch_static_headers_data_use_guidelines")
def prepare_static_headers_data_use_guidelines(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_data_use_guidelines')
    # only check experiment sets that are released, released to project, or in pre-release
    add_search = ('/search/?type=ExperimentSet&award.project=4DN&produced_in_pub.display_title=No%20value'
                  '&status=released&status=released+to+project&status=pre-release&frame=object')
    remove_search = ('/search/?type=ExperimentSet&award.project=4DN&produced_in_pub.display_title!=No%20value'
                     '&status=released&status=released+to+project&status=pre-release&frame=object')
    header_at_id = '/static-sections/621e8359-3885-40ce-965d-91894aa7b758/'
    check.action = 'patch_static_headers_data_use_guidelines'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search, append=False)
    return check


@action_function()
def patch_static_headers_data_use_guidelines(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_data_use_guidelines')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# InSitu Hi-C experiment Sets
@check_function(action="patch_static_headers_inSitu_HiC")
def prepare_static_headers_inSitu_HiC(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_inSitu_HiC')
    add_search = '/search/?experiments_in_set.experiment_type=in+situ+Hi-C&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=in+situ+Hi-C&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/298554ad-20e2-4449-a752-ac190123dab7/'
    check.action = 'patch_static_headers_inSitu_HiC'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_inSitu_HiC(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_inSitu_HiC')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# Dilution Hi-C Experiment Sets
@check_function(action="patch_static_headers_dilution_HiC")
def prepare_static_headers_dilution_HiC(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_dilution_HiC')
    add_search = '/search/?experiments_in_set.experiment_type=Dilution+Hi-C&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=Dilution+Hi-C&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/7627f4eb-9f2d-4171-9e9b-87ab800ab5cd/'
    check.action = 'patch_static_headers_dilution_HiC'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_dilution_HiC(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_dilution_HiC')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# FISH
@check_function(action="patch_static_headers_FISH")
def prepare_static_headers_FISH(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_FISH')
    add_search = '/search/?experiments_in_set.experiment_type=DNA+FISH&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=DNA+FISH&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/911424f9-21c7-49fc-b1df-865dd64ae91e/'
    check.action = 'patch_static_headers_FISH'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_FISH(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_FISH')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# SPT
@check_function(action="patch_static_headers_SPT")
def prepare_static_headers_SPT(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_SPT')
    add_search = '/search/?experiments_in_set.experiment_type=SPT&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=SPT&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/6a313162-e70c-4fbe-93c5-bc78f5faf0c7/'
    check.action = 'patch_static_headers_SPT'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_SPT(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_SPT')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# SPRITE
@check_function(action="patch_static_headers_SPRITE")
def prepare_static_headers_SPRITE(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_SPRITE')
    add_search = '/search/?experiments_in_set.experiment_type=DNA+SPRITE&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=DNA+SPRITE&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/205f35ec-92cd-4c02-bd35-b0d38dd72a90/'
    check.action = 'patch_static_headers_SPRITE'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_SPRITE(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_SPRITE')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# MARGI
@check_function(action="patch_static_headers_MARGI")
def prepare_static_headers_MARGI(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_MARGI')
    add_search = '/search/?experiments_in_set.experiment_type=MARGI&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=MARGI&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/0c2ba23e-b256-47ce-a37c-0f1282471789/'
    check.action = 'patch_static_headers_MARGI'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_MARGI(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_MARGI')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# sci-HiC
@check_function(action="patch_static_headers_sciHiC")
def prepare_static_headers_sciHiC(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_sciHiC')
    add_search = '/search/?experiments_in_set.experiment_type=sci-Hi-C&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=sci-Hi-C&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/ae5a6470-0694-4ba3-893a-40b170401bc0/'
    check.action = 'patch_static_headers_sciHiC'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_sciHiC(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_sciHiC')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action


# DNase Hi-C
@check_function(action="patch_static_headers_DNase_HiC")
def prepare_static_headers_DNase_HiC(connection, **kwargs):
    check = CheckResult(connection, 'prepare_static_headers_DNase_HiC')
    add_search = '/search/?experiments_in_set.experiment_type=DNase+Hi-C&type=ExperimentSet&frame=object'
    remove_search = '/search/?experiments_in_set.experiment_type!=DNase+Hi-C&type=ExperimentSet&frame=object'
    header_at_id = '/static-sections/84448fd6-ccf0-45a7-86c8-673b5686c059/'
    check.action = 'patch_static_headers_DNase_HiC'
    find_items_for_header_processing(connection, check, header_at_id,
                                     add_search, remove_search)
    return check


@action_function()
def patch_static_headers_DNase_HiC(connection, **kwargs):
    action = ActionResult(connection, 'patch_static_headers_DNase_HiC')
    # get latest results from prepare_static_headers
    patch_items_with_headers(connection, action, kwargs)
    return action
