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

# generic CHECK function used to add a static headers to items of some search result
def find_items_for_header_processing(connection, check, header, add_search=None, remove_search=None):
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
                curr_headers.append(header)
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
    add_search='/search/?type=ExperimentSetReplicate&award.project=4DN&publications_of_set.display_title=No%20value&frame=object',
    remove_search='/search/?type=ExperimentSetReplicate&award.project=4DN&publications_of_set.display_title!=No%20value&frame=object',
    header_at_id='/static-sections/621e8359-3885-40ce-965d-91894aa7b758/'
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
