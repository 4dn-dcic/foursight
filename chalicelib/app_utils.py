from __future__ import print_function, unicode_literals
from chalice import Response
from jinja2 import Environment, FileSystemLoader, select_autoescape
import json
import os
import jwt
import boto3
import datetime
import ast
import copy
from itertools import chain
from dateutil import tz
from base64 import b64decode
from dcicutils import ff_utils
from .fs_connection import FSConnection
from .run_result import CheckResult, ActionResult
from .check_utils import (
    get_grouped_check_results,
    get_check_strings,
    get_action_strings,
    get_check_title_from_setup,
    get_schedule_names,
    get_check_schedule,
    run_check_or_action,
    init_check_or_action_res
)
from .utils import (
    get_stage_info,
    basestring,
    list_environments,
    recover_message_and_propogate,
    delete_message_and_propogate,
    invoke_check_runner,
    get_sqs_queue,
    collect_run_info,
    send_sqs_messages,
    get_sqs_attributes
)
from .s3_connection import S3Connection

jin_env = Environment(
    loader=FileSystemLoader('chalicelib/templates'),
    autoescape=select_autoescape(['html', 'xml'])
)


def init_environments(env='all'):
    """
    Generate environment information from the foursight-envs bucket in s3.
    Returns a dictionary keyed by environment name with value of a sub-dict
    with the fields needed to initiate a connection.
    """
    stage = get_stage_info()['stage']
    s3_connection = S3Connection('foursight-envs')
    env_keys = s3_connection.list_all_keys()
    environments = {}
    if env != 'all':
        if env in env_keys:
            env_keys = [env]
        else:
            return {} # provided env is not in s3
    for env_key in env_keys:
        env_res = json.loads(s3_connection.get_object(env_key))
        # check that the keys we need are in the object
        if isinstance(env_res, dict) and {'fourfront', 'es'} <= set(env_res):
            env_entry = {
                'fourfront': env_res['fourfront'],
                'es': env_res['es'],
                'ff_env': env_res.get('ff_env', ''.join(['fourfront-', env_key])),
                'bucket': ''.join(['foursight-', stage, '-', env_key])
            }
            environments[env_key] = env_entry
    return environments


def init_connection(environ):
    """
    Initialize the fourfront/s3 connection using the FSConnection object
    and the given environment.
    Returns an FSConnection object or raises an error.
    """
    error_res = {}
    environments = init_environments()
    # if still not there, return an error
    if environ not in environments:
        error_res = {
            'status': 'error',
            'description': 'invalid environment provided. Should be one of: %s' % (str(list(environments.keys()))),
            'environment': environ,
            'checks': {}
        }
        raise Exception(str(error_res))
    connection = FSConnection(environ, environments[environ])
    return connection


def init_response(environ):
    """
    Generalized function to init response given an environment
    """
    response = Response('Foursight response')
    try:
        connection = init_connection(environ)
    except Exception as e:
        connection = None
        response.body = str(e)
        response.status_code = 400
    return connection, response


def check_authorization(request_dict, env=None):
    """
    Manual authorization, since the builtin chalice @app.authorizer() was not
    working for me and was limited by a requirement that the authorization
    be in a token. Check the cookies of the request for jwtToken using utils

    Take in a dictionary format of the request (app.current_request) so we
    can test this.
    """
    # first check the Authorization header
    dev_auth = request_dict.get('headers', {}).get('authorization')
    # grant admin if dev_auth equals secret value
    if dev_auth and dev_auth == os.environ.get('DEV_SECRET'):
        return True
    token = get_jwt(request_dict)
    auth0_client = os.environ.get('CLIENT_ID', None)
    auth0_secret = os.environ.get('CLIENT_SECRET', None)
    if auth0_client and auth0_secret and token:
        try:
            if env is None:
                return False  # we have no env to check auth
            # leeway accounts for clock drift between us and auth0
            payload = jwt.decode(token, b64decode(auth0_secret, '-_'), audience=auth0_client, leeway=30)
            env_info = init_environments(env)
            user_res = ff_utils.get_metadata('users/' + payload.get('email').lower(),
                                            ff_env=env_info[env]['ff_env'], add_on='frame=object')
            if 'admin' in user_res['groups'] and payload.get('email_verified'):
                # fully authorized
                return True
        except:
            pass
    return False


def get_jwt(request_dict):
    """
    Simple function to extract a jwt from a request that has already been
    dict-transformed
    """
    cookies = request_dict.get('headers', {}).get('cookie')
    cookie_dict = {}
    if cookies:
        for cookie in cookies.split(';'):
            cookie_split = cookie.strip().split('=')
            if len(cookie_split) == 2:
                cookie_dict[cookie_split[0]] = cookie_split[1]
    token = cookie_dict.get('jwtToken', None)
    return token


def get_domain_and_context(request_dict):
    """
    Given a request that has already been dict-transformed, get the host
    and the url context for endpoints. Context will basically either be
    '/api/' or '/'
    """
    domain = request_dict.get('headers', {}).get('host')
    context = '/api/' if request_dict.get('context', {}).get('path', '').startswith('/api/') else '/'
    return domain, context


def forbidden_response(context="/"):
    return Response(status_code=403,
                    body='Forbidden. Login on the %s page.' % (context + 'view/<environment>'))


def process_response(response):
    """
    Does any final processing of a Foursight response before returning it. Right now, this includes:
    * Changing the response body if it is greater than 5.5 MB (Lambda body max is 6 MB)
    """
    if len(json.dumps(response.body)) > 5500000:
        response.body = 'Body size exceeded 6 MB maximum.'
        response.status_code = 413
    return response


def query_params_to_literals(params):
    """
    Simple function to loop through the query params and convert them to
    bools/ints/floats other literals as applicable
    """
    to_delete = []
    for key, value in params.items():
        if not value:
            # handles empty strings
            to_delete.append(key)
            continue
        try:
            as_literal = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            as_literal = value
        params[key] = as_literal
    for key in to_delete:
        del params[key]
    return params


def trim_output(output, max_size=100000):
    """
    AWS lambda has a maximum body response size of 6MB. Since results are currently delivered entirely
    in the body of the response, let's limit the size of the 'full_output', 'brief_output', and
    'admin_output' fields to 100 KB (see if this is a reasonable amount).
    Slice the dictionaries, lists, or string to achieve this.
    max_size input integer is in bites

    Takes in the non-json formatted version of the fields. For now, just use this for /view/.
    """
    formatted = json.dumps(output, indent=4)
    if len(formatted) > max_size:
        return ''.join([formatted[:max_size], '\n\n... Output truncated ...'])
    else:
        return formatted


##### ROUTE RUNNING FUNCTIONS #####


def view_run_check(environ, check, params, context="/"):
    """
    Called from the view endpoint (or manually, I guess), this queues the given
    check for the given environment and redirects to the view_foursight result
    for the new check.
    Params are kwargs that are read from the url query_params; they will be
    added to the kwargs used to run the check.

    Args:
        environ (str): Foursight environment name
        check (str): check function name
        params (dict): kwargs to use for check
        context (str): string context to use for Foursight routing

    Returns:
        chalicelib.Response: redirect to future check landing page
    """
    # convert string query params to literals
    params = query_params_to_literals(params)
    queued_uuid = queue_check(environ, check, params)
    # redirect to view page with a 302 so it isn't cached
    resp_headers = {'Location': '/'.join([context + 'view', environ, check, queued_uuid])}
    return Response(status_code=302, body=json.dumps(resp_headers),
                    headers=resp_headers)


def view_run_action(environ, action, params, context="/"):
    """
    Called from the view endpoint (or manually, I guess), this runs the given
    action for the given environment and refreshes the foursight view.
    Params are kwargs that are read from the url query_params; they will be
    added to the kwargs used to run the check.

    Args:
        environ (str): Foursight environment name
        action (str): action function name
        params (dict): kwargs to use for check
        context (str): string context to use for Foursight routing

    Returns:
        chalicelib.Response: redirect to check view that called this action
    """
    # convert string query params to literals
    params = query_params_to_literals(params)
    queued_uuid = queue_action(environ, action, params)
    # redirect to calling check view page with a 302 so it isn't cached
    if 'check_name' in params and 'called_by' in params:
        check_detail = '/'.join([params['check_name'], params['called_by']])
        resp_headers = {'Location': '/'.join([context + 'view', environ, check_detail])}
    else:
        # no check so cannot redirect
        act_path = '/'.join([context + 'checks', action, queued_uuid])
        return Response(
            body = {
                'status': 'success',
                'details': 'Action is queued. When finished, view at: %s' % act_path,
                'environment': environ
            },
            status_code = 200
        )
    return Response(status_code=302, body=json.dumps(resp_headers),
                    headers=resp_headers)


def view_foursight(environ, is_admin=False, domain="", context="/"):
    """
    View a template of all checks from the given environment(s).
    Environ may be 'all' or a specific FS environments separated by commas.
    With 'all', this function can be somewhat slow.
    Domain is the current FS domain, needed for Auth0 redirect.
    Context is the current context, usually "/api/" or "/"
    Returns a response with html content.
    Non-protected route
    """
    html_resp = Response('Foursight viewing suite')
    html_resp.headers = {'Content-Type': 'text/html'}
    environments = init_environments()
    total_envs = []
    view_envs = environments.keys() if environ == 'all' else [e.strip() for e in environ.split(',')]
    for this_environ in view_envs:
        try:
            connection = init_connection(this_environ)
        except Exception:
            connection = None
        if connection:
            grouped_results = get_grouped_check_results(connection)
            for group in grouped_results:
                for title, result in group.items():
                    if title == '_name':
                        continue
                    elif title == '_statuses':
                        # convert counts to strings for jinja
                        for stat, val in group[title].items():
                            group[title][stat] = str(val)
                        continue
                    else:
                        group[title] = process_view_result(connection, result, is_admin)
            total_envs.append({
                'status': 'success',
                'environment': this_environ,
                'groups': grouped_results
            })
    # prioritize these environments
    env_order = ['data', 'staging', 'webdev', 'hotseat', 'cgap']
    total_envs = sorted(total_envs, key=lambda v: env_order.index(v['environment']) if v['environment'] in env_order else 9999)
    template = jin_env.get_template('view_groups.html')
    # get queue information
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        envs=total_envs,
        stage=get_stage_info()['stage'],
        load_time=get_load_time(),
        is_admin=is_admin,
        domain=domain,
        context=context,
        running_checks=running_checks,
        queued_checks=queued_checks
    )
    html_resp.status_code = 200
    return process_response(html_resp)


def view_foursight_check(environ, check, uuid, is_admin=False, domain="", context="/"):
    """
    View a formatted html response for a single check (environ, check, uuid)
    """
    html_resp = Response('Foursight viewing suite')
    html_resp.headers = {'Content-Type': 'text/html'}
    total_envs = []
    try:
        connection = init_connection(environ)
    except Exception:
        connection = None
    if connection:
        res_check = CheckResult(connection, check)
        if res_check:
            data = res_check.get_result_by_uuid(uuid)
            if data is None:
                # the check hasn't run. Return a placeholder view
                data = {
                    'name': check,
                    'uuid': uuid,
                    'status': 'ERROR',
                    'summary': 'Check has not yet run',
                    'description': 'Check has not yet run'
                }
            title = get_check_title_from_setup(check)
            processed_result = process_view_result(connection, data, is_admin)
            total_envs.append({
                'status': 'success',
                'environment': environ,
                'checks': {title: processed_result}
            })
    template = jin_env.get_template('view_checks.html')
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        envs=total_envs,
        stage=get_stage_info()['stage'],
        load_time=get_load_time(),
        is_admin=is_admin,
        domain=domain,
        context=context,
        running_checks=running_checks,
        queued_checks=queued_checks
    )
    html_resp.status_code = 200
    return process_response(html_resp)


def get_load_time():
    """
    Returns the current time in ET, formatted the same was process_view_result
    """
    ts_utc = datetime.datetime.utcnow().replace(microsecond=0)
    ts_utc = ts_utc.replace(tzinfo=tz.tzutc())
    # change timezone to EST (specific location needed for daylight savings)
    ts_local = ts_utc.astimezone(tz.gettz('America/New_York'))
    return ''.join([str(ts_local.date()), ' at ', str(ts_local.time()), ' (', str(ts_local.tzname()), ')'])


def process_view_result(connection, res, is_admin):
    """
    Do some processing on the content of one check result (res arg, a dict)
    Processes timestamp string, trims output fields, and adds action info.

    For action info, if the check has an action, try to find the associated
    action by looking for '<check name>/action_records<check uuid>' object in
    s3. The contents will be the path to the action. If found, display as
    the "associated action" and disabled further runs of the action from the
    check; also edit the check summary to reflect that action has finished.
    Otherwise, allow runs of the action.
    For now, always show latest action as well.
    """
    # first check to see if res is just a string, meaning
    # the check didn't execute properly
    if not isinstance(res, dict):
        error_res = {
            'status': 'ERROR',
            'content': True,
            'title': 'Check System Error',
            'description': res,
            'uuid': 'Did not successfully run'
        }
        return error_res
    # this can be removed once uuid has been around long enough
    ts_utc = res['uuid'] if 'uuid' in res else res['timestamp']
    ts_utc = datetime.datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
    ts_utc = ts_utc.replace(tzinfo=tz.tzutc())
    # change timezone to EST (specific location needed for daylight savings)
    ts_local = ts_utc.astimezone(tz.gettz('America/New_York'))
    proc_ts = ''.join([str(ts_local.date()), ' at ', str(ts_local.time())])
    res['local_time'] = proc_ts
    if res.get('brief_output'):
        res['brief_output'] = trim_output(res['brief_output'])
    if res.get('full_output'):
        res['full_output'] = trim_output(res['full_output'])
    # only return admin_output if an admin is logged in
    if res.get('admin_output') and is_admin:
        res['admin_output'] = trim_output(res['admin_output'])
    else:
        res['admin_output'] = None

    # ### LOGIC FOR VIEWING ACTION ###
    # if this check has already run an action, display that. Otherwise, allow
    # action to be run.
    # For now also get the latest result for the checks action
    # TODO: replace latest_action with action history
    if res.get('action'):
        action = ActionResult(connection, res.get('action'))
        if action:
            action_record_key = '/'.join([res['name'], 'action_records', res['uuid']])
            assc_action_key = connection.connections['s3'].get_object(action_record_key)
            if assc_action_key:
                assc_action_key = assc_action_key.decode()  # in bytes
                assc_action = connection.connections['s3'].get_object(assc_action_key)
                # If assc_action_key is written but assc_action is None, then
                # it most likely means the action is still running
                if assc_action is not None:
                    # json.loads followed by json.dumps handles binary storage in s3
                    assc_action_contents = json.loads(assc_action)
                    res['assc_action_status'] = assc_action_contents['status']
                    res['assc_action'] = json.dumps(assc_action_contents, indent=4)
                    # update check summary
                    if res.get('summary'):
                        res['summary'] = 'ACTION %s: %s' % (assc_action_contents['status'], res['summary'])
                else:
                    res['assc_action_status'] = 'PEND'
                    res['assc_action'] = 'Associated action has not finished.'
                    # update check summary
                    if res.get('summary'):
                        res['summary'] = 'ACTION PENDING: %s' % res['summary']
                # don't allow the action to be run again from this check
                del res['action']
                res['allow_action'] = False
            elif res.get('allow_action') is True:
                # if there is an action + allow action is set but the action has
                # not yet run, display an icon status to signify this
                res['assc_action_status'] = 'ready'

            latest_action = action.get_latest_result()
            if latest_action:
                res['latest_action'] = json.dumps(latest_action, indent=4)
            else:
                res['latest_action'] = 'Not yet run.'
        else:
            del res['action']
    return res


def view_foursight_history(environ, check, start=0, limit=25, is_admin=False,
                           domain="", context="/"):
    """
    View a tabular format of the history of a given check or action (str name
    as the 'check' parameter) for the given environment. Results look like:
    status, kwargs.
    start controls where the first result is and limit controls how many
    results are retrieved (see get_foursight_history()).
    Returns html.
    """
    html_resp = Response('Foursight history view')
    html_resp.headers = {'Content-Type': 'text/html'}
    try:
        connection = init_connection(environ)
    except Exception:
        connection = None
    if connection:
        history = get_foursight_history(connection, check, start, limit)
        history_kwargs = list(set(chain.from_iterable([l[2] for l in history])))
    else:
        history, history_kwargs = [], []
    template = jin_env.get_template('history.html')
    check_title = get_check_title_from_setup(check)
    page_title = ''.join(['History for ', check_title, ' (', environ, ')'])
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        env=environ,
        check=check,
        load_time=get_load_time(),
        history=history,
        history_kwargs=history_kwargs,
        res_start=start,
        res_limit=limit,
        res_actual=len(history),
        page_title=page_title,
        stage=get_stage_info()['stage'],
        is_admin=is_admin,
        domain=domain,
        context=context,
        running_checks=running_checks,
        queued_checks=queued_checks
    )
    html_resp.status_code = 200
    return process_response(html_resp)


def get_foursight_history(connection, check, start, limit):
    """
    Get a brief form of the historical results for a check, including
    UUID, status, kwargs. Limit the number of results recieved to 500, unless
    otherwise specified ('limit' arg). 'start' arg determines where the start
    of the results grabbed is, with idx = 0 being the most recent one.

    'check' may be a check or an action (string name)
    """
    # limit 'limit' param to 500
    limit = 500 if limit > 500 else limit
    result_obj = init_check_or_action_res(connection, check)
    if not result_obj:
        return []
    return result_obj.get_result_history(start, limit)


def run_get_check(environ, check, uuid=None):
    """
    Loads a specific check or action result given an environment, check or
    action name, and uuid (all strings).
    If uuid is not provided, get the primary_result.
    """
    connection, response = init_response(environ)
    if not connection:
        return response
    res_obj = init_check_or_action_res(connection, check)
    if not res_obj:
        response.body = {
            'status': 'error',
            'description': 'Not a valid check or action.'
        }
        response.status_code = 400
    else:
        if uuid:
            data = res_obj.get_result_by_uuid(uuid)
        else:
            data = res_obj.get_primary_result()
        response.body = {
            'status': 'success',
            'data': data
        }
        response.status_code = 200
    return process_response(response)


def run_put_check(environ, check, put_data):
    """
    Abstraction of put_check functionality to allow for testing outside of chalice
    framework. Returns a response object
    """
    connection, response = init_response(environ)
    if not connection:
        return response
    if not isinstance(put_data, dict):
        response.body = {
            'status': 'error',
            'endpoint': 'put_check',
            'check': check,
            'description': ' '.join(['PUT request is malformed:', str(put_data)]),
            'environment': environ
        }
        response.status_code = 400
        return response
    put_uuid = put_data.get('uuid', datetime.datetime.utcnow().isoformat())
    putCheck = CheckResult(connection, check, init_uuid=put_uuid)
    # set valid fields from the PUT body. should this be dynamic?
    # if status is not included, it will be set to ERROR
    for field in ['title', 'status', 'summary', 'description', 'brief_output', 'full_output', 'admin_output']:
        put_content = put_data.get(field)
        prev_content = getattr(putCheck, field, None)
        if put_content:
            # append attribute data for _output fields if there are pre-existing
            # values originating from an existing put_uuid
            if prev_content and field in ['full_output', 'brief_output', 'admin_output']:
                # will be list, dict, or string. make sure they are same type
                if isinstance(prev_content, dict) and isinstance(put_content, dict):
                    prev_content.update(put_content)
                elif isinstance(prev_content, list) and isinstance(put_content, list):
                    prev_content.extend(put_content)
                elif isinstance(prev_content, basestring) and isinstance(put_content, basestring):
                    prev_content = prev_content + put_content
                else:
                    # cannot append, just update with new
                    prev_content = put_content
                setattr(putCheck, field, prev_content)
            else:
                setattr(putCheck, field, put_content)
    # set 'primary' kwarg so that the result is stored as 'latest'
    putCheck.kwargs = {'primary': True, 'uuid': put_uuid}
    stored = putCheck.store_result()
    response.body = {
        'status': 'success',
        'endpoint': 'put_check',
        'check': check,
        'updated_content': stored,
        'environment': environ
    }
    response.status_code = 200
    return process_response(response)


def run_put_environment(environ, env_data):
    """
    Abstraction of the functionality of put_environment without the current_request
    to allow for testing.
    """
    proc_environ = environ.split('-')[-1] if environ.startswith('fourfront-') else environ
    response = None
    if isinstance(env_data, dict) and {'fourfront', 'es'} <= set(env_data):
        ff_address = env_data['fourfront'] if env_data['fourfront'].endswith('/') else env_data['fourfront'] + '/'
        es_address = env_data['es'] if env_data['es'].endswith('/') else env_data['es'] + '/'
        ff_env = env_data['ff_env'] if 'ff_env' in env_data else ''.join(['fourfront-', proc_environ])
        env_entry = {
            'fourfront': ff_address,
            'es': es_address,
            'ff_env': ff_env
        }
        s3_connection = S3Connection('foursight-envs')
        s3_connection.put_object(proc_environ, json.dumps(env_entry))
        stage = get_stage_info()['stage']
        s3_bucket = ''.join(['foursight-', stage, '-', proc_environ])
        bucket_res = s3_connection.create_bucket(s3_bucket)
        if not bucket_res:
            response = Response(
                body = {
                    'status': 'error',
                    'description': ' '.join(['Could not create bucket:', s3_bucket]),
                    'environment': proc_environ
                },
                status_code = 500
            )
        else:
            # if not testing, queue checks with 'put_env' condition for the new env
            if 'test' not in get_stage_info()['queue_name']:
                for sched in get_schedule_names():
                    queue_scheduled_checks(environ, sched, conditions=['put_env'])
            response = Response(
                body = {
                    'status': 'success',
                    'description': ' '.join(['Succesfully made:', proc_environ]),
                    'environment': proc_environ
                },
                status_code = 200
            )
    else:
        response = Response(
            body = {
                'status': 'error',
                'description': 'Environment creation failed',
                'body': env_data,
                'environment': proc_environ
            },
            status_code = 400
        )
    return process_response(response)


def run_get_environment(environ):
    """
    Return config information about a given environment, or throw an error
    if it is not valid.
    """
    environments = init_environments()
    if environ in environments:
        response = Response(
            body = {
                'status': 'success',
                'details': environments[environ],
                'environment': environ
            },
            status_code = 200
        )
    else:
        response = Response(
            body = {
                'status': 'error',
                'description': 'Invalid environment provided. Should be one of: %s' % (str(list(environments.keys()))),
                'environment': environ
            },
            status_code = 400
        )
    return process_response(response)


##### QUEUE / CHECK RUNNER FUNCTIONS #####

def queue_scheduled_checks(sched_environ, schedule_name, conditions=None):
    """
    Given a str environment and schedule name, add the check info to the
    existing queue (or creates a new one if there is none). Then initiates 4
    check runners that are linked to the queue that are self-propogating.

    If sched_environ == 'all', then loop through all in list_environments()

    Run with schedule_name = None to skip adding the check group to the queue
    and just initiate the check runners.

    Can optionally provide a list of conditions that will be used as used to
    filter the checks to schedule based on the 'conditions' list in check_setup

    Args:
        sched_environ (str): Foursight environment name to schedule on
        schedule_name (str): schedule name from check_setup / app
        conditions (list): optional list of one or more conditions to filter by

    Returns:
        dict: runner input of queued messages, used for testing
    """
    queue = get_sqs_queue()
    if schedule_name is not None:
        if sched_environ not in ['all', 'all_4dn'] and sched_environ not in list_environments():
            print('-RUN-> %s is not a valid environment. Cannot queue.' % sched_environ)
            return
        sched_environs = list_environments() if sched_environ in ['all', 'all_4dn'] else [sched_environ]
        # remove any environments with 'cgap' if all_4dn
        if sched_environ == 'all_4dn':
            sched_environs = [env for env in sched_environ if 'cgap' not in env]
        check_schedule = get_check_schedule(schedule_name, conditions)
        if not check_schedule:
            print('-RUN-> %s is not a valid schedule. Cannot queue.' % schedule_name)
            return
        for environ in sched_environs:
            # add the run info from 'all' as well as this specific environ
            check_vals = copy.copy(check_schedule.get('all', []))
            # also add 'all_4dn' if we are in a non-cgap environment
            if 'cgap' not in environ:
                check_vals.extend(check_schedule.get('all_4dn', []))
            check_vals.extend(check_schedule.get(environ, []))
            send_sqs_messages(queue, environ, check_vals)
    runner_input = {'sqs_url': queue.url}
    for n in range(4): # number of parallel runners to kick off
        invoke_check_runner(runner_input)
    return runner_input # for testing purposes


def queue_check(environ, check, params={}, deps=[], uuid=None):
    """
    Queue a single check, given by check function name, with given parameters
    and dependencies (both optional). Also optionally pass in a uuid, which
    will be used for the run if provided

    Args:
        environ (str): Foursight environment name
        check (str): check function name
        params (dict): kwargs to use for check. Defaults to {}
        deps (list): list of dependencies for the check. Defaults to []
        uuid (str): optional uuid to pass to the run. Defaults to None

    Returns:
        str: uuid of the queued check (from send_single_to_queue)
    """
    check_str = get_check_strings(check)
    if not check_str:
        error_res = {
            'status': 'error',
            'description': 'could not find check %s' % check,
            'environment': environ,
            'checks': {}
        }
        raise Exception(str(error_res))
    to_send = [check_str, params, deps]
    return send_single_to_queue(environ, to_send, uuid)


def queue_action(environ, action, params={}, deps=[], uuid=None):
    """
    Queue a single action, given by action function name, with given parameters
    and dependencies (both optional). Also optionally pass in a uuid, which
    will be used for the run if provided

    Args:
        environ (str): Foursight environment name
        action (str): action function name
        params (dict): kwargs to use for action. Defaults to {}
        deps (list): list of dependencies for the action. Defaults to []
        uuid (str): optional uuid to pass to the run. Defaults to None

    Returns:
        str: uuid of the queued action (from send_single_to_queue)
    """
    action_str = get_action_strings(action)
    if not action_str:
        error_res = {
            'status': 'error',
            'description': 'could not find action %s' % action,
            'environment': environ,
            'checks': {}
        }
        raise Exception(str(error_res))
    to_send = [action_str, params, deps]
    return send_single_to_queue(environ, to_send, uuid)


def send_single_to_queue(environ, to_send, uuid, invoke_runner=True):
    """
    Send a single formatted check/action to SQS for given environ. Pass
    the given uuid to send_sqs_messages. Invoke a single check runner lambda

    Args:
        environ (str): Foursight environment name
        to_send (list): check/action entry in form [check_str, params, deps]
        uuid (str): uuid to pass to run. If None, generate a new uuid
        invoke_runner (bool): If True, invoke a check_runner lambda

    Returns:
        str: uuid of the queued run (from send_single_to_queue)
    """
    queue = get_sqs_queue()
    queued_uuid = send_sqs_messages(queue, environ, [to_send], uuid=uuid)
    # kick off a single check runner lambda
    if invoke_runner is True:
        invoke_check_runner({'sqs_url': queue.url})
    return queued_uuid


def run_check_runner(runner_input, propogate=True):
    """
    Run logic for a check runner. Used to run checks and actions.

    runner_input should be a dict containing one
    key: sqs_url that corresponds to the aws url for the queue.
    This function attempts to recieve one message from the standard SQS queue
    using long polling, checks the run dependencies for that check/action, and then
    will run the check.

    If the run was a check and the 'queue_action' parameter is set, along with
    check.action and check.allow_action, will attempt to queue the associated
    action.

    If dependencies are not met, the check is not run and
    the run info is put back in the queue. Otherwise, the message is deleted
    from the queue.

    If there are no messages left (should always be true when nothing is
    recieved from sqs with long polling), then exit and do not propogate another
    check runner. Otherwise, initiate another check_runner to continue the process.

    Args:
        runner_input (dict): body of SQS message
        propogate (bool): if True (default), invoke another check runner lambda

    Returns:
        dict: run result if something was run, else None
    """
    sqs_url = runner_input.get('sqs_url')
    if not sqs_url:
        return
    client = boto3.client('sqs')
    response = client.receive_message(
        QueueUrl=sqs_url,
        AttributeNames=['MessageGroupId'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=300,
        WaitTimeSeconds=10
    )
    message = response.get('Messages', [{}])[0]
    body = message.get('Body')
    receipt = message.get('ReceiptHandle')
    if not body or not receipt:
        # if no messages recieved in 10 seconds of long polling, terminate
        return None
    check_list = json.loads(body)
    if not isinstance(check_list, list) or len(check_list) != 5:
        # if not a valid check str, remove the item from the SQS
        delete_message_and_propogate(runner_input, receipt, propogate=propogate)
        return None
    [run_env, run_uuid, run_name, run_kwargs, run_deps] = check_list
    # find information from s3 about completed checks in this run
    # actual id stored in s3 has key: <run_uuid>/<run_name>
    if run_deps and isinstance(run_deps, list):
        already_run = collect_run_info(run_uuid)
        deps_w_uuid = ['/'.join([run_uuid, dep]) for dep in run_deps]
        finished_dependencies = set(deps_w_uuid).issubset(already_run)
        if not finished_dependencies:
            print('-RUN-> Not ready for: %s' % (run_name))
    else:
        finished_dependencies = True
    connection = init_connection(run_env)
    if finished_dependencies:
        # add the run uuid as the uuid to kwargs so that checks will coordinate
        if 'uuid' not in run_kwargs:
            run_kwargs['uuid'] = run_uuid
        run_kwargs['_run_info'] = {'run_id': run_uuid, 'receipt': receipt, 'sqs_url': sqs_url}
        # if this is an action, ensure we have not already written an action record
        if 'check_name' in run_kwargs and 'called_by' in run_kwargs:
            rec_key = '/'.join([run_kwargs['check_name'], 'action_records', run_kwargs['called_by']])
            found_rec = connection.get_object(rec_key)
            if found_rec is not None:
                # the action record has been written. Abort and propogate
                print('-RUN-> Found existing action record: %s. Skipping' % rec_key)
                delete_message_and_propogate(runner_input, receipt, propogate=propogate)
                return None
            else:
                # make a action record before running the action
                # action name is the second part of run_name
                act_name = run_name.split('/')[-1]
                rec_body = ''.join([act_name, '/', run_uuid, '.json'])
                connection.put_object(rec_key, rec_body)
                print('-RUN-> Wrote action record: %s' % rec_key)
        run_result = run_check_or_action(connection, run_name, run_kwargs)
        print('-RUN-> RESULT:  %s (uuid)' % str(run_result.get('uuid')))
        # invoke action if running a check and kwargs['queue_action'] matches stage
        stage = get_stage_info()['stage']
        if run_result['type'] == 'check' and run_result['kwargs']['queue_action'] == stage:
            # must also have check.action and check.allow_action set
            if run_result['allow_action'] and run_result['action']:
                action_params = {'check_name': run_result['name'],
                                 'called_by': run_result['kwargs']['uuid']}
                try:
                    queue_action(run_env, run_result['action'],
                                 params=action_params, uuid=run_uuid)
                except Exception as exc:
                    print('-RUN-> Could not queue action %s on stage %s with kwargs: %s. Error: %s'
                          % (run_result['action'], stage, action_params, str(exc)))
                else:
                    print('-RUN-> Queued action %s on stage %s with kwargs: %s'
                          % (run_result['action'], stage, action_params))
        print('-RUN-> Finished: %s' % (run_name))
        delete_message_and_propogate(runner_input, receipt, propogate=propogate)
        return run_result
    else:
        print('-RUN-> Recovered: %s' % (run_name))
        recover_message_and_propogate(runner_input, receipt, propogate=propogate)
        return None
