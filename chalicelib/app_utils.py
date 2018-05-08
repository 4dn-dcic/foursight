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
from .fs_connection import FSConnection
from .check_utils import (
    get_check_group_results,
    get_check_strings,
    get_action_strings,
    get_check_schedule,
    run_check_or_action,
    init_check_res,
    init_action_res,
    init_check_or_action_res
)
from .utils import basestring
from .s3_connection import S3Connection
from .check_groups import CHECK_GROUPS

jin_env = Environment(
    loader=FileSystemLoader('chalicelib/templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

# set environmental variables in .chalice/config.json
STAGE = os.environ.get('chalice_stage', 'dev') # default to dev
QUEUE_NAME = '-'.join(['foursight', STAGE, 'check_queue'])
RUNNER_NAME = '-'.join(['foursight', STAGE, 'check_runner'])


def list_environments():
    """
    Lists all environments in the foursight-envs s3. Returns a list of names
    """
    s3_connection = S3Connection('foursight-envs')
    return s3_connection.list_all_keys()


def init_environments(env='all'):
    """
    Generate environment information from the foursight-envs bucket in s3.
    Returns a dictionary keyed by environment name with value of a sub-dict
    with the fields needed to initiate a connection.
    """
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
                'bucket': ''.join(['foursight-', STAGE, '-', env_key])
            }
            environments[env_key] = env_entry
    return environments


def init_connection(environ):
    """
    Initialize the fourfront/s3 connection using the FSConnection object
    and the given environment.
    Returns an FSConnection object (or None if error) and a dictionary
    error response.
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
        return None, error_res
    connection = FSConnection(environ, environments[environ])
    return connection, error_res


def init_response(environ):
    """
    Generalized function to init response given an environment
    """
    response = Response('Foursight response')
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
    return connection, response


def check_authorization(request_dict):
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
            # leeway accounts for clock drift between us and auth0
            payload = jwt.decode(token, b64decode(auth0_secret, '-_'), audience=auth0_client, leeway=30)
            if payload.get('email') == os.environ.get('ADMIN', '') and payload.get('email_verified') is True:
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


def forbidden_response():
    return Response(
        status_code=403,
        body='Forbidden. Login on the /api/view/<environ> page.'
        )

def process_response(response):
    """
    Does any final processing of a Foursight response before returning it. Right now, this includes:
    * Changing the response body if it is greater than 5.5 MB (Lambda body max is 6 MB)
    """
    if len(json.dumps(response.body)) > 5500000:
        response.body = 'Body size exceeded 6 MB maximum. Try visiting /api/view/data.'
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


def view_run_check(environ, check, params):
    """
    Called from the view endpoint (or manually, I guess), this re-runs the given
    checks for the given environment and returns the
    view_foursight templated result with the new check result.
    Params are kwargs that are read from the url query_params; they will be
    added to the kwargs used to run the check.

    This also be used to queue a check group. This is checked before individual check names
    """
    resp_headers = {'Location': '/api/view/' + environ}
    connection, _ = init_connection(environ)
    check_str = get_check_strings(check)
    # convert string query params to literals
    params = query_params_to_literals(params)
    if connection and check_str:
        res = run_check_or_action(connection, check_str, params)
        if res and res.get('uuid'):
            resp_headers = {'Location': '/'.join(['/api/view', environ, check, res['uuid']])}
    # redirect to view page with a 302 so it isn't cached
    return Response(
        status_code=302,
        body=json.dumps(resp_headers),
        headers=resp_headers)


def view_run_action(environ, action, params):
    """
    Called from the view endpoint (or manually, I guess), this runs the given
    action for the given environment and refreshes the foursight view.
    Params are kwargs that are read from the url query_params; they will be
    added to the kwargs used to run the check.

    This also be used to queue an action group. This is checked before individual action names
    """
    connection, _ = init_connection(environ)
    action_str = get_action_strings(action)
    # convert string query params to literals
    params = query_params_to_literals(params)
    if connection and action_str:
        run_check_or_action(connection, action_str, params)
    resp_headers = {'Location': '/api/view/' + environ}
    # redirect to view_foursight page with a 302 so it isn't cached
    return Response(
        status_code=302,
        body=json.dumps(resp_headers),
        headers=resp_headers)


def view_foursight(environ, is_admin=False, domain=""):
    """
    View a template of all checks from the given environment(s).
    Environ may be 'all' or a specific FS environments separated by commas.
    With 'all', this function can be somewhat slow.
    Domain is the current FS domain, needed for Auth0 redirect.
    Returns a response with html content.
    Non-protected route
    """
    html_resp = Response('Foursight viewing suite')
    html_resp.headers = {'Content-Type': 'text/html'}
    environments = init_environments()
    total_envs = []
    view_envs = environments.keys() if environ == 'all' else [e.strip() for e in environ.split(',')]
    for this_environ in view_envs:
        connection, error_res = init_connection(this_environ)
        if connection:
            results = get_check_group_results(connection, 'all')
            processed_results = process_view_results(connection, results, is_admin)
            total_envs.append({
                'status': 'success',
                'environment': this_environ,
                'checks': processed_results
            })
    # prioritize these environments
    env_order = ['data', 'staging', 'webdev', 'hotseat']
    total_envs = sorted(total_envs, key=lambda v: env_order.index(v['environment']) if v['environment'] in env_order else 9999)
    template = jin_env.get_template('view.html')
    # get queue information
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        envs=total_envs,
        stage=STAGE,
        is_admin=is_admin,
        domain=domain,
        running_checks=running_checks,
        queued_checks=queued_checks
    )
    html_resp.status_code = 200
    return process_response(html_resp)


def view_foursight_check(environ, check, uuid, is_admin=False, domain=""):
    """
    View a formatted html response for a single check (environ, check, uuid)
    """
    html_resp = Response('Foursight viewing suite')
    html_resp.headers = {'Content-Type': 'text/html'}
    total_envs = []
    connection, error_res = init_connection(environ)
    if connection:
        res_check = init_check_res(connection, check)
        if res_check:
            data = res_check.get_result_by_uuid(uuid)
            processed_results = process_view_results(connection, [data], is_admin)
            total_envs.append({
                'status': 'success',
                'environment': environ,
                'checks': processed_results
            })
    template = jin_env.get_template('view.html')
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        envs=total_envs,
        stage=STAGE,
        is_admin=is_admin,
        domain=domain,
        running_checks=running_checks,
        queued_checks=queued_checks
    )
    html_resp.status_code = 200
    return process_response(html_resp)


def process_view_results(connection, results, is_admin):
    processed_results = []
    for res in results:
        # first check to see if res is just a string, meaning
        # the check didn't execute properly
        if not isinstance(res, dict):
            error_res = {
                'status': 'ERROR',
                'content': True,
                'title': 'Check System Error',
                'description': res,
                'uuid': 'Did not run.'
            }
            processed_results.append(error_res)
            continue
        # this can be removed once uuid has been around long enough
        ts_utc = res['uuid'] if 'uuid' in res else res['timestamp']
        ts_utc = datetime.datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
        ts_utc = ts_utc.replace(tzinfo=tz.tzutc())
        # change timezone to EST (specific location needed for daylight savings)
        ts_local = ts_utc.astimezone(tz.gettz('America/New_York'))
        proc_ts = ''.join([str(ts_local.date()), ' at ', str(ts_local.time()), ' (', str(ts_local.tzname()), ')'])
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
        # get the latest result for the checks action, if present
        if res.get('action'):
            action = init_action_res(connection, res.get('action'))
            if action:
                latest_action = action.get_latest_result()
                if latest_action:
                    res['latest_action'] = json.dumps(latest_action, indent=4)
                else:
                    res['latest_action'] = 'Not yet run.'
            else:
                del res['action']
        processed_results.append(res)
    return processed_results


def view_foursight_history(environ, check, start=0, limit=25, is_admin=False, domain=""):
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
    connection, error_res = init_connection(environ)
    if connection:
        history = get_foursight_history(connection, check, start, limit)
        history_kwargs = list(set(chain.from_iterable([l[1] for l in history])))
    else:
        history, history_kwargs = [], []
    template = jin_env.get_template('history.html')
    check_title = ' '.join(check.split('_')).title()
    page_title = ''.join(['History for ', check_title, ' (', environ, ')'])
    queue_attr = get_sqs_attributes(get_sqs_queue().url)
    running_checks = queue_attr.get('ApproximateNumberOfMessagesNotVisible')
    queued_checks = queue_attr.get('ApproximateNumberOfMessages')
    html_resp.body = template.render(
        env=environ,
        check=check,
        history=history,
        history_kwargs=history_kwargs,
        res_start=start,
        res_limit=limit,
        res_actual=len(history),
        page_title=page_title,
        stage=STAGE,
        is_admin=is_admin,
        domain=domain,
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
    putCheck = init_check_res(connection, check, init_uuid=put_uuid)
    # set valid fields from the PUT body. should this be dynamic?
    # if status is not included, it will be set to ERROR
    for field in ['title', 'status', 'description', 'brief_output', 'full_output', 'admin_output']:
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
        s3_bucket = ''.join(['foursight-', STAGE, '-', proc_environ])
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
            # run some checks on the new env
            queue_check_group(environ, 'all_checks')
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


def get_environment(environ):
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

##### CHECK RUNNER FUNCTIONS #####

def queue_scheduled_checks(sched_environ, schedule_name):
    """
    Given a str environment and schedule name, add the check info to the
    existing queue (or creates a new one if there is none). Then initiates 4
    check runners that are linked to the queue that are self-propogating.

    If sched_environ == 'all', then loop through all in list_environments()

    Run with schedule_name = None to skip adding the check group to the queue
    and just initiate the check runners.
    """
    queue = get_sqs_queue()
    if schedule_name is not None:
        if sched_environ != 'all' and sched_environ not in list_environments():
            print('-RUN-> %s is not a valid environment. Cannot queue.' % sched_environ)
            return
        sched_environs = list_environments() if sched_environ == 'all' else [sched_environ]
        check_schedule = get_check_schedule(schedule_name)
        if not check_schedule:
            print('-RUN-> %s is not a valid schedule. Cannot queue.' % schedule_name)
            return
        for environ in sched_environs:
            # add the run info from 'all' as well as this specific environ
            check_vals = copy.copy(check_schedule.get('all', []))
            check_vals.extend(check_schedule.get(environ, []))
            send_sqs_messages(queue, environ, check_vals)
    runner_input = {'sqs_url': queue.url}
    for n in range(4): # number of parallel runners to kick off
        invoke_check_runner(runner_input)
    return runner_input # for testing purposes


def get_sqs_queue():
    """
    Returns boto3 sqs resource with QueueName=QUEUE_NAME
    """
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    except:
        queue = sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '300',
                'MessageRetentionPeriod': '3600'
            }
        )
    return queue


def send_sqs_messages(queue, environ, check_vals):
    """
    Send the messafges to the queue. Check_vals are entries within a check_group
    """
    # uuid used as the MessageGroupId
    uuid = datetime.datetime.utcnow().isoformat()
    # append environ and uuid as first elements to all check_vals
    proc_vals = [[environ, uuid] + val for val in check_vals]
    for val in proc_vals:
        response = queue.send_message(MessageBody=json.dumps(val))


def get_sqs_attributes(sqs_url):
    """
    Returns a dict of the desired attributes form the queue with given url
    """
    backup = {
        'ApproximateNumberOfMessages': 'ERROR',
        'ApproximateNumberOfMessagesNotVisible': 'ERROR'
    }
    client = boto3.client('sqs')
    try:
        result = client.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
    except:
        return backup
    return result.get('Attributes', backup)


def invoke_check_runner(runner_input):
    """
    Simple function to invoke the next check_runner lambda with runner_input
    (dict containing {'sqs_url': <str>})
    """
    client = boto3.client('lambda')
    # InvocationType='Event' makes asynchronous
    # try/except while async invokes are problematics
    try:
        response = client.invoke(
            FunctionName=RUNNER_NAME,
            InvocationType='Event',
            Payload=json.dumps(runner_input)
        )
    except:
        response = client.invoke(
            FunctionName=RUNNER_NAME,
            Payload=json.dumps(runner_input)
        )
    return response


def delete_message_and_propogate(runner_input, receipt):
    """
    Delete the message with given receipt from sqs queue and invoke the next
    lambda runner.
    """
    sqs_url = runner_input.get('sqs_url')
    if not sqs_url or not receipt:
        return
    client = boto3.client('sqs')
    client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=receipt
    )
    invoke_check_runner(runner_input)


def recover_message_and_propogate(runner_input, receipt):
    """
    Recover the message with given receipt to sqs queue and invoke the next
    lambda runner.

    Changing message VisibilityTimeout to 15 seconds means the message will be
    available to the queue in that much time. This is a slight lag to allow
    dependencies to process.
    NOTE: VisibilityTimeout should be less than WaitTimeSeconds in run_check_runner
    """
    sqs_url = runner_input.get('sqs_url')
    if not sqs_url or not receipt:
        return
    client = boto3.client('sqs')
    client.change_message_visibility(
        QueueUrl=sqs_url,
        ReceiptHandle=receipt,
        VisibilityTimeout=15
    )
    invoke_check_runner(runner_input)


def record_run_info(run_uuid, dep_id, check_status):
    """
    Add a record of the completed check to the foursight-runs bucket with name
    equal to the dependency id. The object itself is only the status of the run.
    Returns True on success, False otherwise
    """
    s3_connection = S3Connection('foursight-runs')
    record_key = '/'.join([run_uuid, dep_id])
    resp = s3_connection.put_object(record_key, json.dumps(check_status))
    return resp is not None


def collect_run_info(run_uuid):
    """
    Returns a set of run checks under this run uuid
    """
    s3_connection = S3Connection('foursight-runs')
    run_prefix = ''.join([run_uuid, '/'])
    complete = s3_connection.list_all_keys_w_prefix(run_prefix)
    # eliminate duplicates
    return set(complete)


def run_check_runner(runner_input):
    """
    Run logic for a check runner. runner_input should be a dict containing one
    key: sqs_url that corresponds to the aws url for the queue.
    This function attempts to recieve one message from the standard SQS queue
    using long polling, checks the run dependencies for that check, and then
    will run the check. If dependencies are not met, the check is not run and
    the run info is put back in the queue. Otherwise, the message is deleted
    from the queue.

    If there are no messages left (should always be true when nothing is
    recieved from sqs with long polling), then exit and do not propogate another
    check runner. Otherwise, initiate another check_runner to continue the process.
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
        WaitTimeSeconds=20
    )
    message = response.get('Messages', [{}])[0]
    body = message.get('Body')
    receipt = message.get('ReceiptHandle')
    if not body or not receipt:
        # if no messages recieved in 20 seconds of long polling, terminate
        return
    check_list = json.loads(body)
    if not isinstance(check_list, list) or len(check_list) != 6:
        # if not a valid check str, remove the item from the SQS
        delete_message_and_propogate(runner_input, receipt)
        return
    [run_env, run_uuid, check_name, check_kwargs, check_deps, dep_id] = check_list
    # find information from s3 about completed checks in this run
    if check_deps and isinstance(check_deps, list):
        already_run = collect_run_info(run_uuid)
        deps_w_uuid = ['/'.join([run_uuid, dep]) for dep in check_deps]
        finished_dependencies = set(deps_w_uuid).issubset(already_run)
        if not finished_dependencies:
            print('-RUN-> Not ready for: %s' % (check_name))
    else:
        finished_dependencies = True
    connection, error_res = init_connection(run_env)
    if connection and finished_dependencies:
        # add the run uuid as the uuid to kwargs so that checks will coordinate
        if 'uuid' not in check_kwargs:
            check_kwargs['uuid'] = run_uuid
        # if run_checks times out, sqs will recover message in 300 sec (VisibilityTimeout)
        run_result = run_check_or_action(connection, check_name, check_kwargs)
        print('-RUN-> RESULT:  %s (uuid)' % str(run_result.get('uuid')))
        recorded = record_run_info(run_uuid, dep_id, run_result.get('status'))
    else:
        recorded = False
    if recorded:
        print('-RUN-> Finished: %s' % (check_name))
        delete_message_and_propogate(runner_input, receipt)
    else:
        print('-RUN-> Recovered: %s' % (check_name))
        recover_message_and_propogate(runner_input, receipt)
