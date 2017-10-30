from __future__ import print_function, unicode_literals
from chalice import Chalice, Cron, Rate, Response
import json
import boto3
import os
from chalicelib.ff_connection import FFConnection
from chalicelib.checksuite import CheckSuite, daily_check, rate_check
from chalicelib.checkresult import CheckResult
from chalicelib.utils import get_methods_by_deco
from chalicelib.s3_connection import S3Connection
from botocore.exceptions import ClientError
from itertools import chain

app = Chalice(app_name='foursight')
app.debug = True

ENVIRONMENTS = {}
CACHED = {}
# set environmental variables in .chalice/config.json
STAGE = os.environ.get('chalice_stage', 'dev') # default to dev


def init_environments(env='all'):
    """
    Generate environments for ENVIRONMENTS global variable by reading
    the foursight-dev bucket.
    Returns a list of environments/keys that are not valid.
    """
    s3connection = S3Connection('foursight-envs')
    env_keys = s3connection.list_all_keys()
    bad_keys = []
    if env != 'all':
        if env in env_keys:
            env_keys = [env]
        else:
            return [env]
    else: # reset ENVIRONMENTS if we're trying to init all
        global ENVIRONMENTS
        ENVIRONMENTS = {}
    for env_key in env_keys:
        env_res = json.loads(s3connection.get_object(env_key))
        # check that the keys we need are in the object
        if isinstance(env_res, dict) and {'fourfront', 'es'} <= set(env_res):
            env_entry = {
                'fourfront': env_res['fourfront'],
                'es': env_res['es'],
                'bucket': ''.join(['foursight-', STAGE, '-', env_key])
            }
            if 'local_server' in env_res:
                env_entry['local_server'] = env_res['local_server']
            ENVIRONMENTS[env_key] = env_entry
        else:
            bad_keys.append(env_key)
    return bad_keys


def init_connection(environ):
    """
    Initialize the fourfront/s3 connection using the FFConnection object
    and the given environment.
    Returns an FFConnection object (or None if error) and a dictionary
    error response.
    """
    error_res = {}
    # try re-initializing ENVIRONMENTS if environ is not found
    if environ not in ENVIRONMENTS:
        init_environments()
    # if still not there, return an error
    if environ not in ENVIRONMENTS:
        error_res = {
            'status': 'error',
            'description': 'invalid environment provided. Should be one of: %s' % (str(list(ENVIRONMENTS.keys()))),
            'environment': environ,
            'checks': {}
        }
        return None, error_res
    try:
        connection = CACHED[environ]
    except KeyError:
        info = ENVIRONMENTS[environ]
        CACHED[environ] = FFConnection(environ, info['fourfront'], info['bucket'], info['es'])
        connection = CACHED[environ]
    if not connection.is_up:
        error_res = {
            'status': 'error',
            'description': 'The connection to fourfront is down',
            'environment': environ,
            'checks': {}
        }
        return None, error_res
    return connection, error_res


def init_check_suite(checks, connection):
    """
    Build a CheckSuite object from the given connection and find suitable
    methods to run from it, based on the desired checks input (a string)
    If checks == 'all', then every check in checksuite will be run.
    If checks == 'daily', then every daily_check in checksuite will be run.
    If checks == 'rate', then every rate_check in checksuite will be run.
    Otherwise, checks should be a comma separated list of checks to run,
    such as: 'item_counts_by_type,indexing_progress'
    """
    checkSuite = CheckSuite(connection)
    check_methods = []
    daily_check_methods = get_methods_by_deco(CheckSuite, daily_check)
    rate_check_methods = get_methods_by_deco(CheckSuite, rate_check)
    if checks == 'all' or checks == 'daily':
        check_methods.extend(daily_check_methods)
    if checks == 'all' or checks == 'rate':
        check_methods.extend(rate_check_methods)
    if checks not in ['all', 'daily', 'rate']:
        specified_checks = [spec.strip() for spec in checks.split(',')]
        all_methods = chain(daily_check_methods, rate_check_methods)
        method_lookup = {method.__name__: method for method in all_methods}
        for in_check in specified_checks:
            if in_check in method_lookup:
                check_methods.append(method_lookup[in_check])
    return check_methods, checkSuite


def init_response(request, environ):
    """
    Initialize the response object that will be returned from chalice.
    Please not that this function is not strictly necessary, as returning
    JSON will automatically send a response with status_code of 200.
    This function also handles CORS requests by echoing Access-Control-*
    headers back if Origin is in the provided request headers.
    Returns an initialized chalice response.
    MUST receive a valid request object, so this should be run from lambdas.
    """
    resp = Response('Foursight preflight response') # response body
    req_dict = request.to_dict()
    origin = req_dict.get('headers', {}).get('origin', None)
    if origin:
        use_origin = origin if origin.endswith('/') else ''.join([origin, '/'])
        allowed_origins = []
        if environ not in ENVIRONMENTS:
            init_environments()
        env_ff_server = ENVIRONMENTS.get(environ, {}).get('fourfront')
        if env_ff_server: allowed_origins.append(env_ff_server)
        # special case for test server
        if environ == 'local':
            local_ff_server = ENVIRONMENTS.get('local', {}).get('local_server')
            if local_ff_server: allowed_origins.append(local_ff_server)
        if use_origin in allowed_origins:
            resp.headers = {
                'Access-Control-Allow-Origin': origin,
                'Access-Control-Allow-Credentials': 'true',
                'Access-Control-Allow-Headers': ', '.join([
                    'Authorization',
                    'Content-Type',
                    'X-Amz-Date',
                    'X-Amz-Security-Token',
                    'X-Api-Key',
                    'X-Requested-With'
                ])
            }
    return resp


def perform_run_checks(connection, checks):
    """
    This function needed to be split out to separate timed invocation of
    checks to manual invocation.
    """
    check_methods, checkSuite = init_check_suite(checks, connection)
    did_run = []
    for method in check_methods:
        name = method.__name__
        try:
            run_res = method(checkSuite)
        except:
            continue
        if run_res:
            did_run.append(name)
    return did_run


def perform_get_latest(connection, checks):
    """
    Abstraction of get_lastest_checks to allow the functionality of running
    the function from a request to foursight or a scheduled/manual function.
    """
    check_methods, checkSuite = init_check_suite(checks, connection)
    results = []
    did_check = []
    for method in check_methods:
        name = method.__name__
        # the CheckResult below is used solely to collect the latest check
        TempCheck = CheckResult(connection.s3connection, name)
        latest_res = TempCheck.get_latest_check()
        if latest_res:
            results.append(latest_res)
            did_check.append(name)
    return results, did_check


# from chalice import AuthResponse
# import jwt
# from base64 import b64decode
"""
All you need to do to add authorizer to a route:
app.route('/', ... , authorizer=auth0_authorizer)
See: https://github.com/aws/chalice/blob/master/docs/source/topics/authorizers.rst
"""

# @app.authorizer()
# def auth0_authorizer(auth_request):
#     token = getattr(req_headers, 'token', None)
#     if not token:
#         return AuthResponse(routes=[], principal_id='user')
#
#     req_headers = auth_request.headers
#     if not req_headers:
#         return AuthResponse(routes=[], principal_id='user')
#     auth0_client = getattr(req_headers, 'auth0_client', None)
#     auth0_secret = getattr(req_headers, 'auth0_secret', None)
#     if auth0_client and auth0_secret:
#         try:
#             # leeway accounts for clock drift between us and auth0
#             payload = jwt.decode(token, b64decode(auth0_secret, '-_'),
#                                  audience=auth0_client, leeway=30)
#             if 'email' in payload and payload.get('email_verified') is True:
#                 return AuthResponse(routes=['/'], principal_id='user')
#         except:
#             return AuthResponse(routes=['/'], principal_id='test')
#     else:
#         return AuthResponse(routes=[], principal_id='user')


@app.route('/', methods=['GET'])
def index():
    """
    Test route
    """
    return json.dumps({'foursight': 'insight into fourfront'})


@app.route('/run/{environ}/{checks}', methods=['PUT', 'GET', 'OPTIONS'])
def run_foursight(environ, checks):
    """
    Two functionalities, based on the request method.

    If PUT:
    Run the given checks on the given environment, creating a record in the
    corresponding S3 bucket under the check's method name.
    The latest run of checks replaces the 'latest' label for each check
    directory in S3 and also creates a timestamped record.

    If GET:
    Return JSON of each check tagged with the "latest" tag for speicified current
    checks in checksuite for the given environment. If checks == 'all', every
    registered check will be returned. Otherwise, send a comma separated list
    of check names (the method names!) as the check argument.

    CORS enabled.
    """
    response = init_response(app.current_request, environ)
    if app.current_request.method == 'OPTIONS':
        return response
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
        return response
    if app.current_request.method == 'PUT':
        did_run = perform_run_checks(connection, checks)
        response.body = {
            'status': 'success',
            'checks_specified': checks,
            'checks_run': did_run,
            'environment': environ
        }
        response.status_code = 200
        return response
    else: # GET. latest results for each check
        results, did_check = perform_get_latest(connection, checks)
        response.body = {
            'status': 'success',
            'environment': environ,
            'checks': results,
            'checks_found': did_check
        }
        response.status_code = 200
        return response

# will be removed once FF is updated
@app.route('/latest/{environ}/{checks}', methods=['GET', 'OPTIONS'])
def get_latest_checks(environ, checks):
    response = init_response(app.current_request, environ)
    if app.current_request.method == 'OPTIONS':
        return response
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
        return response
    results, did_check = perform_get_latest(connection, checks)
    response.body = {
        'status': 'success',
        'environment': environ,
        'checks': results,
        'checks_found': did_check
    }
    response.status_code = 200
    return response


@app.route('/checks/{environ}/{check}', methods=['GET'])
def get_check(environ, check):
    """
    Get a check result that isn't necessarily defined within foursight.
    Check name must be a valid check (not 'all', 'daily', or 'rate')
    """
    response = Response('Foursight get_check')
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
        return response
    TempCheck = CheckResult(connection.s3connection, check)
    latest_res = TempCheck.get_latest_check()
    if latest_res:
        response.body = {
            'status': 'success',
            'checks': latest_res,
            'checks_found': check,
            'environment': environ
        }
        response.status_code = 200
    else:
        response.body = {
            'status': 'error',
            'checks': {},
            'description': ''.join(['Could not get results for: ', check,'. Maybe no such check result exists?']),
            'environment': environ
        }
        response.status_code = 400
    return response


@app.route('/checks/{environ}/{check}', methods=['PUT'])
def put_check(environ, check):
    """
    Take a PUT request. Body of the request should be a json object with keys
    corresponding to the fields in CheckResult, namely:
    title, status, description, brief_output, full_output.

    Please note: at this time, a check should be initialized in CheckSuite
    for every tests to be supported with the run_foursight function.
    """
    response = Response('Foursight put_check')
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
        return response
    request = app.current_request
    put_data = request.json_body
    if not isinstance(put_data, dict):
        response.body = {
            'status': 'error',
            'endpoint': 'put_check',
            'check': check,
            'description': ' '.join(['PUT request is malformed:', put_data]),
            'environment': environ
        }
        response.status_code = 400
        return response
    ##### Maybe add this back in if we decide to go all-in on deocorators
    # valid_methods, CheckSuite = init_check_suite('all', connection)
    # valid_checks = [method.__name__ for method in valid_methods]
    # if check not in valid_checks:
    #     response.body = {
    #         'status': 'error',
    #         'endpoint': 'put_check',
    #         'check': check,
    #         'description': ' '.join(['Could not PUT invalid check:', check]),
    #         'environment': environ
    #     }
    #     response.status_code = 400
    #     return response
    #####
    checkSuite = CheckSuite(connection)
    putCheck = checkSuite.init_check(check)
    # set valid fields from the PUT body. should this be dynamic?
    # if status is not included, it will be set to ERROR
    for field in ['title', 'status', 'description', 'brief_output', 'full_output']:
        put_content = put_data.get(field)
        if put_content:
            setattr(putCheck, field, put_content)
    stored = putCheck.store_result()
    response.body = {
        'status': 'success',
        'endpoint': 'put_check',
        'check': check,
        'updated_content': stored,
        'environment': environ
    }
    response.status_code = 200
    return response


@app.route('/environments/{environ}', methods=['PUT'])
def put_environment(environ):
    """
    Take a PUT request that has a json payload with 'fourfront' (ff server)
    and 'es' (es server).
    If environ == local, you may provide a 'local_server' in the request body
    for CORS compatibility.
    Attempts to generate an new environment and runs all checks initially
    if successful.
    """
    request = app.current_request
    env_data = request.json_body
    if isinstance(env_data, dict) and {'fourfront', 'es'} <= set(env_data):
        ff_address = env_data['fourfront'] if env_data['fourfront'].endswith('/') else env_data['fourfront'] + '/'
        es_address = env_data['es'] if env_data['es'].endswith('/') else env_data['es'] + '/'
        env_entry = {
            'fourfront': ff_address,
            'es': es_address
        }
        if 'local_server' in env_data and environ == 'local':
            env_entry['local_server'] = env_data['local_server']
        s3connection = S3Connection('foursight-envs')
        s3connection.put_object(environ, json.dumps(env_entry))
        s3_bucket = ''.join(['foursight-', STAGE, '-', environ])
        bucket_res = s3connection.create_bucket(s3_bucket)
        if not bucket_res:
            return Response(
                body = {
                    'status': 'error',
                    'description': ' '.join(['Could not create bucket:', s3_bucket]),
                    'environment': environ
                },
                status_code = 500
            )
        # run some checks on the new env
        connection, error_res = init_connection(environ)
        if connection and connection.is_up:
            did_run = perform_run_checks(connection, 'all')
        else:
            did_run = []
        return Response(
            body = {
                'status': 'success',
                'description': ' '.join(['Succesfully made:', environ]),
                'initial_checks_run': did_run,
                'environment': environ
            },
            status_code = 200
        )
    else:
        return Response(
            body = {
                'status': 'error',
                'description': 'Environment creation failed',
                'body': env_data,
                'environment': environ
            },
            status_code = 400
        )


@app.route('/environments/{environ}', methods=['GET'])
def get_environment(environ):
    """
    Return config information about a given environment, or throw an error
    if it is not valid.
    """
    init_environments()
    if environ in ENVIRONMENTS:
        return Response(
            body = {
                'status': 'success',
                'details': ENVIRONMENTS[environ],
                'environment': environ
            },
            status_code = 200
        )
    else:
        return Response(
            body = {
                'status': 'error',
                'description': 'invalid environment provided. Should be one of: %s' % (str(list(ENVIRONMENTS.keys()))),
                'environment': environ
            },
            status_code = 400
        )


### SCHEDULED FXNS ###

# run at 10 am UTC every day
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def daily_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        if environ == 'local':
            continue
        connection, error_res = init_connection(environ)
        if connection and connection.is_up:
            perform_run_checks(connection, 'daily')


# run every 2 hrs
@app.schedule(Rate(2, unit=Rate.HOURS))
def two_hour_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        if environ == 'local':
            continue
        connection, error_res = init_connection(environ)
        if connection and connection.is_up:
            perform_run_checks(connection, 'item_counts_by_type,indexing_progress')


### NON-STANDARD ENDPOINTS ###


@app.route('/all_latest/{checks}', methods=['GET'])
def all_environs_latest_checks(checks):
    """
    Returns "latest" results for given checks for all foursight environments.
    The checks parameter works the same as it does for get_latest_checks.
    Returned json will have keys for each environment.
    Is NOT CORS enabled.
    """
    init_environments()
    total_checks = {}
    for environ in ENVIRONMENTS:
        if environ == 'local':
            continue
        connection, error_res = init_connection(environ)
        if connection and connection.is_up:
            results, did_check = perform_get_latest(connection, checks)
            total_checks[environ] = {
                'status': 'success',
                'environment': environ,
                'checks': results,
                'checks_found': did_check
            }
    return Response(
        body = {
            'status': 'success',
            'description': 'Latest check results for all foursight environments',
            'environments': total_checks
        },
        status_code = 200
    )


@app.route('/cleanup/{environ}', methods=['GET', 'OPTIONS'])
def cleanup(environ):
    """
    For a given environment, remove all tests records from S3 that are no
    long being used (i.e. not currently defined within checksuite).
    Will not remove auth.
    CORS enabled.
    """
    response = init_response(app.current_request, environ)
    if app.current_request.method == 'OPTIONS':
        return response
    connection, error_res = init_connection(environ)
    if connection is None:
        response.body = error_res
        response.status_code = 400
        return response
    all_keys = set(connection.s3connection.list_all_keys())
    # never delete these keys
    if 'auth' in all_keys:
        all_keys.remove('auth')
    check_methods, _ = init_check_suite('all', connection)
    for method in check_methods:
        name = method.__name__
        # remove all keys with prefix equal to this method name
        method_keys = set(connection.s3connection.list_keys_w_prefix(name))
        all_keys = all_keys - method_keys
    if len(all_keys) > 0:
        connection.s3connection.delete_keys(list(all_keys))
    response.body = {
        'status': 'success',
        'environment': environ,
        'number_cleaned': ' '.join([str(len(all_keys)), 'items']),
        'keys_cleaned': list(all_keys)
    }
    response.status_code = 200
    return response


# this route is purposefully un-authorized
@app.route('/introspect', methods=['GET'])
def introspect():
    return json.dumps(app.current_request.to_dict())
