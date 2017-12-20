from __future__ import print_function, unicode_literals
from chalice import Response
from jinja2 import Environment, FileSystemLoader, select_autoescape
import json
import os
import jwt
import datetime
from dateutil import tz
from base64 import b64decode
from fs_connection import FSConnection
from check_utils import run_check_group, get_check_group_latest, run_check, get_check_strings
from checkresult import CheckResult
from s3_connection import S3Connection
from check_groups import CHECK_GROUPS

jin_env = Environment(
    loader=FileSystemLoader('chalicelib/templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

ENVIRONMENTS = {}
# set environmental variables in .chalice/config.json
STAGE = os.environ.get('chalice_stage', 'dev') # default to dev
ADMIN = "4dndcic@gmail.com"

# compare strings in both python 2 and python 3
# in other files, compare with utils.basestring
try:
    basestring = basestring
except NameError:
    basestring = str


def init_environments(env='all'):
    """
    Generate environments for ENVIRONMENTS global variable by reading
    the foursight-dev bucket.
    Returns a list of environments/keys that are not valid.
    """
    s3_connection = S3Connection('foursight-envs')
    env_keys = s3_connection.list_all_keys()
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
        env_res = json.loads(s3_connection.get_object(env_key))
        # check that the keys we need are in the object
        if isinstance(env_res, dict) and {'fourfront', 'es'} <= set(env_res):
            env_entry = {
                'fourfront': env_res['fourfront'],
                'es': env_res['es'],
                'ff_env': env_res.get('ff_env', ''.join(['fourfront-', env_key])),
                'bucket': ''.join(['foursight-', STAGE, '-', env_key])
            }
            ENVIRONMENTS[env_key] = env_entry
        else:
            bad_keys.append(env_key)
    return bad_keys


def init_connection(environ):
    """
    Initialize the fourfront/s3 connection using the FSConnection object
    and the given environment.
    Returns an FSConnection object (or None if error) and a dictionary
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
    connection = FSConnection(environ, ENVIRONMENTS[environ])
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
            if payload.get('email') == ADMIN and payload.get('email_verified') is True:
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


##### ROUTE RUNNING FUNCTIONS #####


def view_rerun(environ, check):
    """
    Called from the view endpoint (or manually, I guess), this re-runs the given
    checks for the given environment (CANNOT be 'all'; too slow) and returns the
    view_foursight templated result with the new check result.

    This also be used to run a check group. This is checked before individual check names
    """
    connection, error_res = init_connection(environ)
    if connection:
        if check in CHECK_GROUPS:
            run_check_group(connection, check)
        else:
            check_str = get_check_strings(check)
            if check_str:
                run_check(connection, check_str, {})
    resp_headers = {'Location': '/api/view/' + environ}
    # redirect to view_foursight page
    return Response(
        status_code=301,
        body=json.dumps(resp_headers),
        headers=resp_headers)


def view_foursight(environ, is_admin=False):
    """
    View a template of all checks from the given environment(s).
    Environ may be 'all' or a specific FS environments separated by commas.
    With 'all', this function can be somewhat slow.
    Returns a response with html content.
    Non-protected route
    """
    html_resp = Response('Foursight viewing suite')
    html_resp.headers = {'Content-Type': 'text/html'}
    init_environments()
    total_envs = []
    view_envs = ENVIRONMENTS.keys() if environ == 'all' else [e.strip() for e in environ.split(',')]
    for this_environ in view_envs:
        connection, error_res = init_connection(this_environ)
        if connection:
            results = get_check_group_latest(connection, 'all')
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
                # change timezone to local
                from_zone = tz.tzutc()
                to_zone = tz.tzlocal()
                # this can be removed once uuid has been around long enough
                ts_utc = res['uuid'] if 'uuid' in res else res['timestamp']
                ts_utc = datetime.datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:%S.%f").replace(microsecond=0)
                ts_utc = ts_utc.replace(tzinfo=from_zone)
                ts_local = ts_utc.astimezone(to_zone)
                proc_ts = ''.join([str(ts_local.date()), ' at ', str(ts_local.time()), ' (', str(ts_local.tzname()), ')'])
                res['uuid'] = proc_ts
                if not res.get('description') and not res.get('brief_output') and not res.get('full_output') and not res.get('ff_link'):
                    res['content'] = False
                else:
                    res['content'] = True
                if res.get('brief_output'):
                    res['brief_output'] = json.dumps(res['brief_output'], indent=4)
                if res.get('full_output'):
                    res['full_output'] = json.dumps(res['full_output'], indent=4)
                # only return admin_output if an admin is logged in
                if res.get('admin_output') and is_admin:
                    res['admin_output'] = json.dumps(res['admin_output'], indent=4)
                else:
                    res['admin_output'] = None
                processed_results.append(res)
            total_envs.append({
                'status': 'success',
                'environment': this_environ,
                'checks': processed_results
            })
    # prioritize these environments
    env_order = ['data', 'staging', 'webdev', 'hotseat']
    total_envs = sorted(total_envs, key=lambda v: env_order.index(v['environment']) if v['environment'] in env_order else 9999)
    template = jin_env.get_template('template.html')
    groups = list(CHECK_GROUPS.keys()) # only the keys needed
    # get these into groups of 4
    groups_4 = [groups[i:i + 4] for i in range(0, len(groups), 4)]
    html_resp.body = template.render(envs=total_envs, groups_4=groups_4, stage=STAGE, is_admin=is_admin)
    html_resp.status_code = 200
    return html_resp


def run_foursight_checks(environ, check_group):
    """
    Run the given checks on the given environment, creating a record in the
    corresponding S3 bucket under the check's method name.
    The latest run of checks replaces the 'latest' label for each check
    directory in S3 and also creates a timestamped record.
    """
    connection, response = init_response(environ)
    if not connection:
        return response
    did_run = run_check_group(connection, check_group)
    response.body = {
        'status': 'success',
        'environment': environ,
        'check_group': check_group,
        'checks': did_run
    }
    response.status_code = 200
    return response


def get_foursight_checks(environ, check_group):
    """
    Return JSON of each check tagged with the "latest" tag for checks
    within given check_group for the given environment. If check_group == 'all', every
    registered check will be returned. Otherwise, must be a valid check_group
    name.
    """
    connection, response = init_response(environ)
    if not connection:
        return response
    results = get_check_group_latest(connection, check_group)
    response.body = {
        'status': 'success',
        'environment': environ,
        'check_group': check_group,
        'checks': results
    }
    response.status_code = 200
    return response


def get_check(environ, check):
    """
    Get a check result that isn't necessarily defined within foursight.
    """
    connection, response = init_response(environ)
    if not connection:
        return response
    TempCheck = CheckResult(connection.s3_connection, check)
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
    put_uuid = put_data.get('uuid')
    putCheck = CheckResult(connection.s3_connection, check, uuid=put_uuid)
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


def run_put_environment(environ, env_data):
    """
    Abstraction of the functionality of put_environment without the current_request
    to allow for testing.
    """
    proc_environ = environ.split('-')[-1] if environ.startswith('fourfront-') else environ
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
            return Response(
                body = {
                    'status': 'error',
                    'description': ' '.join(['Could not create bucket:', s3_bucket]),
                    'environment': proc_environ
                },
                status_code = 500
            )
        # run some checks on the new env
        connection, error_res = init_connection(proc_environ)
        did_run = run_check_group(connection, 'all') if connection else []
        return Response(
            body = {
                'status': 'success',
                'description': ' '.join(['Succesfully made:', proc_environ]),
                'initial_checks': did_run,
                'environment': proc_environ
            },
            status_code = 200
        )
    else:
        return Response(
            body = {
                'status': 'error',
                'description': 'Environment creation failed',
                'body': env_data,
                'environment': proc_environ
            },
            status_code = 400
        )


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
                'description': 'Invalid environment provided. Should be one of: %s' % (str(list(ENVIRONMENTS.keys()))),
                'environment': environ
            },
            status_code = 400
        )
