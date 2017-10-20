from __future__ import print_function, unicode_literals
from chalice import Chalice, BadRequestError, NotFoundError, CORSConfig, Cron, Rate
import json
from chalicelib.ff_connection import FFConnection
from chalicelib.checksuite import CheckSuite, run_check
from chalicelib.checkresult import CheckResult
from chalicelib.utils import get_methods_by_deco
from chalicelib.s3_connection import S3Connection
import boto3
from botocore.exceptions import ClientError

app = Chalice(app_name='foursight')
app.debug = True

ENVIRONMENTS = {}
CACHED = {}

foursight_cors = CORSConfig(
    allow_origin = '*',
    allow_headers = ['Authorization',
                     'Content-Type',
                     'X-Amz-Date',
                     'X-Amz-Security-Token',
                     'X-Api-Key',
                     'X-Requested-With']
)


def init_environments(env='all'):
    """
    Generate environments for ENVIRONMENTS global variable by reading
    the foursight-dev bucket.
    Returns a list of environments/keys that are not valid.
    """
    s3connection = S3Connection('foursight-dev')
    env_keys = s3connection.list_all_keys()
    bad_keys = []
    if env != 'all':
        if env in env_keys:
            env_keys = [env]
        else:
            return [env]
    else: # reset ENVIRONMENTS if we're trying to init all
        ENVIRONMENTS = {}
    for env_key in env_keys:
        env_res = json.loads(s3connection.get_object(env_key))
        # check that the keys we need are in the object
        if isinstance(env_res, dict) and {'fourfront', 'es'} <= set(env_res):
            env_entry = {
                'fourfront': env_res['fourfront'],
                'es': env_res['es'],
                'bucket': 'foursight-' + env_key
            }
            if 'local_server' in env_res:
                env_entry['local_server'] = env_res['local_server']
            ENVIRONMENTS[env_key] = env_entry
        else:
            bad_keys.append(env_key)
    return bad_keys


def init_connection(environ, supplied_connection=None):
    """
    Initialize the fourfront/s3 connection using the FFConnection object
    and the given environment. The supplied_connection argument is used for
    testing and a FFConnection object can be supplied directly to it,
    which will bypass the rest of the function.
    Returns an FFConnection object (or None if error) and a dictionary
    error response.
    """
    error_res = {}
    if supplied_connection is None:
        if environ not in ENVIRONMENTS:
            bad_keys = init_environments()
            if environ in bad_keys:
                error_res = {
                    'status': 'error',
                    'description': 'invalid environment provided. Should be one of: %s' % (str(list(ENVIRONMENTS.keys()))),
                    'checks': {}
                }
            return None, error_res
        try:
            connection = CACHED[environ]
        except KeyError:
            info = ENVIRONMENTS[environ]
            CACHED[environ] = FFConnection(info['fourfront'], info['bucket'], info['es'])
            connection = CACHED[environ]
        if not connection.is_up:
            error_res = {
                'status': 'error',
                'description': 'The connection to fourfront is down',
                'checks': {}
            }
            return None, error_res
    else:
        connection = supplied_connection
    return connection, error_res


def check_origin(current_request, environ):
    """
    Returns None if origin passes
    """
    allowed_origins = []
    env_ff_server = ENVIRONMENTS.get(environ, {}).get('fourfront')
    if env_ff_server:
        allowed_origins.append(env_ff_server)
    # special case for test server
    if environ == 'local':
        local_ff_server = ENVIRONMENTS.get('local', {}).get('local_server')
        if local_ff_server is not None:
            allowed_origins.append(local_ff_server)
    req_headers = current_request.headers
    if req_headers and getattr(req_headers, 'origin', None) is not None:
        if req_headers.origin and req_headers.origin not in allowed_origins:
            return json.dumps({
                'status': 'error',
                'description': 'CORS check failed.',
                'checks': {},
                'request_environ': environ,
                'request': current_request.to_dict()
            })
    return None


@app.route('/')
def index():
    """
    Test route
    """
    return json.dumps({'foursight': 'insight into fourfront'})


@app.route('/run/{environ}/{checks}', cors=foursight_cors)
def run_checks(environ, checks, supplied_connection=None, scheduled=False):
    """
    Run the given checks on the given environment, creating a record in the
    corresponding S3 bucket under the check's method name. If checks == 'all',
    then every check in checksuite will be run. The latest run of checks
    replaces the 'latest' label for each check directory in S3 and also
    creates a timestamped record.
    CORS enabled.
    """
    # skip origin checks for scheduled jobs
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    if not scheduled:
        origin_flag = check_origin(app.current_request, environ)
        if origin_flag:
            return origin_flag
    checkSuite = CheckSuite(connection)
    decoMethods = get_methods_by_deco(CheckSuite, run_check)
    to_run = []
    did_run = []
    if checks != 'all':
        to_run = checks.split(',')
    for method in decoMethods:
        name = method.__name__
        if to_run and name not in to_run:
            continue
        method(checkSuite)
        did_run.append(name)

    return json.dumps({
        'checks_runs': did_run,
        'checks': {},
        's3_info': connection.s3connection.head_info
    })


@app.route('/latest/{environ}/{checks}', cors=foursight_cors)
def get_latest_checks(environ, checks, supplied_connection=None, scheduled=False):
    """
    Return JSON of each check tagged with the "latest" tag for speicified current
    checks in checksuite for the given environment. If checks == 'all', every
    registered check will be returned. Otherwise, send a comma separated list
    of check names (the method names!) as the check argument.
    CORS enabled.
    """
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    if not scheduled:
        origin_flag = check_origin(app.current_request, environ)
        if origin_flag:
            return origin_flag
    checkSuite = CheckSuite(connection)
    decoMethods = get_methods_by_deco(CheckSuite, run_check)
    results = []
    to_check = []
    did_check = []
    if checks != 'all':
        to_check = checks.split(',')
    for method in decoMethods:
        name = method.__name__
        if to_check and name not in to_check:
            continue
        # the CheckResult below is used solely to collect the latest check
        TempCheck = CheckResult(connection.s3connection, name)
        results.append(TempCheck.get_latest_check())
        did_check.append(name)

    return json.dumps({
        'checks': results,
        'checks_found': did_check,
        's3_info': connection.s3connection.head_info
    })


@app.route('/cleanup/{environ}', cors=foursight_cors)
def cleanup(environ, supplied_connection=None, scheduled=False):
    """
    For a given environment, remove all tests records from S3 that are no
    long being used (i.e. not currently defined within checksuite).
    Will not remove auth.
    CORS enabled.
    """
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    if not scheduled:
        origin_flag = check_origin(app.current_request, environ)
        if origin_flag:
            return origin_flag
    checkSuite = CheckSuite(connection)
    decoMethods = get_methods_by_deco(CheckSuite, run_check)
    all_keys = set(connection.s3connection.list_all_keys())
    # never delete these keys
    all_keys.remove('auth')
    for method in decoMethods:
        name = method.__name__
        # remove all keys with prefix equal to this method name
        method_keys = set(connection.s3connection.list_keys_w_prefix(name))
        all_keys = all_keys - method_keys
    if len(all_keys) > 0:
        connection.s3connection.delete_keys(list(all_keys))
    return json.dumps({
        'cleaned': ' '.join([str(len(all_keys)), 'items']),
        'checks': {},
        's3_info': connection.s3connection.head_info
    })


@app.route('/build_env/{environ}', methods=['PUT'])
def build_environment(environ):
    """
    Take a PUT request that has a json payload with 'fourfront' (ff server),
    'es' (es server), and optionally, 'bucket' (s3 bucket name).
    Attempts to generate an new environment and runs all checks initially
    if successful.
    """
    request = app.current_request
    env_data = request.json_body
    if isinstance(env_data, dict) and {'fourfront', 'es'} <= set(env_data):
        env_entry = {
            'fourfront': env_data['fourfront'],
            'es': env_data['es'],
            'bucket': 'foursight-' + environ
        }
        if 'local_server' in env_data:
            env_entry['local_server'] = env_data['local_server']
        s3connection = S3Connection('foursight-dev')
        s3connection.put_object(environ, json.dumps(env_entry))
        # do a quick update
        ENVIRONMENTS[environ] = env_entry
        # run some checks on the new env
        checks_run_json = run_checks(environ, 'all', scheduled=True)
        return json.dumps({
            'status': 'success',
            'description': ' '.join(['Succesfully made:', environ]),
            'initial_checks_run': json.loads(checks_run_json),
            's3': s3connection.head_info
        })
    else:
        return json.dumps({
            'status': 'error',
            'description': ' '.join(['Could not make environment:', envrion]),
            's3': s3connection.head_info
        })


@app.route('/test_s3/{bucket_name}')
def test_s3_connection(bucket_name):
    s3Connection = S3Connection(bucket_name)
    return json.dumps({
        'bucket': s3Connection.bucket,
        'region': s3Connection.location,
        'status': s3Connection.status_code,
        'info': s3Connection.head_info
    })


@app.route('/introspect')
def introspect():
    return json.dumps(app.current_request.to_dict())

### SCHEDULED FXNS ###

# run at 10 am UTC every day
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def daily_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        if environ == 'local':
            continue
        run_checks(environ, 'all', scheduled=True)


# run every 2 hrs
@app.schedule(Rate(2, unit=Rate.HOURS))
def two_hour_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        if environ == 'local':
            continue
        run_checks(environ, 'item_counts_by_type,indexing_progress', scheduled=True)
