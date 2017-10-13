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


# should we base origin checks solely off of SERVER_INFO?
# could compare request.headers.origin to SERVER_INFO[environ]['server']
ALLOWED_ORIGINS = [
    'http://localhost:8000/',
    'https://data.4dnucleome.org/',
    'http://fourfront-webdev.us-east-1.elasticbeanstalk.com/'
]

CACHED = {}
SERVER_INFO = {
    'webprod': {
        'server': 'https://data.4dnucleome.org/',
        'bucket': 'foursight-webprod',
        'es': 'https://search-fourfront-webprod-hmrrlalm4ifyhl4bzbvl73hwv4.us-east-1.es.amazonaws.com/'
    }
}

foursight_cors = CORSConfig(
    allow_origin = '*',
    allow_headers = ['Authorization',
                     'Content-Type',
                     'X-Amz-Date',
                     'X-Amz-Security-Token',
                     'X-Api-Key',
                     'X-Requested-With']
)

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
        if environ not in SERVER_INFO:
            error_res = {
                'status': 'error',
                'description': 'invalid environment provided. Should be one of: %s' % (str(list(SERVER_INFO.keys()))),
                'checks': {}
            }
            return None, error_res
        try:
            connection = CACHED[environ]
        except KeyError:
            info = SERVER_INFO[environ]
            CACHED[environ] = FFConnection(info['server'], info['bucket'], info['es'])
            connection = CACHED[environ]
        if not connection.is_up:
            error_res = {
                'status': 'error',
                'description': 'The connection to fourfront is down.',
                'checks': {}
            }
            return None, error_res
    else:
        connection = supplied_connection
    return connection, error_res


def check_origin(current_request):
    """
    Returns None if origin passes (in ALLOWED_ORIGINS)
    """
    req_headers = current_request.headers
    if req_headers and getattr(req_headers, 'origin', None) is not None:
        if req_headers.origin and req_headers.origin not in ALLOWED_ORIGINS:
            return json.dumps({
                'status': 'error',
                'description': 'CORS check failed.',
                'checks': {},
                'request': current_request.to_dict()
            })
    return None


@app.route('/')
def index():
    return json.dumps({'foursight': 'insight into fourfront'})


@app.route('/run/{environ}/{checks}', cors=foursight_cors)
def run_checks(environ, checks, supplied_connection=None):
    origin_flag = check_origin(app.current_request)
    if origin_flag:
        return origin_flag
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    testSuite = CheckSuite(connection)
    decoMethods = get_methods_by_deco(CheckSuite, run_check)
    to_run = []
    did_run = []
    if checks != 'all':
        to_run = checks.split(',')
    for method in decoMethods:
        name = method.__name__
        if to_run and name not in to_run:
            continue
        method(testSuite)
        did_run.append(name)

    return json.dumps({
        'checks_runs': did_run,
        'checks': {},
        's3_info': connection.s3connection.head_info
    })


@app.route('/latest/{environ}/{checks}', cors=foursight_cors)
def get_latest_checks(environ, checks, supplied_connection=None):
    origin_flag = check_origin(app.current_request)
    if origin_flag:
        return origin_flag
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    testSuite = CheckSuite(connection)
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
def cleanup(environ, supplied_connection=None):
    origin_flag = check_origin(app.current_request)
    if origin_flag:
        return origin_flag
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return json.dumps(error_res)
    testSuite = CheckSuite(connection)
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
    for environ in SERVER_INFO:
        run_checks(environ, 'all')


# run every 2 hrs
@app.schedule(Rate(2, unit=Rate.HOURS))
def two_hour_checks(event):
    for environ in SERVER_INFO:
        run_checks(environ, 'item_counts_by_type,indexing_progress')
