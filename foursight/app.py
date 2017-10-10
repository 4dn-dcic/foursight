from __future__ import print_function, unicode_literals
from chalice import Chalice, BadRequestError, NotFoundError
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


CACHED = {}
SERVER_INFO = {
    'webprod': {
        'server': 'https://data.4dnucleome.org/',
        'bucket': 'foursight-webprod',
        'es': 'https://search-fourfront-webprod-hmrrlalm4ifyhl4bzbvl73hwv4.us-east-1.es.amazonaws.com/'
    }
}


def init_connection(environ, supplied_connection):
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
                'checks_run': {}
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
                'checks_run': {}
            }
            return None, error_res
    else:
        connection = supplied_connection
    return connection, error_res


@app.route('/')
def index():
    return {'foursight': 'insight into fourfront'}


@app.route('/run/{environ}/{checks}')
def run_checks(environ, checks, supplied_connection=None):
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
        's3_info': connection.s3connection.head_info
    })


@app.route('/latest/{environ}/{checks}')
def get_latest_checks(environ, checks, supplied_connection=None):
    connection, error_res = init_connection(environ, supplied_connection)
    if connection is None:
        return error_res
    testSuite = CheckSuite(connection)
    decoMethods = get_methods_by_deco(CheckSuite, run_check)
    results = {}
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
        results[name] = TempCheck.get_latest()
        did_check.append(name)

    return json.dumps({
        'checks': results,
        'checks_found': did_check,
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
    return app.current_request.to_dict()
