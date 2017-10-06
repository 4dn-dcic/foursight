from __future__ import print_function, unicode_literals
from chalice import Chalice, BadRequestError, NotFoundError
import json
from chalicelib.ff_connection import FFConnection
from chalicelib.ff_checks import FFCheckSuite, run_check
from chalicelib.utils import getMethodsByDecorator
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

@app.route('/')
def index():
    return {'foursight': 'insight into fourfront'}


@app.route('/foursight/{environ}/{checks}')
def run_checks(environ, checks, supplied_connection=None):
    if supplied_connection is None:
        if environ not in SERVER_INFO:
            return {
                'ERROR': 'invalid environment provided. Should be one of: %s' % (str(list(SERVER_INFO.keys()))),
                'checks_run': {}
            }
        try:
            connection = CACHED[environ]
        except KeyError:
            info = SERVER_INFO[environ]
            CACHED[environ] = FFConnection(info['server'], info['bucket'], info['es'])
            connection = CACHED[environ]
        if not connection.is_up:
            return {
                'ERROR': 'connection to FF is down',
                'checks_run': {}
            }
    else:
        connection = supplied_connection
    testSuite = FFCheckSuite(connection)
    decoMethods = getMethodsByDecorator(FFCheckSuite, run_check)
    results = {}
    to_run = []
    did_run = []
    if checks != 'all':
        to_run = checks.split(',')
    for method in decoMethods:
        name = method.__name__
        if to_run and name not in to_run:
            continue
        res = method(testSuite)
        results[name] = res
        did_run.append(name)
    if results:
        s3_key = connection.log_result(checks, results)
    else:
        s3_key = None

    return {
        'checks_runs': did_run,
        'results_stored_as': s3_key,
        'latest_run': connection.latest_run,
        's3_info': connection.s3connection.head_info
    }


@app.route('/latest_run/{environ}')
def latest_run(environ):
    if environ not in SERVER_INFO:
        return {
            'ERROR': 'invalid environment provided. Should be one of: %s' % (str(list(SERVER_INFO.keys()))),
            'checks_run': {}
        }
    try:
        connection = CACHED[environ]
    except KeyError:
        info = SERVER_INFO[environ]
        CACHED[environ] = FFConnection(info['server'], info['bucket'], info['es'])
        connection = CACHED[environ]
    if not connection.is_up:
        return {
            'ERROR': 'connection to FF is down',
            'checks_run': {}
        }
    if connection.latest_run is None:
        run_checks(environ, 'all', connection)
        return connection.get_latest_run()
    else:
        return connection.get_latest_run()


@app.route('/test_s3/{bucket_name}')
def test_s3_connection(bucket_name):
    s3Connection = S3Connection(bucket_name)
    return {
        'bucket': s3Connection.bucket,
        'region': s3Connection.location,
        'status': s3Connection.status_code,
        'info': s3Connection.head_info
    }


@app.route('/introspect')
def introspect():
    return app.current_request.to_dict()
