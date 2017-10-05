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

# this is messy
alias = 'webprod'
info = SERVER_INFO[alias]
CACHED[alias] = FFConnection(info['server'], info['bucket'], info['es'])

@app.route('/')
def index():
    return {'foursight': 'insight into fourfront'}


@app.route('/run_checks/{checks}')
def test_FF_connection(checks):
    connection = CACHED['webprod']
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
        'status': connection.is_up,
        'checks_runs': did_run,
        'results_stored_as': s3_key,
        'latest_run': connection.latest_run,
        's3_info': connection.s3connection.head_info
    }


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
