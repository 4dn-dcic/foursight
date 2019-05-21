from __future__ import print_function, unicode_literals
from chalice import Chalice, Cron, Rate, Response
import json
import os
import requests
import datetime
from chalicelib.app_utils import *

app = Chalice(app_name='foursight')
app.debug = True
STAGE = os.environ.get('chalice_stage', 'dev')
DEFAULT_ENV = 'data'

######### SCHEDULED FXNS #########

# this dictionary defines the CRON schedules for the dev and prod foursight
# stagger them to reduce the load on Fourfront. Times are UTC
# info: https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
# TODO: remove hardcoding of stage
foursight_cron_by_schedule = {
    'prod': {
        'ten_min_checks': Cron('0/10', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('0/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('0', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('15', '0/1', '*', '*', '?', '*'),
        'morning_checks': Cron('0', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('15', '10', '*', '*', '?', '*'),
        'monthly_checks': Cron('0', '9', '1', '*', '?', '*')
    },
    'dev': {
        'ten_min_checks': Cron('5/10', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('15/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('30', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('45', '0/1', '*', '*', '?', '*'),
        'morning_checks': Cron('30', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('45', '10', '*', '*', '?', '*'),
        'monthly_checks': Cron('30', '9', '1', '*', '?', '*')
    }
}

@app.schedule(foursight_cron_by_schedule[STAGE]['ten_min_checks'])
def ten_min_checks(event):
    queue_scheduled_checks('all', 'ten_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['thirty_min_checks'])
def thirty_min_checks(event):
    queue_scheduled_checks('all','thirty_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks'])
def hourly_checks(event):
    queue_scheduled_checks('all', 'hourly_checks')

@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks_2'])
def hourly_checks_2(event):
    queue_scheduled_checks('all', 'hourly_checks_2')

@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks'])
def morning_checks(event):
    queue_scheduled_checks('all', 'morning_checks')

@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks_2'])
def morning_checks_2(event):
    queue_scheduled_checks('all', 'morning_checks_2')

@app.schedule(foursight_cron_by_schedule[STAGE]['monthly_checks'])
def monthly_checks(event):
    queue_scheduled_checks('all', 'monthly_checks')


######### END SCHEDULED FXNS #########

@app.route('/callback')
def auth0_callback():
    """
    Special callback route, only to be used as a callback from auth0
    Will return a redirect to view on error/any missing callback info.
    """
    request = app.current_request
    req_dict = request.to_dict()
    resp_headers = {'Location': '/api/view/data,staging'}
    domain = req_dict.get('headers', {}).get('host')
    params = req_dict.get('query_params')
    if not params:
        return forbidden_response()
    auth0_code = params.get('code', None)
    auth0_client = os.environ.get('CLIENT_ID', None)
    auth0_secret = os.environ.get('CLIENT_SECRET', None)
    if not (domain and auth0_code and auth0_client and auth0_secret):
        return Response(status_code=301, body=json.dumps(resp_headers),
                        headers=resp_headers)
    payload = {
        'grant_type': 'authorization_code',
        'client_id': auth0_client,
        'client_secret': auth0_secret,
        'code': auth0_code,
        'redirect_uri': ''.join(['https://', domain, '/api/callback/'])
    }
    json_payload = json.dumps(payload)
    headers = { 'content-type': "application/json" }
    res = requests.post("https://hms-dbmi.auth0.com/oauth/token", data=json_payload, headers=headers)
    id_token = res.json().get('id_token', None)
    if id_token:
        cookie_str = ''.join(['jwtToken=', id_token, '; Domain=', domain, '; Path=/;'])
        expires_in = res.json().get('expires_in', None)
        if expires_in:
            expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)
            cookie_str +=  (' Expires=' + expires.strftime("%a, %d %b %Y %H:%M:%S GMT") + ';')
        resp_headers['Set-Cookie'] = cookie_str
    return Response(status_code=302, body=json.dumps(resp_headers), headers=resp_headers)


@app.route('/', methods=['GET'])
def index():
    """
    Redirect with 302 to /api/view/data
    Non-protected route
    """
    resp_headers = {'Location': '/api/view/data'}
    return Response(status_code=302, body=json.dumps(resp_headers),
                    headers=resp_headers)


@app.route('/introspect', methods=['GET'])
def introspect():
    """
    Test route
    """
    auth = check_authorization(app.current_request.to_dict())
    if auth:
        return Response(status_code=200, body=json.dumps(app.current_request.to_dict()))
    else:
        return forbidden_response()


@app.route('/view_run/{environ}/{check}/{method}', methods=['GET'])
def view_run_route(environ, check, method):
    """
    Protected route
    """
    req_dict = app.current_request.to_dict()
    query_params = req_dict.get('query_params', {})
    if check_authorization(req_dict):
        if method == 'action':
            return view_run_action(environ, check, query_params)
        else:
            return view_run_check(environ, check, query_params)
    else:
        return forbidden_response()


@app.route('/view/{environ}', methods=['GET'])
def view_route(environ):
    """
    Non-protected route
    """
    req_dict = app.current_request.to_dict()
    domain = req_dict.get('headers', {}).get('host', "")
    return view_foursight(environ, check_authorization(req_dict), domain)


@app.route('/view/{environ}/{check}/{uuid}', methods=['GET'])
def view_check_route(environ, check, uuid):
    """
    Protected route
    """
    req_dict = app.current_request.to_dict()
    domain = req_dict.get('headers', {}).get('host', "")
    if check_authorization(req_dict):
        return view_foursight_check(environ, check, uuid, True, domain)
    else:
        return forbidden_response()


@app.route('/history/{environ}/{check}', methods=['GET'])
def history_route(environ, check):
    """
    Non-protected route
    """
    # get some query params
    req_dict = app.current_request.to_dict()
    query_params = req_dict.get('query_params')
    start = int(query_params.get('start', '0')) if query_params else 0
    limit = int(query_params.get('limit', '25')) if query_params else 25
    domain = req_dict.get('headers', {}).get('host', "")
    return view_foursight_history(environ, check, start, limit, check_authorization(req_dict), domain)


@app.route('/checks/{environ}/{check}/{uuid}', methods=['GET'])
def get_check_with_uuid_route(environ, check, uuid):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        return run_get_check(environ, check, uuid)
    else:
        return forbidden_response()


@app.route('/checks/{environ}/{check}', methods=['GET'])
def get_check_route(environ, check):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        return run_get_check(environ, check, None)
    else:
        return forbidden_response()


@app.route('/checks/{environ}/{check}', methods=['PUT'])
def put_check_route(environ, check):
    """
    Take a PUT request. Body of the request should be a json object with keys
    corresponding to the fields in CheckResult, namely:
    title, status, description, brief_output, full_output, uuid.
    If uuid is provided and a previous check is found, the default
    behavior is to append brief_output and full_output.

    Protected route
    """
    request = app.current_request
    if check_authorization(request.to_dict()):
        put_data = request.json_body
        return run_put_check(environ, check, put_data)
    else:
        return forbidden_response()


@app.route('/environments/{environ}', methods=['PUT'])
def put_environment(environ):
    """
    Take a PUT request that has a json payload with 'fourfront' (ff server)
    and 'es' (es server).
    Attempts to generate an new environment and runs all checks initially
    if successful.

    Protected route
    """
    request = app.current_request
    if check_authorization(request.to_dict()):
        env_data = request.json_body
        return run_put_environment(environ, env_data)
    else:
        return forbidden_response()


@app.route('/environments/{environ}', methods=['GET'])
def get_environment_route(environ):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        return run_get_environment(environ)
    else:
        return forbidden_response()

######### PURE LAMBDA FUNCTIONS #########

@app.lambda_function()
def check_runner(event, context):
    """
    Pure lambda function to pull run and check information from SQS and run
    the checks. Self propogates. event is a dict of information passed into
    the lambda at invocation time.
    """
    if not event:
        return
    run_check_runner(event)

######### MISC UTILITY FUNCTIONS #########


def set_stage(stage):
    from deploy import CONFIG_BASE
    if stage != 'test' and stage not in CONFIG_BASE['stages']:
        print('ERROR! Input stage is not valid. Must be one of: %s' % str(list(CONFIG_BASE['stages'].keys()).extend('test')))
    os.environ['chalice_stage'] = stage


def set_timeout(timeout):
    from chalicelib import utils
    try:
        timeout = int(timeout)
    except ValueError:
        print('ERROR! Timeout must be an integer. You gave: %s' % timeout)
    else:
        utils.CHECK_TIMEOUT = timeout
