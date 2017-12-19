from __future__ import print_function, unicode_literals
from chalice import Chalice, Cron, Rate, Response
import json
import os
import requests
import datetime
from chalicelib.app_utils import *

app = Chalice(app_name='foursight')
app.debug = True

######### SCHEDULED FXNS #########

# run at 10 am UTC every day
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def daily_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'daily_checks')


# run every 2 hrs
@app.schedule(Rate(2, unit=Rate.HOURS))
def two_hour_checks(event):
    init_environments()
    for environ in ENVIRONMENTS:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'two_hour_checks')

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
    # resp_headers = {}
    domain = req_dict.get('headers', {}).get('host')
    auth0_code = req_dict.get('query_params', {}).get('code', None)
    auth0_client = os.environ.get('CLIENT_ID', None)
    auth0_secret = os.environ.get('CLIENT_SECRET', None)
    if not (domain and auth0_code and auth0_client and auth0_secret):
        return Response(
            status_code=301,
            body=json.dumps(resp_headers),
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
    return Response(
        status_code=301,
        body=json.dumps(resp_headers),
        headers=resp_headers)


@app.route('/', methods=['GET'])
def index():
    """
    Test route
    """
    return json.dumps({'foursight': 'insight into fourfront'})


@app.route('/introspect', methods=['GET'])
def introspect():
    """
    Test route
    """
    auth = check_authorization(app.current_request.to_dict())
    if auth:
        return Response(
            status_code=200,
            body=json.dumps(app.current_request.to_dict())
            )
    else:
        return Response(
            status_code=403,
            body=json.dumps(app.current_request.to_dict())
            )


@app.route('/view/{environ}/{check}', methods=['GET'])
def view_rerun_route(environ, check):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        return view_rerun(environ, check)
    else:
        return forbidden_response()


@app.route('/view/{environ}', methods=['GET'])
def view_route(environ):
    """
    Non-protected route
    """
    return view_foursight(environ, check_authorization(app.current_request.to_dict()))


@app.route('/run/{environ}/{check_group}', methods=['PUT', 'GET'])
def run_route(environ, check_group):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        if app.current_request.method == 'PUT':
            return run_foursight_checks(environ, check_group)
        elif app.current_request.method == 'GET':
            return get_foursight_checks(environ, check_group)
    else:
        return forbidden_response()


@app.route('/checks/{environ}/{check}', methods=['GET'])
def get_check_route(environ, check):
    """
    Protected route
    """
    if check_authorization(app.current_request.to_dict()):
        return get_check(environ, check)
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
        return get_environment(environ)
    else:
        return forbidden_response()


# @app.route('/favicon.ico')
# def return_favicon():
#     """
#     Unprotected route to return favicon
#     """
