from __future__ import print_function, unicode_literals
from chalice import Chalice, Cron, Rate, Response
import json
import os
import requests
import datetime
import boto3
from chalicelib.app_utils import *

app = Chalice(app_name='foursight')
app.debug = True

######### SCHEDULED FXNS #########

# run at 10 am UTC every day
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def daily_checks(event):
    environments = list_environments()
    for environ in environments:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'daily_checks')


# run every 6 hrs
@app.schedule(Rate(6, unit=Rate.HOURS))
def six_hour_checks(event):
    environments = list_environments()
    for environ in environments:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'six_hour_checks')


# run every 2 hrs
@app.schedule(Rate(2, unit=Rate.HOURS))
def two_hour_checks(event):
    environments = list_environments()
    for environ in environments:
        connection, error_res = init_connection(environ)
        if connection:
            run_check_group(connection, 'two_hour_checks')


def queue_check_group(environ, check_group):
    check_vals = CHECK_GROUPS.get(check_group)
    if not check_vals:
        print('%s is not a valid check group. Cannot queue it.' % (check_group))
        return
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    except:
        queue = sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '300',
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'true'
            }
        )
    # append environ as first element to all check_vals
    proc_vals = [[environ] + val for val in check_vals]
    # uuid used as the MessageGroupId
    uuid = datetime.datetime.utcnow().isoformat()
    for val in proc_vals:
        response = queue.send_message(
            MessageBody=json.dumps(val),
            MessageGroupId=uuid
        )
    invoke_lambda_runner(queue.url)
    return queue.url # for testing purposes


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
        status_code=302,
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
        return forbidden_response()


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
    req_dict = app.current_request.to_dict()
    domain = req_dict.get('headers', {}).get('host', "")
    return view_foursight(environ, check_authorization(req_dict), domain)


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

######### PURE LAMBDA FUNCTIONS AND UTILS #########

### NEED TO ADD SOME FINISHED-CHECK MONITORING FOR HANDLING DEPENDENCIES

def invoke_lambda_runner(url):
    client = boto3.client('lambda')
    # InvocationType=Event makes asynchronous
    response = client.invoke(
        FunctionName=RUNNER_NAME,
        InvocationType='Event',
        Payload=json.dumps(url)
    )
    # expect http status code to be 202 for 'Event'
    return response


@app.lambda_function()
def check_runner(event, context):
    print(event)
    if not event:
        return
    # ValueError: No JSON object could be decoded
    # ^^ error or json.loads(event)
    queue_url = json.loads(event)
    if not queue_url or not isinstance(queue_url, basestring):
        # bail
        return
    # do nothing with event as this point, which is built from Payload param in invoke()
    # first, ReceiveMessage from the top of the SQS; this should be a check str
    client = boto3.client('sqs')
    response = client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['MessageGroupId'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=300
    )
    message = response.get('Messages', [{'Body': None}])[0]
    body = message.get('Body')
    if not body:
        delete_message_and_deploy(client, queue_url, message.get('ReceiptHandle'))
        return
    # if not a valid check str, remove the item from the SQS
    check_list = json.loads(body)
    if not isinstance(check_list, list) or len(check_list) != 4:
        delete_message_and_deploy(client, queue_url, message.get('ReceiptHandle'))
        return
    # next, run the check using the check str
    # message visibility will timeout after 300 seconds if this fails and be
    # available from the queue
    connection, error_res = init_connection(check_list[0])
    if connection:
        run_check(connection, check_list[1], check_list[2])
        delete_message_and_deploy(client, queue_url, message.get('ReceiptHandle'))
    else:
        invoke_lambda_runner(queue_url)


def delete_message_and_deploy(client, queue_url, receipt):
    client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt
    )
    invoke_lambda_runner(queue_url)
