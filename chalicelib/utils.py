# General utils for foursight
from __future__ import print_function, unicode_literals
import types
from datetime import datetime, timedelta
from dateutil import tz
import traceback
import signal
import time
import boto3
import sys
import os
import json
from importlib import import_module
from functools import wraps, partial
from .run_result import CheckResult, ActionResult
from .s3_connection import S3Connection

# set environmental variables in .chalice/config.json
STAGE = os.environ.get('chalice_stage', 'dev') # default to dev
QUEUE_NAME = '-'.join(['foursight', STAGE, 'check_queue'])
RUNNER_NAME = '-'.join(['foursight', STAGE, 'check_runner'])
CHECK_DECO = 'check_function'
ACTION_DECO = 'action_function'
CHECK_TIMEOUT = 280  # in seconds. set to less than lambda limit (300 s)

# compare strings in both python 2 and python 3
# in other files, compare with utils.basestring
try:
    basestring = basestring
except NameError:
    basestring = str


def init_check_res(connection, name, init_uuid=None):
    """
    Initialize a CheckResult object, which holds all information for a
    check and methods necessary to store and retrieve latest/historical
    results. name is the only required parameter and MUST be equal to
    the method name of the check as defined in CheckSuite.

    init_uuid is a a result uuid that the check will look for upon initialization.
    If found, the check fields will be pre-populated with its results.
    """
    return CheckResult(connection.s3_connection, name, init_uuid=init_uuid)


def init_action_res(connection, name):
    """
    Similar to init_check_res, but meant to be used for ActionResult items
    """
    return ActionResult(connection.s3_connection, name)


def get_methods_by_deco(cls, decorator):
    """
    Returns all methods in cls/module with decorator as a list;
    the decorator is set in check_function()
    """
    methods = []
    for maybeDecorated in cls.__dict__.values():
        if hasattr(maybeDecorated, 'check_decorator'):
            if maybeDecorated.check_decorator == decorator:
                methods.append(maybeDecorated)
    return methods


def check_method_deco(method, decorator):
    """
    See if the given method has the given decorator. Returns True if so,
    False if not.
    """
    return hasattr(method, 'check_decorator') and method.check_decorator == decorator


def handle_kwargs(kwargs, default_kwargs):
    # add all default args that are not defined in kwargs
    # also ensure 'uuid' and 'primary' are in there
    for key in default_kwargs:
        if key not in kwargs:
            kwargs[key] = default_kwargs[key]
    if 'uuid' not in kwargs:
        kwargs['uuid'] = datetime.utcnow().isoformat()
    if 'primary' not in kwargs:
        kwargs['primary'] = False
    return kwargs


def check_function(*default_args, **default_kwargs):
    """
    Import decorator, used to decorate all checks.
    Sets the check_decorator attribute so that methods can be fetched.
    Any kwargs provided to the decorator will be passed to the function
    if no kwargs are explicitly passed.
    Handles all exceptions within running of the check, including validation
    issues/some common errors when writing checks. Will also keep track of overall
    runtime and cancel the check with status=ERROR if runtime exceeds CHECK_TIMEOUT.
    If an exception is raised, will store the result in full_output and
    return an ERROR CheckResult.
    """
    def check_deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            kwargs = handle_kwargs(kwargs, default_kwargs)
            partials = {'name': func.__name__, 'kwargs': kwargs, 'is_check': True,
                        'start_time': start_time, 'connection': args[0]}
            signal.signal(signal.SIGALRM, partial(timeout_handler, partials))
            signal.alarm(CHECK_TIMEOUT)  # run time allowed in seconds
            try:
                check = func(*args, **kwargs)
                validate_run_result(check, is_check=True)
            except Exception as e:
                # connection should be the first (and only) positional arg
                check = init_check_res(args[0], func.__name__)
                check.status = 'ERROR'
                check.description = 'Check failed to run. See full output.'
                check.full_output = traceback.format_exc().split('\n')
            signal.alarm(0)
            kwargs['runtime_seconds'] = round(time.time() - start_time, 2)
            check.kwargs = kwargs
            return check.store_result()
        wrapper.check_decorator = CHECK_DECO
        return wrapper
    return check_deco


def action_function(*default_args, **default_kwargs):
    """
    Import decorator, used to decorate all actions.
    Required for action functions.
    Any kwargs provided to the decorator will be passed to the function
    if no kwargs are explicitly passed.
    Handles all exceptions within running of the action, including validation
    issues/some common errors when writing actions. Will also keep track of overall
    runtime and cancel the check with status=ERROR if runtime exceeds CHECK_TIMEOUT.
    If an exception is raised, will store the result in output and return an
    ActionResult with status FAIL.
    """
    def action_deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            kwargs = handle_kwargs(kwargs, default_kwargs)
            partials = {'name': func.__name__, 'kwargs': kwargs, 'is_check': False,
                        'start_time': start_time, 'connection': args[0]}
            signal.signal(signal.SIGALRM, partial(timeout_handler, partials))
            signal.alarm(CHECK_TIMEOUT)  # run time allowed in seconds
            try:
                if 'called_by' not in kwargs:
                    raise BadCheckOrAction('Action is missing called_by in its kwargs.')
                action = func(*args, **kwargs)
                validate_run_result(action, is_check=False)
            except Exception as e:
                # connection should be the first (and only) positional arg
                action = init_action_res(args[0], func.__name__)
                action.status = 'FAIL'
                action.description = 'Action failed to run. See output.'
                action.output = traceback.format_exc().split('\n')
            signal.alarm(0)
            kwargs['runtime_seconds'] = round(time.time() - start_time, 2)
            action.kwargs = kwargs
            return action.store_result()
        wrapper.check_decorator = ACTION_DECO
        return wrapper
    return action_deco


def validate_run_result(result, is_check=True):
    """
    Result should be an ActionResult or CheckResult. Raises an exception if not.
    If is_check is false, assume we have an action.
    """
    error_message = None
    class_name = type(result).__name__
    if is_check and class_name != 'CheckResult':
        error_message = 'Check function must return a CheckResult object. Initialize one with init_check_res.'
    elif not is_check and class_name != 'ActionResult':
        error_message = 'Action functions must return a ActionResult object. Initialize one with init_action_res.'
    else:
        pass
    store_method = getattr(result, 'store_result', None)
    if not callable(store_method):
        error_message = 'Do not overwrite the store_result method of the check or action result.'
    if error_message:
        raise BadCheckOrAction(error_message)


class BadCheckOrAction(Exception):
    """
    Generic exception for a badly written check or library.
    __init__ takes some string error message
    """
    def __init__(self, message=None):
        # default error message if none provided
        if message is None:
            message = "Check or action function seems to be malformed."
        super().__init__(message)


def timeout_handler(partials, signum, frame):
    """
    Custom handler for signal that stores the current check
    or action with the appropriate information and then exits using sys.exit
    """
    if partials['is_check']:
        result = init_check_res(partials['connection'], partials['name'])
        result.status = 'ERROR'
    else:
        result = init_action_res(partials['connection'], partials['name'])
        result.status = 'FAIL'
    result.description = 'AWS lambda execution reached the time limit. Please see check/action code.'
    signal.alarm(0)
    kwargs = partials['kwargs']
    kwargs['runtime_seconds'] = round(time.time() - partials['start_time'], 2)
    result.kwargs = kwargs
    result.store_result()
    # need to delete the sqs message and propogate if this is using the queue
    if kwargs.get('_run_info') and {'receipt', 'sqs_url'} <= set(kwargs['_run_info'].keys()):
        runner_input = {'sqs_url': kwargs['_run_info']['sqs_url']}
        delete_message_and_propogate(runner_input, kwargs['_run_info']['receipt'])
    print('-RUN-> TIMEOUT for execution of %s. Elapsed time is %s seconds; keep under %s.'
          % (partials['name'], kwargs['runtime_seconds'], CHECK_TIMEOUT))
    sys.exit()


def parse_datetime_to_utc(time_str, manual_format=None):
    """
    Attempt to parse the string time_str with the given string format.
    If no format is given, attempt to automatically parse the given string
    that may or may not contain timezone information.
    Returns a datetime object of the string in UTC
    or None if the parsing was unsuccessful.
    """
    if manual_format and isinstance(manual_format, basestring):
        timeobj = datetime.strptime(time_str, manual_format)
    else:  # automatic parsing
        if len(time_str) > 26 and time_str[26] in ['+', '-']:
            try:
                timeobj = datetime.strptime(time_str[:26],'%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                return None
            if time_str[26]=='+':
                timeobj -= timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
            elif time_str[26]=='-':
                timeobj += timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
        elif len(time_str) == 26 and '+' not in time_str[-6:] and '-' not in time_str[-6:]:
            # nothing known about tz, just parse it without tz in this cause
            try:
                timeobj = datetime.strptime(time_str[0:26],'%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                return None
        else:
            # last try: attempt without milliseconds
            try:
                timeobj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
    return timeobj.replace(tzinfo=tz.tzutc())


##### SQS utils #####


def invoke_check_runner(runner_input):
    """
    Simple function to invoke the next check_runner lambda with runner_input
    (dict containing {'sqs_url': <str>})
    """
    client = boto3.client('lambda')
    # InvocationType='Event' makes asynchronous
    # try/except while async invokes are problematics
    try:
        response = client.invoke(
            FunctionName=RUNNER_NAME,
            InvocationType='Event',
            Payload=json.dumps(runner_input)
        )
    except:
        response = client.invoke(
            FunctionName=RUNNER_NAME,
            Payload=json.dumps(runner_input)
        )
    return response


def delete_message_and_propogate(runner_input, receipt):
    """
    Delete the message with given receipt from sqs queue and invoke the next
    lambda runner.
    """
    sqs_url = runner_input.get('sqs_url')
    if not sqs_url or not receipt:
        return
    client = boto3.client('sqs')
    client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=receipt
    )
    invoke_check_runner(runner_input)


def recover_message_and_propogate(runner_input, receipt):
    """
    Recover the message with given receipt to sqs queue and invoke the next
    lambda runner.

    Changing message VisibilityTimeout to 15 seconds means the message will be
    available to the queue in that much time. This is a slight lag to allow
    dependencies to process.
    NOTE: VisibilityTimeout should be less than WaitTimeSeconds in run_check_runner
    """
    sqs_url = runner_input.get('sqs_url')
    if not sqs_url or not receipt:
        return
    client = boto3.client('sqs')
    client.change_message_visibility(
        QueueUrl=sqs_url,
        ReceiptHandle=receipt,
        VisibilityTimeout=15
    )
    invoke_check_runner(runner_input)


def get_sqs_queue():
    """
    Returns boto3 sqs resource with QueueName=QUEUE_NAME
    """
    sqs = boto3.resource('sqs')
    try:
        queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    except:
        queue = sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '300',
                'MessageRetentionPeriod': '3600'
            }
        )
    return queue


def collect_run_info(run_uuid):
    """
    Returns a set of run checks under this run uuid
    """
    s3_connection = S3Connection('foursight-runs')
    run_prefix = ''.join([run_uuid, '/'])
    complete = s3_connection.list_all_keys_w_prefix(run_prefix)
    # eliminate duplicates
    return set(complete)


def send_sqs_messages(queue, environ, check_vals):
    """
    Send the messages to the queue. Check_vals are entries within a check_group
    """
    # uuid used as the MessageGroupId
    uuid = datetime.utcnow().isoformat()
    # append environ and uuid as first elements to all check_vals
    proc_vals = [[environ, uuid] + val for val in check_vals]
    for val in proc_vals:
        response = queue.send_message(MessageBody=json.dumps(val))


def get_sqs_attributes(sqs_url):
    """
    Returns a dict of the desired attributes form the queue with given url
    """
    backup = {
        'ApproximateNumberOfMessages': 'ERROR',
        'ApproximateNumberOfMessagesNotVisible': 'ERROR'
    }
    client = boto3.client('sqs')
    try:
        result = client.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )
    except:
        return backup
    return result.get('Attributes', backup)
