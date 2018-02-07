# General utils for foursight
from __future__ import print_function, unicode_literals
import types
import datetime
from importlib import import_module
from functools import wraps
from .run_result import CheckResult, ActionResult

CHECK_DECO = 'check_function'
ACTION_DECO = 'action_function'


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
        kwargs['uuid'] = datetime.datetime.utcnow().isoformat()
    if 'primary' not in kwargs:
        kwargs['primary'] = False
    return kwargs


def check_function(*default_args, **default_kwargs):
    """
    Import decorator, used to decorate all checks.
    Sets the check_decorator attribute so that methods can be fetched.
    Any kwargs provided to the decorator will be passed to the function
    if no kwargs are explicitly passed.
    """
    def check_deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = handle_kwargs(kwargs, default_kwargs)
            check = func(*args, **kwargs)
            return store_result_wrapper(check, kwargs, is_check=True)
        wrapper.check_decorator = CHECK_DECO
        return wrapper
    return check_deco


def action_function(*default_args, **default_kwargs):
    """
    Import decorator, used to decorate all actions.
    Required for action functions.
    Any kwargs provided to the decorator will be passed to the function
    if no kwargs are explicitly passed.
    """
    def action_deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = handle_kwargs(kwargs, default_kwargs)
            action = func(*args, **kwargs)
            return store_result_wrapper(action, kwargs, is_action=True)
        wrapper.check_decorator = ACTION_DECO
        return wrapper
    return action_deco


def store_result_wrapper(result, kwargs, is_check=False, is_action=False):
    """
    Result should be an ActionResult or CheckResult. Raises an exception if not.
    Sets the kwargs attr of the result and calls store_result method.
    """
    error_message = None
    class_name = type(result).__name__
    if is_check and class_name != 'CheckResult':
        error_message = 'Check function must return a CheckResult object. Initialize one with init_check_res.'
    elif is_action and class_name != 'ActionResult':
        error_message = 'Action functions must return a ActionResult object. Initialize one with init_action_res.'
    store_method = getattr(result, 'store_result', None)
    if not callable(store_method):
        error_message = 'Do not overwrite the store_result method of the check or action result.'
    if error_message:
        raise BadCheckOrAction(error_message)
    else:
        # set the kwargs parameter
        result.kwargs = kwargs
        return result.store_result()


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
