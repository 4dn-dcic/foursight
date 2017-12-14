# General utils for foursight
from __future__ import print_function, unicode_literals
import types
import datetime
from importlib import import_module
from functools import wraps
from .checkresult import CheckResult

CHECK_DECO = 'check_function'

# compare strings in both python 2 and python 3
# in other files, compare with utils.basestring
try:
    basestring = basestring
except NameError:
    basestring = str


def init_check_res(connection, name, title=None, description=None, uuid=None, ff_link=None, runnable=False, extension=".json"):
    """
    Initialize a CheckResult object, which holds all information for a
    check and methods necessary to store and retrieve latest/historical
    results. name is the only required parameter and MUST be equal to
    the method name of the check as defined in CheckSuite.

    uuid is a timestamp-style unique identifier that can be used to control
    where the output of the check is written.
    """
    return CheckResult(connection.s3_connection, name, title, description, uuid, ff_link, runnable, extension)


def build_dummy_result(check_name):
    """
    Simple function to return a dict consistent with a CheckResult dictionary
    content but is not actually stored.
    """
    return {
        'status': 'IGNORE',
        'name': check_name,
        'uuid': datetime.datetime.utcnow().isoformat()
    }


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
            # add all default args that are not defined in kwargs
            for key in default_kwargs:
                if key not in kwargs:
                    kwargs[key] = default_kwargs[key]
            return func(*args, **kwargs)
        wrapper.check_decorator = CHECK_DECO
        return wrapper
    return check_deco
