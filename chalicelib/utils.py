# General utils for foursight
from __future__ import print_function, unicode_literals
import types
from importlib import import_module
from .checkresult import CheckResult


def init_check_res(connection, name, title=None, description=None, extension=".json"):
    """
    Initialize a CheckResult object, which holds all information for a
    check and methods necessary to store and retrieve latest/historical
    results. name is the only required parameter and MUST be equal to
    the method name of the check as defined in CheckSuite.
    """
    return CheckResult(connection.s3_connection, name, title, description, extension)


def set_default_kwargs(default_kwargs, in_kwargs):
    """
    Simple time-saver function to set intial kwargs in checks.
    Triggers if the given args are {}
    """
    return default_kwargs if in_kwargs == {} else in_kwargs


def make_registration_deco(inDecorator):
    """
    Copies and returns the given decorator, with an added .decorator property
    See: S.O. 5910703
    This allows methods to be tagged with arbitrary decoractors and
    subsequently retrieved, providing control over what is run in foursight.
    """
    def newDecorator(func):
        new = inDecorator(func)
        new.decorator = newDecorator
        return new

    newDecorator.__name__ == inDecorator.__name__
    newDecorator.__doc__ == inDecorator.__doc__
    return newDecorator


def get_methods_by_deco(cls, decorator):
    """
    Returns all methods in cls/module with decorator as a list;
    the input decorator must be registered with the make_registration_deco.
    Again, see: S.O. 5910703
    """
    methods = []
    for maybeDecorated in cls.__dict__.values():
        if hasattr(maybeDecorated, 'decorator'):
            if maybeDecorated.decorator == decorator:
                methods.append(maybeDecorated)
    return methods


def check_method_deco(method, decorator):
    """
    See if the given method has the given decorator. Returns True if so,
    False if not.
    """
    return hasattr(method, 'decorator') and method.decorator == decorator


def check_function(*default_args, **default_kwargs):
    def outer(func):
        def inner_func(*args, **kwargs):
            if kwargs == {}:
                kwargs.update(default_kwargs)
            return func(*args, **kwargs)
        return inner_func
    return outer_func


# the decorator used for all check functions
check_function = make_registration_deco(check_function)
