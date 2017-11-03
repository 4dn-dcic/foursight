# General utils for foursight
from __future__ import print_function, unicode_literals

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
    Returns all methods in cls with decorator as a generator;
    the input decorator must be registered with the make_registration_deco.
    Again, see: S.O. 5910703
    """
    for maybeDecorated in cls.__dict__.values():
        if hasattr(maybeDecorated, 'decorator'):
            if maybeDecorated.decorator == decorator:
                yield maybeDecorated


def get_closest(items, pivot):
    """
    Return the item in the list of items closest to the given pivot.
    Items should be given in tuple form (ID, value (to compare))
    Intended primarily for use with datetime objects.
    See: S.O. 32237862
    """
    return min(items, key=lambda x: abs(x[1] - pivot))
