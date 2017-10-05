# General utils for foursight
from __future__ import print_function, unicode_literals

def makeRegistrationDecorator(inDecorator):
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


def getMethodsByDecorator(cls, decorator):
    """
    Returns all methods in cls with decorator as a generator;
    the input decorator must be registered with the makeRegistrationDecorator.
    Again, see: S.O. 5910703
    """
    for maybeDecorated in cls.__dict__.values():
        if hasattr(maybeDecorated, 'decorator'):
            if maybeDecorated.decorator == decorator:
                yield maybeDecorated
