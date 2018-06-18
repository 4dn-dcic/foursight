from os.path import dirname, basename, isfile
import glob

# Add all files in this directory to the package
modules = glob.glob(dirname(__file__)+"/*.py")
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not f.endswith('__init__.py')]
