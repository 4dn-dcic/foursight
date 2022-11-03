import os
from foursight_core.app_utils import app  # Chalice object
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from foursight_core.identity import apply_identity_globally
from .vars import FOURSIGHT_PREFIX, HOST


class AppUtils(AppUtils_from_core):

    # dmichaels/C4-826: Apply identity globally.
    apply_identity_globally()

    # Overridden from subclass.
    APP_PACKAGE_NAME = "foursight"

    # Note that this is set in the new (as of August 2022) apply_identity code;
    # see foursight-core/foursight_core/{app_utils.py,identity.py}.
    es_host = os.environ.get("ES_HOST")
    if not es_host:
        raise Exception("Foursight ES_HOST environment variable not set!")
    HOST = es_host
    
    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = "https://data.4dnucleome.org/static/img/favicon-fs.ico"
    host = HOST
    package_name = 'chalicelib_fourfront'

    check_setup_file = AppUtils_from_core.locate_check_setup_file(os.path.dirname(__file__))
    if not check_setup_file:
        raise Exception("Unable to locate the check setup file!")
    print(f"Using check setup file: {check_setup_file}")

    DEFAULT_ENV = os.environ.get("ENV_NAME", "foursight-fourfront-env-uninitialized")


app_utils_obj = AppUtils.singleton(AppUtils)
