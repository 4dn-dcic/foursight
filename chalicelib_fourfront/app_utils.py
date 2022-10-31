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
    FAVICON = 'https://cgap-dbmi.hms.harvard.edu/static/img/favicon-fs.ico'
    host = HOST
    package_name = 'chalicelib_fourfront'
    check_setup_dir_fallback = os.path.abspath(os.path.dirname(__file__))
    if os.environ.get("CHALICE_LOCAL") == "1":
        check_setup_dir = "chalicelib_local"
    else:
        check_setup_dir = os.environ.get("FOURSIGHT_CHECK_SETUP_DIR") or os.path.dirname(__file__)
    check_setup_dir = os.path.abspath(check_setup_dir)
    DEFAULT_ENV = os.environ.get("ENV_NAME", "foursight-fourfront-env-uninitialized")
    html_main_title = "Foursight" # Foursight CGAP vs Fourfront difference now conveyed in the upper left icon.


app_utils_obj = AppUtils.singleton(AppUtils)
