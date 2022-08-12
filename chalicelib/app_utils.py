from os.path import dirname
from foursight_core.app_utils import AppUtilsCore
from .vars import FOURSIGHT_PREFIX, HOST
from dcicutils.env_utils import public_url_for_app


class AppUtils(AppUtilsCore):
    
    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = public_url_for_app('fourfront') + '/static/img/favicon-fs.ico'  # favicon acquired from prod
    host = HOST
    package_name = 'chalicelib'
    check_setup_dir = dirname(__file__)
    html_main_title = 'Foursight'
