from os.path import dirname
from foursight_core.app_utils import AppUtils as AppUtils_from_core
from .vars import FOURSIGHT_PREFIX, HOST
from dcicutils.env_utils import FF_PUBLIC_URL_PRD


class AppUtils(AppUtils_from_core):
    
    # overwriting parent class
    prefix = FOURSIGHT_PREFIX
    FAVICON = FF_PUBLIC_URL_PRD + '/static/img/favicon-fs.ico'  # favicon acquired from prod
    host = HOST
    package_name = 'chalicelib'
    check_setup_dir=dirname(__file__)
    html_main_title = 'Foursight'
