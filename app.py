# This app.py is the main Chalice entry point and is used only when running
# Foursight locally, i.e. via chalice local. This is effectively equivalent
# to app.py in 4dn-cloud-infra, which is the "real" app.py which is used
# as the main Chalice entry point when running Foursight deployed in AWS.

from chalicelib_fourfront.app_utils import AppUtils
from chalicelib_fourfront.check_schedules import *
from foursight_core.app_utils import set_stage, set_timeout
