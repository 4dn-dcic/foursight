# Utils for scripts used for wrangler checks
# Please add functions here as you see fit
from __future__ import print_function, unicode_literals
from dcicutils import s3_utils, ff_utils
from datetime import datetime, timedelta
from dateutil import tz


def get_FDN_Connection(connection):
    """
    Use connection.ff_env to connect and dcicutils.s3_utils to get the access
    key needed to build a FDN_Connection object. Returns None if the process
    fails or the FDN_Connection if successful.
    """
    # custom handling of data and staging
    if connection.fs_environment in ['data', 'staging']:
        use_env = 'fourfront-webprod'
    else:
        use_env = connection.ff_env
    s3Obj = s3_utils.s3Utils(env=use_env)
    # workaround to check if key is in the bucket
    contents = s3_utils.s3.list_objects_v2(Bucket=s3Obj.sys_bucket)
    key_names = [obj['Key'] for obj in contents.get('Contents', [])]
    if 'illnevertell' not in key_names:
        return None
    access_key = s3Obj.get_key()
    # need to overwrite the staging environment at the moment because
    # data and staging share a sys bucket
    if connection.fs_environment == 'staging':
        access_key['default']['server'] = 'http://staging.4dnucleome.org'
    # try to establish fdn_connection
    try:
        fdn_conn = ff_utils.fdn_connection(key=access_key)
    except:
        return None
    return fdn_conn if fdn_conn else None


def parse_datetime_with_tz_to_utc(time_str):
    """
    Attempt to parse a given datetime string that may or may not contain
    timezone information. Returns a datetime object of the string in UTC
    or None if the parsing was unsuccessful.
    """
    if len(time_str) > 26 and time_str[26] in ['+', '-']:
        try:
            timeobj = datetime.strptime(time_str[:26],'%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            return None
        if time_str[26]=='+':
            timeobj -= timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
        elif time_str[26]=='-':
            timeobj += timedelta(hours=int(time_str[27:29]), minutes=int(time_str[30:]))
    elif len(time_str) == 26 and '+' not in time_str[-6:] and '-' not in time_str[-6:]:
        # nothing known about tz, just parse it without tz in this cause
        try:
            timeobj = datetime.strptime(time_str[0:26],'%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            return None
    else:
        # last try: attempt without milliseconds
        try:
            timeobj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    return timeobj.replace(tzinfo=tz.tzutc())
