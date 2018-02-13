# Utils for scripts used for wrangler checks
# Please add functions here as you see fit
from __future__ import print_function, unicode_literals
from dcicutils import s3_utils, ff_utils
from datetime import datetime, timedelta
from dateutil import tz


def get_s3_utils_obj(connection):
    """
    Returns a dcicutils.s3_utils.s3Utils object
    """
    # custom handling of data and staging
    if connection.fs_environment in ['data', 'staging']:
        use_env = 'fourfront-webprod'
    else:
        use_env = connection.ff_env
    return s3_utils.s3Utils(env=use_env)


def get_FDN_connection(connection):
    """
    Use connection.ff_env to connect and dcicutils.s3_utils to get the access
    key needed to build a FDN_Connection object. Returns None if the process
    fails or the FDN_Connection if successful.
    """
    s3Obj = get_s3_utils_obj(connection)
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


def safe_search_with_callback(fdn_conn, query, container, callback, limit=20, frame='embedded'):
    """
    Somewhat temporary function to avoid making search queries that cause
    memory issues. Takes a ff_utils fdn_conn, a search query (without 'limit' or
    'from' parameters), a container to put search results in after running
    them through a given callback function, which should take a search hit as
    its first parameter and the container as its second parameter.
    """
    last_total = None
    curr_from = 0
    while not last_total or last_total == limit:
        print('...', curr_from)
        search_query = ''.join([query, '&from=', str(curr_from), '&limit=', str(limit)])
        search_res = ff_utils.search_metadata(search_query, connection=fdn_conn, frame=frame)
        if not search_res: # 0 results
            break
        last_total = len(search_res)
        curr_from += last_total
        for hit in search_res:
            callback(hit, container)


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
