# Utils for scripts used for wrangler checks
# Please add functions here as you see fit
from __future__ import print_function, unicode_literals
from dcicutils import s3_utils, ff_utils
from datetime import datetime, timedelta
from dateutil import tz
from .utils import basestring


def safe_search_with_callback(ff_env, query, container, callback, limit=20, frame='embedded'):
    """
    Somewhat temporary function to avoid making search queries that cause
    memory issues. Takes a string ff_env , a search query (without 'limit' or
    'from' parameters), a container to put search results in after running
    them through a given callback function, which should take a search hit as
    its first parameter and the container as its second parameter.
    Returns ALL hits for a given search
    """
    last_total = None
    curr_from = 0
    while not last_total or last_total == limit:
        print('...', curr_from)
        search_query = ''.join([query, '&from=', str(curr_from), '&limit=', str(limit), '&frame=', frame])
        search_res = ff_utils.search_metadata(search_query, ff_env=ff_env)
        if not search_res: # 0 results
            break
        last_total = len(search_res)
        curr_from += last_total
        for hit in search_res:
            callback(hit, container)


def parse_datetime_to_utc(time_str, manual_format=None):
    """
    Attempt to parse the string time_str with the given string format.
    If no format is given, attempt to automatically parse the given string
    that may or may not contain timezone information.
    Returns a datetime object of the string in UTC
    or None if the parsing was unsuccessful.
    """
    if manual_format and isinstance(manual_format, basestring):
        timeobj = datetime.strptime(time_str, manual_format)
    else:  # automatic parsing
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
