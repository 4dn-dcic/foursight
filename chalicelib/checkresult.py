from __future__ import print_function, unicode_literals
import datetime
import json

# standardized class to take output from a signle CheckSuite check
# also used to retrieve and store s3 results for the check.

# contains these fields:
# name, description, date run, status, brief output, full output
# holds a reference to the overall s3 connection as well

class CheckResult(object):
    def __init__(self, s3_connection, name, title=None, description=None, timestamp=None, extension=".json"):
        self.s3_connection = s3_connection
        # timestamp arg used if you want to overwrite an existing check
        if timestamp:
            ts_key = ''.join([name, '/', timestamp, extension])
            stamp_res = s3_connection.get_object(ts_key)
            if stamp_res:
                # see if json
                try:
                    parsed_res = json.loads(stamp_res)
                except ValueError:
                    parsed_res = stamp_res
                for key, val in parsed_res.items():
                    setattr(self, key, val)
                return
            else:
                # no previous results exist for this timestamp yet
                self.timestamp = timestamp
        else:
            self.timestamp = None
        self.name = name
        if title is None:
            self.title = ' '.join(self.name.split('_')).title()
        else:
            self.title = title
        self.description = description
        # should I make an enum for status?
        # valid values are: 'PEND', 'PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'
        self.status = 'PEND'
        self.extension = extension
        self.brief_output = None
        self.full_output = None


    def format_result(self, timestamp):
        return {
            'name': self.name,
            'title': self.title,
            'description': self.description,
            'status': self.status.upper(),
            'timestamp': timestamp,
            'extension': self.extension,
            'brief_output': self.brief_output,
            'full_output': self.full_output
        }


    def get_latest_check(self):
        latest_key = ''.join([self.name, '/latest', self.extension])
        result = self.s3_connection.get_object(latest_key)
        if result is None:
            return None
        # see if data is in json format
        try:
            json_result = json.loads(result)
        except ValueError:
            return result
        return json_result


    def get_closest_check(self, diff_hours, diff_mins=0):
        # check_tuples is a list of items of form (s3key, datetime timestamp)
        check_tuples = []
        s3_prefix = ''.join([self.name, '/'])
        relevant_checks = self.s3_connection.list_keys_w_prefix(s3_prefix)
        if not relevant_checks:
            return None
        # now use only s3 objects with a valid timestamp
        for check in relevant_checks:
            if check.startswith(s3_prefix) and check.endswith(self.extension):
                time_str = check[len(s3_prefix):-len(self.extension)]
                try:
                    check_time = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f")
                except ValueError:
                    continue
                check_tuples.append((check, check_time))
        desired_time = (datetime.datetime.utcnow() -
            datetime.timedelta(hours=diff_hours, minutes=diff_mins))
        best_match = get_closest(check_tuples, desired_time)
        result = self.s3_connection.get_object(best_match[0])
        # see if data is in json format
        try:
            json_result = json.loads(result)
        except ValueError:
            return result
        return json_result


    def store_result(self):
        # normalize status, probably not the optimal place to do this
        if self.status.upper() not in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE']:
            self.status = 'ERROR'
            self.description = 'Malformed status; look at Foursight check definition.'
        # if there's a set timestamp field, use that instead of curr utc time
        timestamp = self.timestamp if self.timestamp else datetime.datetime.utcnow().isoformat()
        formatted = self.format_result(timestamp)
        time_key = ''.join([self.name, '/', timestamp, self.extension])
        latest_key = ''.join([self.name, '/latest', self.extension])
        if self.extension == ".json":
            s3_formatted = json.dumps(formatted)
        self.s3_connection.put_object(time_key, s3_formatted)
        # put result as 'latest'
        self.s3_connection.put_object(latest_key, s3_formatted)
        # return stored data in case we're interested
        return formatted


### Utility functions for checkresult
def get_closest(items, pivot):
    """
    Return the item in the list of items closest to the given pivot.
    Items should be given in tuple form (ID, value (to compare))
    Intended primarily for use with datetime objects.
    See: S.O. 32237862
    """
    return min(items, key=lambda x: abs(x[1] - pivot))
