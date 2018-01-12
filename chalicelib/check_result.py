from __future__ import print_function, unicode_literals
import datetime
import json

# standardized class to take output from a signle CheckSuite check
# also used to retrieve and store s3 results for the check.

# contains these fields:
# name, description, date run, status, brief output, full output
# holds a reference to the overall s3 connection as well

class CheckResult(object):
    def __init__(self, s3_connection, name, uuid=None, runnable=False, extension=".json"):
        self.s3_connection = s3_connection
        # uuid arg used if you want to overwrite an existing check
        # uuid is in the stringified datetime format
        if uuid:
            ts_key = ''.join([name, '/', uuid, extension])
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
                # no previous results exist for this uuid yet
                self.uuid = uuid
        else:
            self.uuid = None
        self.name = name
        self.title = ' '.join(self.name.split('_')).title()
        self.description = None
        # should I make an enum for status?
        # valid values are: 'PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'
        # start with IGNORE as the default check status
        self.status = 'IGNORE'
        self.extension = extension
        self.brief_output = None
        self.full_output = None
        # admin output is only seen by admins on the UI
        self.admin_output = None
        self.ff_link = None
        # runnable controls whether a check can be individually run on the UI
        self.runnable = runnable


    def format_result(self, uuid):
        return {
            'name': self.name,
            'title': self.title,
            'description': self.description,
            'status': self.status.upper(),
            'uuid': uuid,
            'extension': self.extension,
            'brief_output': self.brief_output,
            'full_output': self.full_output,
            'admin_output': self.admin_output,
            'ff_link': self.ff_link,
            'runnable': self.runnable
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


    def get_all_checks(self):
        # return all results for this check. Should use with care
        all_results = []
        s3_prefix = ''.join([self.name, '/'])
        relevant_checks = self.s3_connection.list_keys_w_prefix(s3_prefix)
        for check in relevant_checks:
            if check.startswith(s3_prefix) and check.endswith(self.extension):
                result = self.s3_connection.get_object(check)
                try:
                    result = json.loads(result)
                except ValueError:
                    pass
                all_results.append(result)
        return all_results


    def get_closest_check(self, diff_hours, diff_mins=0):
        # check_tuples is a list of items of form (s3key, datetime uuid)
        check_tuples = []
        s3_prefix = ''.join([self.name, '/'])
        relevant_checks = self.s3_connection.list_keys_w_prefix(s3_prefix)
        if not relevant_checks:
            return None
        # now use only s3 objects with a valid uuid
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
        # if there's a set uuid field, use that instead of curr utc time
        uuid = self.uuid if self.uuid else datetime.datetime.utcnow().isoformat()
        formatted = self.format_result(uuid)
        time_key = ''.join([self.name, '/', uuid, self.extension])
        latest_key = ''.join([self.name, '/latest', self.extension])
        if self.extension == ".json":
            s3_formatted = json.dumps(formatted)
        self.s3_connection.put_object(time_key, s3_formatted)
        # put result as 'latest'
        self.s3_connection.put_object(latest_key, s3_formatted)
        # return stored data in case we're interested
        return formatted


### Utility functions for check_result
def get_closest(items, pivot):
    """
    Return the item in the list of items closest to the given pivot.
    Items should be given in tuple form (ID, value (to compare))
    Intended primarily for use with datetime objects.
    See: S.O. 32237862
    """
    return min(items, key=lambda x: abs(x[1] - pivot))
