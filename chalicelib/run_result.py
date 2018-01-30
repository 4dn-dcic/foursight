from __future__ import print_function, unicode_literals
import datetime
import json


class RunResult(object):
    """
    Generic class for CheckResult and ActionResult. Contains methods common
    to both.
    """
    def __init__(self, s3_connection, name):
        self.s3_connection = s3_connection
        self.name = name
        self.extension = ".json"
        self.kwargs = {}


    def get_latest_result(self):
        """
        Returns the latest result (the last check run)
        """
        latest_key = ''.join([self.name, '/latest', self.extension])
        return self.get_s3_object(latest_key)


    def get_primary_result(self):
        """
        Returns the most recent primary result run (with 'primary'=True in kwargs)
        """
        primary_key = ''.join([self.name, '/primary', self.extension])
        return self.get_s3_object(primary_key)


    def get_closest_result(self, diff_hours=0, diff_mins=0):
        """
        Returns check result that is closest to the current time minus
        diff_hours and diff_mins (both integers).

        TODO: Add some way to control which results are returned by kwargs?
        For example, you might only want primary results.
        """
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
        return self.get_s3_object(best_match[0])


    def get_s3_object(self, key):
        """
        Returns None if not present, otherwise returns a JSON parsed res.
        """
        result = self.s3_connection.get_object(key)
        if result is None:
            return None
        # see if data is in json format
        try:
            json_result = json.loads(result)
        except ValueError:
            return result
        return json_result


    def get_all_results(self):
        # return all results for this check. Should use with care
        all_results = []
        s3_prefix = ''.join([self.name, '/'])
        relevant_checks = self.s3_connection.list_keys_w_prefix(s3_prefix)
        for n in range(len(relevant_checks)):
            check = relevant_checks[n]
            if check.startswith(s3_prefix) and check.endswith(self.extension):
                result = self.s3_connection.get_object(check)
                try:
                    result = json.loads(result)
                except ValueError:
                    pass
                all_results.append(result)
        return all_results


    def get_result_history(self, start, limit):
        """
        Used to get the uuid, status, and kwargs for a specific check.
        Results are ordered by uuid (timestamped) and sliced from start to limit.
        Probably only called from app_utils.get_foursight_history.
        Returns a list of lists (inner lists: [status, kwargs])
        """
        all_keys = self.s3_connection.list_keys_w_prefix(self.name, records_only=True)
        # sort them from newest to oldest
        all_keys.sort(key = lambda x: x[len(self.name + '/'):], reverse=True)
        # enforce limit and start
        all_keys = all_keys[start:start+limit]
        results = []
        for n in range(len(all_keys)):
            s3_res = self.get_s3_object(all_keys[n])
            # order: status <str>, kwargs <dict>
            # handle records that might be malformed
            res_val = [
                s3_res.get('status', 'Not found'),
                s3_res.get('kwargs', {})
            ]
            results.append(res_val)
        return results


    def store_formatted_result(self, uuid, formatted, primary):
        """
        Store the result in s3. Always makes an entry with key equal to the
        uuid timestamp. Will also store under (i.e. overwrite)the 'latest' key.
        If is_primary, will also overwrite the 'primary' key.
        """
        time_key = ''.join([self.name, '/', uuid, self.extension])
        latest_key = ''.join([self.name, '/latest', self.extension])
        primary_key = ''.join([self.name, '/primary', self.extension])
        s3_formatted = json.dumps(formatted)
        # store the timestamped result
        self.s3_connection.put_object(time_key, s3_formatted)
        # put result as 'latest' key
        self.s3_connection.put_object(latest_key, s3_formatted)
        # if primary, store as the primary result
        if primary:
            self.s3_connection.put_object(primary_key, s3_formatted)
        # return stored data in case we're interested
        return formatted



class CheckResult(RunResult):
    """
    Inherits from RunResult and is meant to be used with checks.
    It is best to initialize this object using the init_check_res function in
    utils.py.

    Usage:
    check = init_check_res(connection, <name>)
    check.status = ...
    check.descritpion = ...
    check.store_result()
    """
    def __init__(self, s3_connection, name, init_uuid=None, runnable=False):
        # init_uuid arg used if you want to initialize using an existing check
        # init_uuid is in the stringified datetime format
        if init_uuid:
            # maybe make this '.json' dynamic with parent's self.extension?
            ts_key = ''.join([name, '/', init_uuid, '.json'])
            stamp_res = s3_connection.get_object(ts_key)
            if stamp_res:
                # see if json
                try:
                    parsed_res = json.loads(stamp_res)
                except ValueError:
                    parsed_res = stamp_res
                for key, val in parsed_res.items():
                    if key not in ['uuid', 'kwargs']: # dont copy these
                        setattr(self, key, val)
                super().__init__(s3_connection, name)
                return
        self.title = ' '.join(name.split('_')).title()
        self.description = None
        # valid values are: 'PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE'
        # start with IGNORE as the default check status
        self.status = 'IGNORE'
        self.brief_output = None
        self.full_output = None
        # admin output is only seen by admins on the UI
        self.admin_output = None
        self.ff_link = None
        # self.action_name is the function name of the action AND
        # should be an group in ACTION_GROUPS
        # you must set both create both of these to make an action work
        self.action = None
        self.allow_action = False # by default do not allow the action to be run
        # runnable controls whether a check can be individually run on the UI
        self.runnable = runnable
        super().__init__(s3_connection, name)


    def format_result(self, uuid):
        return {
            'name': self.name,
            'title': self.title,
            'description': self.description,
            'status': self.status.upper(),
            'uuid': uuid,
            'brief_output': self.brief_output,
            'full_output': self.full_output,
            'admin_output': self.admin_output,
            'ff_link': self.ff_link,
            'action': self.action,
            'allow_action': self.allow_action,
            'runnable': self.runnable,
            'kwargs': self.kwargs
        }


    def store_result(self):
        # normalize status, probably not the optimal place to do this
        if self.status.upper() not in ['PASS', 'WARN', 'FAIL', 'ERROR', 'IGNORE']:
            self.status = 'ERROR'
            self.description = 'Malformed status; look at Foursight check definition.'
        # if there's a set uuid field, use that instead of curr utc time
        # kwargs should **always** have uuid and some value of primary
        if 'uuid' not in self.kwargs:
            self.kwargs['uuid'] = datetime.datetime.utcnow().isoformat()
        if 'primary' not in self.kwargs:
            self.kwargs['primary'] = False
        formatted = self.format_result(self.kwargs['uuid'])
        is_primary = self.kwargs.get('primary', False) == True
        # bail if do_not_store is True within kwargs
        if self.kwargs.get('do_not_store', False) == True:
            return formatted
        return self.store_formatted_result(self.kwargs['uuid'], formatted, is_primary)



class ActionResult(RunResult):
    """
    Inherits from RunResult and is meant to be used with actions.
    """
    def __init__(self, s3_connection, name):
        self.description = None
        # valid values are: 'DONE', 'FAIL', 'PEND'
        # start with PEND as the default status
        self.status = 'PEND'
        self.output = None
        super().__init__(s3_connection, name)


    def format_result(self, uuid):
        return {
            'name': self.name,
            'description': self.description,
            'status': self.status.upper(),
            'uuid': uuid,
            'output': self.output,
            'kwargs': self.kwargs
        }


    def store_result(self):
        # normalize status, probably not the optimal place to do this
        if self.status.upper() not in ['DONE', 'FAIL', 'PEND']:
            self.status = 'FAIL'
            self.description = 'Malformed status; look at Foursight action definition.'
        # kwargs should **always** have uuid
        if 'uuid' not in self.kwargs:
            self.kwargs['uuid'] = datetime.datetime.utcnow().isoformat()
        formatted = self.format_result(self.kwargs['uuid'])
        # action results are always stored as 'primary' and 'latest' and can be
        # fetched with the get_latest_result method.
        # bail if do_not_store is True within kwargs
        if self.kwargs.get('do_not_store', False) == True:
            return formatted
        return self.store_formatted_result(self.kwargs['uuid'], formatted, True)



### Utility functions for check_result
def get_closest(items, pivot):
    """
    Return the item in the list of items closest to the given pivot.
    Items should be given in tuple form (ID, value (to compare))
    Intended primarily for use with datetime objects.
    See: S.O. 32237862
    """
    return min(items, key=lambda x: abs(x[1] - pivot))
