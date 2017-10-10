from __future__ import print_function, unicode_literals
import datetime
import json

# standardized class to take output from a signle CheckSuite check
# also used to retrieve and store s3 results for the check.

# contains these fields:
# name, description, date run, status, brief output, full output
# holds a reference to the overall s3 connection as well

class CheckResult(object):
    def __init__(self, s3connection, name, description="No description", extension=".json"):
        self.s3connection = s3connection
        self.name = name
        self.description = description
        # should I make an enum for status?
        # valid values are: 'PEND', 'PASS', 'WARN', 'FAIL', 'ERROR'
        self.status = 'PEND'
        self.extension = extension
        self.brief_output = None
        self.full_output = None


    def format_result(self, timestamp):
        return {
            'name': self.name,
            'description': self.description,
            'status': self.status,
            'timestamp': timestamp,
            'extension': self.extension,
            'brief_output': self.brief_output,
            'full_output': self.full_output
        }


    def get_latest(self):
        latest_key = ''.join([self.name, '/latest', self.extension])
        result = self.s3connection.get_object(latest_key)
        if result is None:
            timestamp = datetime.datetime.utcnow().isoformat()
            formatted = self.format_result(timestamp)
            formatted['status'] = 'ERROR'
            return formatted
        # see if data is in json format
        try:
            json_result = json.loads(result)
        except ValueError:
            return result
        return json_result


    def store_result(self):
        timestamp = datetime.datetime.utcnow().isoformat()
        formatted = self.format_result(timestamp)
        time_key = ''.join([self.name, '/', timestamp, self.extension])
        latest_key = ''.join([self.name, '/latest', self.extension])
        if self.extension == ".json":
            formatted = json.dumps(formatted)
        self.s3connection.put_object(time_key, formatted)
        # put result as 'latest'
        self.s3connection.put_object(latest_key, formatted)
