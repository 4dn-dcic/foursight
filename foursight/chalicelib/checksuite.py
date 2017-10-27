from __future__ import print_function, unicode_literals
from .utils import make_registration_deco
from .checkresult import CheckResult
import requests
import json
import datetime
import boto3
from collections import OrderedDict

# initialize the daily_check decorator
def daily_check(func):
    return func

def rate_check(func):
    return func


daily_check = make_registration_deco(daily_check)
rate_check = make_registration_deco(rate_check)


class CheckSuite(object):
    """
    This class represents the entirety of the checks run in Foursight.
    To create a new check, simply create a method for this class and add
    the '@daily_check' or '@rate_check decorator to it.
    This decorator MUST be used or the check will not be shown in fourfront.
    @daily_check should be used for checks that are scheduled to run every day
    using the app.run function, which runs at 10am UTC.
    @rate_check should be used for any non-daily check that will have a cron/
    rate defined for it in app.py.
    @daily_check methods MAY be run at custom intervals, but @rate_check
    methods will never run daily.

    Each check method should initialize a CheckResult object, which holds the
    name, status, output, and more for the check. This object should be
    initialized using the init_check function, which MUST be passed a name
    argument EXACTLY equal to the check name (i.e. method name).

    For example, the 'status_of_servers' check initilizes a CheckResult like so:
    check = self.init_check('status_of_servers')
    Then, fields on that CheckResult (named check) can be easily set:
    >> check.status = 'PASS'
    Lastly, once the check is finished, finalize and store S3 results using:
    >> return check.store_result()
    Returning the result from store_result(), or a custom value, is
    encouraged because the /run/ function uses these to report that tests
    have succesfully run.
    Returning None or no value without calling store_result() will effectively
    abort the check.

    You can get results from past/latest checks with any name in any check
    method by initializing a CheckResult with the corresponding name.
    For example, get the result of 'item_counts_by_type' check 24 hours ago:
    >> health_count_check = self.init_check('item_counts_by_type')
    >> prior = health_count_check.get_closest_check(24)
    get_closest_check() returns a Python dict of the check result, which
    can be interrogated in ways such as:
    >> prior['status']
    There is also a get_latest_check() method that returns the same type
    of object for the latest result of a given check.
    """
    def __init__(self, connection):
        # self.connection is an FFConnection object.
        # Reference s3 connection with self.connection.s3connection,
        # which is an S3Connection object.
        self.connection = connection


    def init_check(self, name, title=None, description=None, extension=".json"):
        """
        Initialize a CheckResult object, which holds all information for a
        check and methods necessary to store and retrieve latest/historical
        results. name is the only required parameter and MUST be equal to
        the method name of the check as defined in CheckSuite.
        """
        return CheckResult(self.connection.s3connection, name, title, description, extension)


    @daily_check
    def status_of_servers(self):
        ff_server = self.connection.is_up
        es = self.connection.es
        try:
            es_resp = requests.get(es)
        except:
            es_server = None
        else:
            es_server = es_resp.status_code == 200 and "You Know, for Search" in es_resp.text
        check = self.init_check('status_of_servers')
        if ff_server and es_server:
            check.status = 'PASS'
            check.description = 'Fourfront and ES servers are up.'
        else:
            check.status = 'FAIL'
            descrip = ''
            if not ff_server:
                descrip = ' '.join([descrip, 'Fourfront server is down.'])
            if not es_server:
                descrip = ' '.join([descrip, 'ES server is down.'])
            check.description = descrip
        return check.store_result()


    @daily_check
    def elastic_beanstalk_health(self):
        check = self.init_check('elastic_beanstalk_health')
        brief_output = {}
        full_output = {}
        eb_client = boto3.client('elasticbeanstalk')
        try:
            resp = eb_client.describe_instances_health(
                EnvironmentName=''.join(['fourfront-', self.connection.environment]),
                AttributeNames=['All']
            )
        except:
            return
        resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
        if resp_status != 200:
            check.status = 'ERROR'
            check.description = 'Could not establish a connection to AWS.'
            return check.store_result()
        instances_health = resp.get('InstanceHealthList', [])
        for instance in instances_health:
            inst_info = {}
            inst_info['deploy_status'] = instance['Deployment']['Status']
            inst_info['deploy_version'] = instance['Deployment']['VersionLabel']
            inst_info['deployed_at'] = datetime.datetime.strftime(instance['Deployment']['DeploymentTime'], "%Y-%m-%dT%H:%M:%S")
            inst_info['id'] = instance['InstanceId']
            inst_info['color'] = instance['Color']
            inst_info['health'] = instance['HealthStatus']
            inst_info['launced_at'] = datetime.datetime.strftime(instance['LaunchedAt'], "%Y-%m-%dT%H:%M:%S")
            inst_info['causes'] = instance.get('causes', [])
            if inst_info['causes'] or inst_info['color'] in ['Red', 'Yellow', 'Grey'] or inst_info['health'] in ['Degraded', 'Severe', 'Warning']:
                brief_output[inst_info['id']] = inst_info
            full_output[inst_info['id']] = inst_info
        if brief_output:
            check.status = 'FAIL'
            check.description = 'One or more Elastic Beanstalk instances may require attention.'
            check.brief_output = brief_output
        else:
            check.status = 'PASS'
        check.full_output = full_output
        return check.store_result()


    @daily_check
    def status_of_elasticsearch_indices(self):
        check = self.init_check('status_of_elasticsearch_indices')
        ### the check
        es = self.connection.es
        resp = requests.get(''.join([es,'_cat/indices?v']))
        indices = resp.text.split('\n')
        split_indices = [ind.split() for ind in indices]
        headers = split_indices.pop(0)
        index_info = {} # for full output
        warn_index_info = {} # for brief output
        for index in split_indices:
            if len(index) == 0:
                continue
            index_info[index[2]] = {header: index[idx] for idx, header in enumerate(headers)}
            if index_info[index[2]]['health'] != 'green' or index_info[index[2]]['status'] != 'open':
                warn_index_info[index[2]] = index_info[index[2]]
        # set fields, store result
        if not index_info:
            check.status = 'FAIL'
            check.description = 'Error reading status of ES.'
        elif warn_index_info:
            check.status = 'WARN'
            check.description = 'One or more ES indices have health != green or status != open.'
            check.brief_output = warn_index_info
        else:
            check.status = 'PASS'
        check.full_output = index_info
        return check.store_result()


    @rate_check
    def item_counts_by_type(self):
        def process_counts(count_str):
            # specifically formatted for FF health page
            ret = {}
            split_str = count_str.split()
            ret[split_str[0].strip(':')] = int(split_str[1])
            ret[split_str[2].strip(':')] = int(split_str[3])
            return ret

        check = self.init_check('item_counts_by_type')
        # run the check
        health_counts = {}
        warn_health_counts = {}
        server = self.connection.server
        try:
            health_res = requests.get(''.join([server,'health?format=json']))
        except:
            check.status = 'ERROR'
            return check.store_result()
        health_json = json.loads(health_res.text)
        for index in health_json['db_es_compare']:
            counts = process_counts(health_json['db_es_compare'][index])
            health_counts[index] = counts
            if counts['DB'] != counts['ES']:
                warn_health_counts[index] = counts
        # add ALL for total counts
        total_counts = process_counts(health_json['db_es_total'])
        health_counts['ALL'] = total_counts
        # set fields, store result
        if not health_counts:
            check.status = 'FAIL'
            check.description = 'Error on fourfront health page.'
        elif warn_health_counts:
            check.status = 'WARN'
            check.description = 'DB and ES counts are not equal.'
            check.brief_output = warn_health_counts
        else:
            check.status = 'PASS'
        check.full_output = health_counts
        return check.store_result()


    @daily_check
    def change_in_item_counts(self):
        # use this check to get the comparison
        health_count_check = self.init_check('item_counts_by_type')
        latest = health_count_check.get_latest_check()
        # get_health_counts run closest to 24 hours ago
        prior = health_count_check.get_closest_check(24)
        if not latest or not prior:
            return
        diff_counts = {}
        # drill into full_output
        latest = latest['full_output']
        prior = prior['full_output']
        # get any keys that are in prior but not latest
        prior_unique = list(set(prior.keys()) - set(latest.keys()))
        for index in latest:
            if index == 'ALL':
                continue
            if index not in prior:
                diff_counts[index] = latest[index]['DB']
            else:
                diff_DB = latest[index]['DB'] - prior[index]['DB']
                if diff_DB != 0:
                    diff_counts[index] = diff_DB
        for index in prior_unique:
            diff_counts[index] = -1 * prior[index]['DB']
        check = self.init_check('change_in_item_counts')
        if diff_counts:
            check.status = 'WARN'
            check.full_output = diff_counts
            check.description = 'DB counts have changed in past day; positive numbers represent an increase in current counts.'
        else:
            check.status = 'PASS'
        return check.store_result()


    @rate_check
    def indexing_progress(self):
        # get latest and db/es counts closest to 2 hrs ago
        health_count_check = self.init_check('item_counts_by_type')
        latest = health_count_check.get_latest_check()
        prior = health_count_check.get_closest_check(2)
        if not latest or not prior:
            return
        latest_unindexed = latest['full_output']['ALL']['DB'] - latest['full_output']['ALL']['ES']
        prior_unindexed = prior['full_output']['ALL']['DB'] - prior['full_output']['ALL']['ES']
        diff_unindexed = latest_unindexed - prior_unindexed
        check = self.init_check('indexing_progress')
        if diff_unindexed == 0 and latest_unindexed != 0:
            check.status = 'FAIL'
            check.description = ' '.join(['Total number of unindexed items is',
                str(latest_unindexed), 'and has not changed in the past two hours.',
                'The indexer may be malfunctioning.'])
        elif diff_unindexed > 0:
            check.status = 'WARN'
            check.description = ' '.join(['Total number of unindexed items has increased by',
                str(diff_unindexed), 'in the past two hours. Remaining items to index:',
                str(latest_unindexed)])
        else:
            check.status = 'PASS'
            check.description = ' '.join(['Indexing seems healthy. There are', str(latest_unindexed),
            'remaining items to index, a change of', str(diff_unindexed), 'from two hours ago.'])
        return check.store_result()


    @daily_check
    def indexing_records(self):
        check = self.init_check('indexing_records')
        es = self.connection.es
        es_resp = requests.get(''.join([es,'meta/meta/_search?q=_exists_:indexing_record&size=1000&sort=uuid:desc']))
        if getattr(es_resp, 'status_code', None) != 200:
            check.status = 'ERROR'
            check.description = "Error connecting to ES at endpoint: meta/meta/_search?q=_exists_:indexing_status"
            return check.store_result()
        # 3 day timedelta
        delta_days = datetime.timedelta(days=3)
        all_records = es_resp.json().get('hits', {}).get('hits', [])
        recent_records = {}
        warn_records = {}
        for rec in all_records:
            time_diff = (datetime.datetime.utcnow() -
                datetime.datetime.strptime(rec['_id'], "%Y-%m-%dT%H:%M:%S.%f"))
            if time_diff < delta_days:
                body = rec['_source'].get('indexing_record')
                this_record = {}
                this_record['record'] = body
                if not body:
                    warn_records[rec['_id']] = 'Indexing run is not finished'
                    if check.status == 'PEND': check.status = 'WARN'
                else:
                    elapsed = body.get('indexing_elapsed')
                    indexed = body.get('indexed')
                    if elapsed and indexed:
                        if 'day' in elapsed:
                            elapsed_dt = datetime.datetime.strptime(elapsed, "%d day, %H:%M:%S.%f")
                            # required to make the days to effect
                            base_dt = datetime.datetime(1899, 12, 31)
                        else:
                            elapsed_dt = datetime.datetime.strptime(elapsed, "%H:%M:%S.%f")
                            base_dt = datetime.datetime(1900, 1, 1)
                        elapsed_mins = ((elapsed_dt-base_dt).total_seconds() / 60.0)
                        this_record['indexed_per_minute'] = indexed / elapsed_mins
                    if body.get('last_xmin') is None or body.get('types_indexed') == 'all':
                        this_record['complete_index'] = True
                    else:
                        this_record['complete_index'] = False
                    if body.get('errors'):
                        warn_records[rec['_id']] = 'Indexing run has errors'
                        check.status = 'FAIL'
                recent_records[rec['_id']] = this_record
        del all_records
        # sort so most recent records are first
        sort_records = OrderedDict(sorted(recent_records.iteritems(), key=lambda rec: datetime.datetime.strptime(rec[0], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True))
        check.full_output = sort_records
        if warn_records:
            check.description = 'One or more indexing runs in the past three days may require attention.'
            check.brief_output = warn_records
        else:
            check.description = 'Indexing runs from the past three days seem normal.'
            check.status = 'PASS'
        return check.store_result()
