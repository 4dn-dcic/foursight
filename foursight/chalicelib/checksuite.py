from __future__ import print_function, unicode_literals
from .utils import make_registration_deco
from .checkresult import CheckResult
import requests
import json

# initialize the run_check decorator
def run_check(func):
    return func


run_check = make_registration_deco(run_check)


class CheckSuite(object):
    def __init__(self, connection):
        self.connection = connection


    def init_check(self, name, title=None, description=None, extension=".json"):
        """
        Initialize a CheckResult object, which holds all information for a
        check and methods necessary to store and retrieve latest/historical
        results. name is the only required parameter and MUST be equal to
        the method name of the check as defined in CheckSuite.
        """
        return CheckResult(self.connection.s3connection, name, title, description, extension)


    @run_check
    def status_of_servers(self):
        ff_server = self.connection.is_up
        es = self.connection.es
        es_resp = requests.get(es)
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
        check.store_result()


    @run_check
    def elasticsearch_indices(self):
        check = self.init_check('elasticsearch_indices')
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
        check.store_result()


    @run_check
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
            check.store_result()
            return
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
        check.store_result()


    @run_check
    def change_in_item_counts(self):
        # use this check to get the comparison
        health_count_check = self.init_check('item_counts_by_type')
        latest = health_count_check.get_latest_check()
        # get_health_counts run closest to 24 hours ago
        prior = health_count_check.get_closest_check(24)
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
                diff_counts[index] = latest[index]
            else:
                diff_DB = latest[index]['DB'] - prior[index]['DB']
                diff_ES = latest[index]['ES'] - prior[index]['ES']
                if diff_DB != 0 or diff_ES != 0:
                    diff_counts[index] = {'DB': diff_DB, 'ES': diff_ES}
        for index in prior_unique:
            diff_counts[index] = {
                'DB': -1 * prior[index]['DB'],
                'ES': -1 * prior[index]['ES']
            }
        check = self.init_check('change_in_item_counts')
        if diff_counts:
            check.status = 'WARN'
            check.full_output = diff_counts
            check.description = 'DB/ES counts have changed in past 24 hours; Positive numbers represent an increase in current counts.'
        else:
            check.status = 'PASS'
        check.store_result()


    @run_check
    def indexing_progress(self):
        # get latest and db/es counts closest to 2 hrs ago
        health_count_check = self.init_check('item_counts_by_type')
        latest = health_count_check.get_latest_check()
        prior = health_count_check.get_closest_check(2)
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
        check.store_result()


    def test3(self):
        # a non-run test
        return 'TEST 3 FOUND'
