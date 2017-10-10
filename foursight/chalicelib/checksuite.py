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

    def init_check(self, name):
        return CheckResult(self.connection.s3connection, name)

    @run_check
    def get_server(self):
        return self.connection.server

    @run_check
    def get_es_indices(self):
        check = self.init_check('get_es_indices')
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
        elif warn_index_info:
            check.status = 'WARN'
        else:
            check.status = 'PASS'
        check.brief_output = warn_index_info
        check.full_output = index_info
        check.store_result()


    @run_check
    def get_health_counts(self):
        def process_counts(count_str):
            # specifically formatted for FF health page
            ret = {}
            split_str = count_str.split()
            ret[split_str[0].strip(':')] = int(split_str[1])
            ret[split_str[2].strip(':')] = int(split_str[3])
            return ret

        check = self.init_check('get_health_counts')
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
        health_counts['total_counts'] = process_counts(health_json['db_es_total'])
        health_counts['by_index_counts'] = {}
        for index in health_json['db_es_compare']:
            counts = process_counts(health_json['db_es_compare'][index])
            health_counts['by_index_counts'][index] = counts
            if counts['DB'] != counts['ES']:
                warn_health_counts[index] = counts
        # set fields, store result
        if not health_counts:
            check.status = 'FAIL'
        elif warn_health_counts:
            check.status = 'WARN'
        else:
            check.status = 'PASS'
        check.brief_output = warn_health_counts
        check.full_output = health_counts
        check.store_result()


    def test3(self):
        # a non-run test
        return 'TEST 3 FOUND'
