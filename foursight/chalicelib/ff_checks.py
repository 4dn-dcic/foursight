from __future__ import print_function, unicode_literals
from .utils import makeRegistrationDecorator
import requests
import json

# initialize the run_check decorator
def run_check(func):
    return func


run_check = makeRegistrationDecorator(run_check)


class FFCheckSuite(object):
    def __init__(self, connection):
        self.connection = connection

    @run_check
    def get_server(self):
        return self.connection.server

    @run_check
    def get_es_indices(self):
        es = self.connection.es
        resp = requests.get(''.join([es,'_cat/indices?v']))
        indices = resp.text.split('\n')
        split_indices = [ind.split() for ind in indices]
        headers = split_indices.pop(0)
        index_info = {}
        for index in split_indices:
            if len(index) == 0:
                continue
            index_info[index[2]] = {header: index[idx] for idx, header in enumerate(headers)}
        return index_info

    @run_check
    def get_health_counts(self):
        def process_counts(count_str):
            # specifically formatted for FF health page
            ret = {}
            split_str = count_str.split()
            ret[split_str[0].strip(':')] = int(split_str[1])
            ret[split_str[2].strip(':')] = int(split_str[3])
            return ret

        health_counts = {}
        server = self.connection.server
        health_res = requests.get(''.join([server,'health?format=json']))
        health_json = json.loads(health_res.text)
        health_counts['total_counts'] = process_counts(health_json['db_es_total'])
        health_counts['by_index_counts'] = {index: process_counts(health_json['db_es_compare'][index]) for index in health_json['db_es_compare']}
        return health_counts

    def test3(self):
        # a non-run test
        return 'TEST 3 FOUND'
