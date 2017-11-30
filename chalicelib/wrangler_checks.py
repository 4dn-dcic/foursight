from __future__ import print_function, unicode_literals
from .utils import check_function, init_check_res, set_default_kwargs
from collections import OrderedDict
import requests
import sys
import json
import datetime
import boto3


@check_function
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(':')] = int(split_str[1])
        ret[split_str[2].strip(':')] = int(split_str[3])
        return ret

    check = init_check_res(connection, 'item_counts_by_type')
    # run the check
    item_counts = {}
    warn_item_counts = {}
    server = connection.ff
    try:
        counts_res = requests.get(''.join([server,'counts?format=json']))
    except:
        check.status = 'ERROR'
        return check.store_result()
    ##### temporary back up while counts endpoint gets worked into FF
    if counts_res.status_code != 200:
        try:
            counts_res = requests.get(''.join([server,'health?format=json']))
        except:
            check.status = 'ERROR'
            return check.store_result()
    #####
    if counts_res.status_code != 200:
        check.status = 'ERROR'
        return check.store_result()
    counts_json = json.loads(counts_res.text)
    for index in counts_json['db_es_compare']:
        counts = process_counts(counts_json['db_es_compare'][index])
        item_counts[index] = counts
        if counts['DB'] != counts['ES']:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json['db_es_total'])
    item_counts['ALL'] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = 'FAIL'
        check.description = 'Error on fourfront health page.'
    elif warn_item_counts:
        check.status = 'WARN'
        check.description = 'DB and ES counts are not equal.'
        check.brief_output = warn_item_counts
    else:
        check.status = 'PASS'
    check.full_output = item_counts
    return check.store_result()


@check_function
def change_in_item_counts(connection, **kwargs):
    # use this check to get the comparison
    check = init_check_res(connection, 'change_in_item_counts')
    counts_check = init_check_res(connection, 'item_counts_by_type')
    latest = counts_check.get_latest_check()
    # get_item_counts run closest to 24 hours ago
    prior = counts_check.get_closest_check(24)
    if not latest.get('full_output') or not prior.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no counts_check results to run this check with.'
        return check.store_result()
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
    if diff_counts:
        check.status = 'WARN'
        check.full_output = diff_counts
        check.description = 'DB counts have changed in past day; positive numbers represent an increase in current counts.'
    else:
        check.status = 'PASS'
    return check.store_result()
