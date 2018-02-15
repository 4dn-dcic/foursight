from __future__ import print_function, unicode_literals
from .utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
import requests
import sys
import json
import datetime
import boto3


@check_function()
def elastic_beanstalk_health(connection, **kwargs):
    """
    Check both environment health and health of individual instances
    """
    check = init_check_res(connection, 'elastic_beanstalk_health')
    full_output = {}
    eb_client = boto3.client('elasticbeanstalk')
    try:
        resp = eb_client.describe_environment_health(
            EnvironmentName=connection.ff_env,
            AttributeNames=['All']
        )
    except:
        check.status = 'ERROR'
        check.description = 'Could get EB environment information from AWS.'
        return check
    resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    if resp_status != 200:
        check.status = 'ERROR'
        check.description = 'Could not establish a connection to AWS.'
        return check
    full_output['status'] = resp.get('Status')
    full_output['environment_name'] = resp.get('EnvironmentName')
    full_output['color'] = resp.get('Color')
    full_output['health_status'] = resp.get('HealthStatus')
    full_output['causes'] = resp.get('Causes')
    full_output['instance_health'] = []
    try:
        resp = eb_client.describe_instances_health(
            EnvironmentName=connection.ff_env,
            AttributeNames=['All']
        )
    except:
        check.status = 'ERROR'
        check.description = 'Could get EB instance health information from AWS.'
        return check
    resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    if resp_status != 200:
        check.status = 'ERROR'
        check.description = 'Could not establish a connection to AWS.'
        return check
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
        full_output['instance_health'].append(inst_info)
    if full_output['color'] == 'Grey':
        check.status = 'WARN'
        check.description = 'EB environment is updating.'
    elif full_output['color'] == 'Yellow':
        check.status = 'WARN'
        check.description = 'EB environment is compromised; requests may fail.'
    elif full_output['color'] == 'Red':
        check.status = 'FAIL'
        check.description = 'EB environment is degraded; requests are likely to fail.'
    else:
        check.status = 'PASS'
    check.full_output = full_output
    return check


@check_function()
def status_of_elasticsearch_indices(connection, **kwargs):
    check = init_check_res(connection, 'status_of_elasticsearch_indices')
    ### the check
    es = connection.es
    try:
        resp = requests.get(''.join([es,'_cat/indices?v']), timeout=20)
    except:
        resp = None
    if resp is None or getattr(resp, 'status_code', None) != 200:
        check.status = 'ERROR'
        check.description = "Error connecting to ES at endpoint: _cat/indices"
        return check
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
    return check


@check_function()
def indexing_progress(connection, **kwargs):
    check = init_check_res(connection, 'indexing_progress')
    # get latest and db/es counts closest to 10 mins ago
    counts_check = init_check_res(connection, 'item_counts_by_type')
    latest = counts_check.get_primary_result()
    prior = counts_check.get_closest_result(diff_mins=30)
    if not latest.get('full_output') or not prior.get('full_output'):
        check.status = 'ERROR'
        check.description = 'There are no item_counts_by_type results to run this check with.'
        return check
    latest_unindexed = latest['full_output']['ALL']['DB'] - latest['full_output']['ALL']['ES']
    prior_unindexed = prior['full_output']['ALL']['DB'] - prior['full_output']['ALL']['ES']
    diff_unindexed = latest_unindexed - prior_unindexed
    if diff_unindexed == 0 and latest_unindexed != 0:
        check.status = 'FAIL'
        check.description = ' '.join(['Total number of unindexed items is',
            str(latest_unindexed), 'and has not changed in the past thirty minutes.',
            'The indexer may be malfunctioning.'])
    elif diff_unindexed > 0:
        check.status = 'WARN'
        check.description = ' '.join(['Total number of unindexed items has increased by',
            str(diff_unindexed), 'in the past thirty minutes. Remaining items to index:',
            str(latest_unindexed)])
    else:
        check.status = 'PASS'
        check.description = ' '.join(['Indexing seems healthy. There are', str(latest_unindexed),
        'remaining items to index, a change of', str(diff_unindexed), 'from thirty minutes ago.'])
    return check


@check_function()
def indexing_records(connection, **kwargs):
    check = init_check_res(connection, 'indexing_records')
    es = connection.es
    try:
        es_resp = requests.get(''.join([es,'meta/meta/_search?q=_exists_:indexing_status&size=1000&sort=uuid:desc']), timeout=20)
    except:
        es_resp = None
    if es_resp is None or getattr(es_resp, 'status_code', None) != 200:
        check.status = 'ERROR'
        check.description = "Error connecting to ES at endpoint: meta/meta/_search?q=_exists_:indexing_status"
        return check
    # 3 day timedelta
    delta_days = datetime.timedelta(days=3)
    all_records = es_resp.json().get('hits', {}).get('hits', [])
    recent_records = []
    warn_records = []
    for rec in all_records:
        time_diff = (datetime.datetime.utcnow() -
            datetime.datetime.strptime(rec['_id'], "%Y-%m-%dT%H:%M:%S.%f"))
        if time_diff < delta_days:
            body = rec['_source'].get('indexing_record')
            if not body:
                this_record = {
                    'timestamp': rec['_id'],
                    'to_index': rec['_source'].get('to_index'),
                    'finished': False
                }
                warn_records.append(this_record)
                if check.status == 'IGNORE': check.status = 'WARN'
            else:
                this_record = {'record': body}
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
                this_record['indexed'] = indexed
                this_record['timestamp'] = rec['_id']
                this_record['finished'] = True
                if body.get('errors'):
                    warn_records.append(this_record)
                    check.status = 'FAIL'
            recent_records.append(this_record)
    del all_records
    # sort so most recent records are first
    sort_records = sorted(recent_records, key=lambda rec: datetime.datetime.strptime(rec['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True)
    check.full_output = sort_records
    if warn_records:
        sort_warn_records = sorted(warn_records, key=lambda rec: datetime.datetime.strptime(rec['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True)
        check.description = 'One or more indexing runs in the past three days may require attention.'
        check.brief_output = sort_warn_records
    else:
        check.description = 'Indexing runs from the past three days seem normal.'
        check.status = 'PASS'
    return check


# do_not_store kwarg makes it so check.store_result will not write to s3
@check_function(do_not_store=True)
def staging_deployment(connection, **kwargs):
    check = init_check_res(connection, 'staging_deployment')
    return check


@check_function()
def fourfront_performance_metrics(connection, **kwargs):
    check = init_check_res(connection, 'fourfront_performance_metrics')
    return check
