import requests
import json
import datetime
import boto3
import time
import geocoder
from foursight_core.stage import Stage
from foursight_core.checks.helpers.sys_utils import (
    parse_datetime_to_utc,
    wipe_build_indices
)
from dcicutils import (
    ff_utils,
    es_utils,
    beanstalk_utils,
    env_utils
)

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


# XXX: put into utils?
FF_BLUE_ES_CLUSTER_DOMAIN = 'fourfront-blue-6-8'
FF_GREEN_ES_CLUSTER_DOMAIN = 'fourfront-green-6-8'
PROD_ES_CLUSTERS = [
    FF_BLUE_ES_CLUSTER_DOMAIN,
    FF_GREEN_ES_CLUSTER_DOMAIN
]
FF_TEST_CLUSTER = 'search-fourfront-testing-opensearch-kqm7pliix4wgiu4druk2indorq.us-east-1.es.amazonaws.com:443'
TEST_ES_CLUSTERS = [
    FF_TEST_CLUSTER
]


@check_function()
def wipe_ff_build_indices(connection, **kwargs):
    """ Wipes build (number prefixed) indices (on fourfront-testing) """
    check = CheckResult(connection, 'wipe_ff_build_indices')
    return wipe_build_indices(FF_TEST_CLUSTER, check)


@check_function()
def elastic_search_space(connection, **kwargs):
    """ Checks that our ES nodes all have a certain amount of space remaining """
    check = CheckResult(connection, 'elastic_search_space')
    full_output = {}
    client = es_utils.create_es_client(connection.ff_es, True)
    # use cat.nodes to get id,diskAvail for all nodes, filter out empties
    node_space_entries = filter(None, [data.split() for data in client.cat.nodes(h='id,diskAvail').split('\n')])
    check.summary = check.description = None
    full_output['nodes'] = {}
    for _id, remaining_space in node_space_entries:
        if 'gb' not in remaining_space:
            if 'mb' not in remaining_space:
                check.status = 'FAIL'
                check.summary = check.description = 'At least one of the nodes in this env has no space remaining'
            else:
                check.status = 'WARN'
                check.summary = check.description = 'At least one of the nodes in this env is low on space'
        full_output['nodes'][_id.strip()] = { 'remaining_space': remaining_space }
    if check.summary is None:
        check.status = 'PASS'
        check.summary = check.description = 'All nodes have >1gb remaining disk space'
    check.full_output = full_output
    return check


def resolve_es_domain_name(connection):
    """ Resolves blue vs. green depending on the URL, throwing exception if something.  """
    if 'fourfront-blue' in connection.ff_es:
        return FF_BLUE_ES_CLUSTER_DOMAIN
    elif 'fourfront-green' in connection.ff_es:
        return FF_GREEN_ES_CLUSTER_DOMAIN
    else:
        raise Exception('Tried to run autoscaling check on non-production cluster! %s' % connection.ff_es)


@check_function()
def scale_down_elasticsearch_production(connection, **kwargs):
    """ Scales down Elasticsearch (production configuration).
        HOT (0600 to 2000 EST):
            Master:
                3x c5.large.elasticsearch
            Data:
                2x c5.2xlarge.elasticsearch
        COLD (2000 to 0600 EST):  This is what we are resizing to
            Master:
                None
            Data:
                3x c5.xlarge.elasticsearch
        XXX: should probably use constants in ElasticSearchServiceClient
        For now, must be explicitly triggered - but should be put on a schedule.
    """
    check = CheckResult(connection, 'scale_down_elasticsearch_production')
    es_client = es_utils.ElasticSearchServiceClient()
    success = es_client.resize_elasticsearch_cluster(
                domain_name=resolve_es_domain_name(connection),
                master_node_type='t2.medium.elasticsearch',  # discarded
                master_node_count=0,
                data_node_type='c5.xlarge.elasticsearch',
                data_node_count=3
            )
    if not success:
        check.status = 'ERROR'
        check.description = 'Could not trigger cluster resize - check lambda logs'
    else:
        check.status = 'PASS'
        check.description = 'Downward cluster resize triggered'
    return check


@check_function()
def scale_up_elasticsearch_production(connection, **kwargs):
    """ Scales up Elasticsearch (production configuration).
        HOT (0600 to 2000 EST):  This is what we are resizing to
            Master:
                3x c5.large.elasticsearch
            Data:
                2x c5.2xlarge.elasticsearch
        COLD (2000 to 0600 EST):
            Master:
                None
            Data:
                2x c5.large.elasticsearch
        XXX: should probably use constants in ElasticSearchServiceClient
        For now, must be explicitly triggered - but should be put on a schedule.
    """
    check = CheckResult(connection, 'scale_up_elasticsearch_production')
    es_client = es_utils.ElasticSearchServiceClient()
    success = es_client.resize_elasticsearch_cluster(
                domain_name=resolve_es_domain_name(connection),
                master_node_type='c5.large.elasticsearch',
                master_node_count=3,
                data_node_type='c5.2xlarge.elasticsearch',
                data_node_count=2
            )
    if not success:
        check.status = 'ERROR'
        check.description = 'Could not trigger cluster resize - check lambda logs'
    else:
        check.status = 'PASS'
        check.description = 'Downward cluster resize triggered'
    return check


# @check_function()
# def elastic_beanstalk_health(connection, **kwargs):
#     """
#     Check both environment health and health of individual instances
#     """
#     check = CheckResult(connection, 'elastic_beanstalk_health')
#     full_output = {}
#     eb_client = boto3.client('elasticbeanstalk')
#     resp = eb_client.describe_environment_health(
#         EnvironmentName=connection.ff_env,
#         AttributeNames=['All']
#     )
#     resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
#     if resp_status >= 400:
#         check.status = 'ERROR'
#         check.description = 'Could not establish a connection to AWS (status %s).' % resp_status
#         return check
#     full_output['status'] = resp.get('Status')
#     full_output['environment_name'] = resp.get('EnvironmentName')
#     full_output['color'] = resp.get('Color')
#     full_output['health_status'] = resp.get('HealthStatus')
#     full_output['causes'] = resp.get('Causes')
#     full_output['instance_health'] = []
#     # now look at the individual instances
#     resp = eb_client.describe_instances_health(
#         EnvironmentName=connection.ff_env,
#         AttributeNames=['All']
#     )
#     resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
#     if resp_status >= 400:
#         check.status = 'ERROR'
#         check.description = 'Could not establish a connection to AWS (status %s).' % resp_status
#         return check
#     instances_health = resp.get('InstanceHealthList', [])
#     for instance in instances_health:
#         inst_info = {}
#         inst_info['deploy_status'] = instance['Deployment']['Status']
#         inst_info['deploy_version'] = instance['Deployment']['VersionLabel']
#         # get version deployment time
#         application_versions = eb_client.describe_application_versions(
#             ApplicationName='4dn-web',
#             VersionLabels=[inst_info['deploy_version']]
#         )
#         deploy_info = application_versions['ApplicationVersions'][0]
#         inst_info['version_deployed_at'] = datetime.datetime.strftime(deploy_info['DateCreated'], "%Y-%m-%dT%H:%M:%S")
#         inst_info['instance_deployed_at'] = datetime.datetime.strftime(instance['Deployment']['DeploymentTime'], "%Y-%m-%dT%H:%M:%S")
#         inst_info['instance_launced_at'] = datetime.datetime.strftime(instance['LaunchedAt'], "%Y-%m-%dT%H:%M:%S")
#         inst_info['id'] = instance['InstanceId']
#         inst_info['color'] = instance['Color']
#         inst_info['health'] = instance['HealthStatus']
#         inst_info['causes'] = instance.get('causes', [])
#         full_output['instance_health'].append(inst_info)
#     if full_output['color'] == 'Grey':
#         check.status = 'WARN'
#         check.summary = check.description = 'EB environment is updating'
#     elif full_output['color'] == 'Yellow':
#         check.status = 'WARN'
#         check.summary = check.description = 'EB environment is compromised; requests may fail'
#     elif full_output['color'] == 'Red':
#         check.status = 'FAIL'
#         check.summary = check.description = 'EB environment is degraded; requests are likely to fail'
#     else:
#         check.summary = check.description = 'EB environment seems healthy'
#         check.status = 'PASS'
#     check.full_output = full_output
#     return check


@check_function()
def status_of_elasticsearch_indices(connection, **kwargs):
    check = CheckResult(connection, 'status_of_elasticsearch_indices')
    ### the check
    client = es_utils.create_es_client(connection.ff_es, True)
    indices = client.cat.indices(v=True).split('\n')
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
        check.summary = 'Error reading status of ES indices'
        check.description = 'Error reading status of ES indices'
    elif warn_index_info:
        check.status = 'WARN'
        check.summary = 'ES indices may not be healthy'
        check.description = 'One or more ES indices have health != green or status != open.'
        check.brief_output = warn_index_info
    else:
        check.status = 'PASS'
        check.summary = 'ES indices seem healthy'
    check.full_output = index_info
    return check


@check_function()
def indexing_progress(connection, **kwargs):
    check = CheckResult(connection, 'indexing_progress')
    # get latest and db/es counts closest to 10 mins ago
    counts_check = CheckResult(connection, 'item_counts_by_type')
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
        check.summary = 'Indexing is not progressing'
        check.description = ' '.join(['Total number of unindexed items is',
            str(latest_unindexed), 'and has not changed in the past thirty minutes.',
            'The indexer may be malfunctioning.'])
    elif diff_unindexed > 0:
        check.status = 'PASS'
        check.summary = 'Indexing load has increased'
        check.description = ' '.join(['Total number of unindexed items has increased by',
            str(diff_unindexed), 'in the past thirty minutes. Remaining items to index:',
            str(latest_unindexed)])
    else:
        check.status = 'PASS'
        check.summary = 'Indexing seems healthy'
        check.description = ' '.join(['Indexing seems healthy. There are', str(latest_unindexed),
        'remaining items to index, a change of', str(diff_unindexed), 'from thirty minutes ago.'])
    return check


@check_function()
def indexing_records(connection, **kwargs):
    check = CheckResult(connection, 'indexing_records')
    client = es_utils.create_es_client(connection.ff_es, True)
    namespaced_index = connection.ff_env + 'indexing'
    # make sure we have the index and items within it
    if (not client.indices.exists(namespaced_index) or
        client.count(index=namespaced_index).get('count', 0) < 1):
        check.summary = check.description = 'No indexing records found'
        check.status = 'PASS'
        return check

    res = client.search(index=namespaced_index, doc_type='indexing', sort='uuid:desc', size=1000,
                        body={'query': {'query_string': {'query': '_exists_:indexing_status'}}})
    delta_days = datetime.timedelta(days=3)
    all_records = res.get('hits', {}).get('hits', [])
    recent_records = []
    warn_records = []
    for rec in all_records:
        if rec['_id'] == 'latest_indexing':
            continue
        time_diff = (datetime.datetime.utcnow() -
            datetime.datetime.strptime(rec['_id'], "%Y-%m-%dT%H:%M:%S.%f"))
        if time_diff < delta_days:
            body = rec['_source']
            # needed to handle transition to queue. can use 'indexing_started'
            body['timestamp'] = rec['_id']
            if body.get('errors') or body.get('indexing_status') != 'finished':
                warn_records.append(body)
            recent_records.append(body)
    del all_records
    # sort so most recent records are first
    sort_records = sorted(recent_records, key=lambda rec: datetime.datetime.strptime(rec['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True)
    check.full_output = sort_records
    if warn_records:
        sort_warn_records = sorted(warn_records, key=lambda rec: datetime.datetime.strptime(rec['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True)
        check.summary = check.description = 'Indexing runs in the past three days may require attention'
        check.status = 'WARN'
        check.brief_output = sort_warn_records
    else:
        check.summary = check.description = 'Indexing runs from the past three days seem normal'
        check.status = 'PASS'
    return check


# this is a dummy check that is not run but instead updated with put API
# do_not_store parameter ensures running this check normally won't add to s3
@check_function(do_not_store=True)
def staging_deployment(connection, **kwargs):
    check = CheckResult(connection, 'staging_deployment')
    return check


#@check_function()
def fourfront_performance_metrics(connection, **kwargs):
    check = CheckResult(connection, 'fourfront_performance_metrics')
    full_output = {}  # contains ff_env, env_health, deploy_version, num instances, and performance
    performance = {}  # keyed by check_url
    # get information from elastic_beanstalk_health
    eb_check = CheckResult(connection, 'elastic_beanstalk_health')
    eb_info = eb_check.get_primary_result()['full_output']
    full_output['ff_env'] = connection.ff_env
    full_output['env_health'] = eb_info.get('health_status', 'Unknown')
    # get deploy version from the first instance
    full_output['deploy_version'] = eb_info.get('instance_health', [{}])[0].get('deploy_version', 'Unknown')
    full_output['num_instances'] = len(eb_info.get('instance_health', []))
    check_urls = [
        'counts',
        'joint-analysis-plans',
        'bar_plot_aggregations/type=ExperimentSetReplicate&experimentset_type=replicate/?field=experiments_in_set.experiment_type',
        'browse/?type=ExperimentSetReplicate&experimentset_type=replicate',
        'experiment-set-replicates/4DNESIE5R9HS/',
        'experiment-set-replicates/4DNESIE5R9HS/?datastore=database',
        'experiment-set-replicates/4DNESQWI9K2F/',
        'experiment-set-replicates/4DNESQWI9K2F/?datastore=database',
        'workflow-runs-awsem/ba50d240-5312-4aa7-b600-6b18d8230311/',
        'workflow-runs-awsem/ba50d240-5312-4aa7-b600-6b18d8230311/?datastore=database',
        'files-fastq/4DNFIX75FSJM/',
        'files-fastq/4DNFIX75FSJM/?datastore=database'
    ]
    for check_url in check_urls:
        performance[check_url] = {}
        try:
            # set timeout really high
            ff_resp = ff_utils.authorized_request(connection.ff_server + check_url,
                                                  auth=connection.ff_keys, timeout=1000)
        except Exception as e:
            performance[check_url]['error'] = str(e)
        if ff_resp and hasattr(ff_resp, 'headers') and 'X-stats' in ff_resp.headers:
            x_stats = ff_resp.headers['X-stats']
            if not isinstance(x_stats, str):
                performance[check_url]['error'] = 'Stats response is not a string.'
                continue
            # X-stats in form: 'db_count=148&db_time=1215810&es_count=4& ... '
            split_stats = x_stats.strip().split('&')
            parse_stats = [stat.split('=') for stat in split_stats]
            # stats can be strings or integers
            for stat in parse_stats:
                if not len(stat) == 2:
                    continue
                try:
                    performance[check_url][stat[0]] = int(stat[1])
                except ValueError:
                    performance[check_url][stat[0]] = stat[1]
            performance[check_url]['error'] = ''
    check.status = 'PASS'
    full_output['performance'] = performance
    check.full_output = full_output
    return check


#@check_function(time_limit=480)
def secondary_queue_deduplication(connection, **kwargs):
    check = CheckResult(connection, 'secondary_queue_deduplication')
    # maybe handle this in check_setup.json
    if Stage.is_stage_prod() is False:
        check.full_output = 'Will not run on dev foursight.'
        check.status = 'PASS'
        return check

    client = boto3.client('sqs')
    sqs_res = client.get_queue_url(
        QueueName=connection.ff_env + '-secondary-indexer-queue'
    )
    queue_url = sqs_res['QueueUrl']
    # get approx number of messages
    attrs = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    visible = attrs.get('Attributes', {}).get('ApproximateNumberOfMessages', '0')
    starting_count = int(visible)
    time_limit = kwargs['time_limit']
    t0 = time.time()
    sent = 0
    deleted = 0
    deduplicated = 0
    total_msgs = 0
    replaced = 0
    repeat_replaced = 0
    problem_msgs = []
    elapsed = round(time.time() - t0, 2)
    failed = []
    seen_uuids = set()
    # this is a bit of a hack -- send maximum sid with every message we replace
    # get the maximum sid at the start of deduplication and update it if we
    # encounter a higher sid
    max_sid_resp = ff_utils.authorized_request(connection.ff_server + 'max-sid',
                                               auth=connection.ff_keys).json()
    if max_sid_resp['status'] != 'success':
        check.status = 'FAIL'
        check.summary = 'Could not retrieve max_sid from the server'
        return check
    max_sid = max_sid_resp['max_sid']

    exit_reason = 'out of time'
    dedup_msg = 'FS dedup uuid: %s' % kwargs['uuid']
    while elapsed < time_limit:
        # end if we are spinning our wheels replacing the same uuids
        if (replaced + repeat_replaced) >= starting_count:
            exit_reason = 'starting uuids fully covered'
            break
        send_uuids = set()
        to_send = []
        to_delete = []
        recieved = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # batch size for all sqs ops
            WaitTimeSeconds=1  # 1 second of long polling
        )
        batch = recieved.get("Messages", [])
        if not batch:
            exit_reason = 'no messages left'
            break
        for msg in batch:
            try:
                msg_body = json.loads(msg['Body'])
            except json.JSONDecodeError:
                problem_msgs.append(msg['Body'])
                continue
            total_msgs += 1
            msg_uuid = msg_body['uuid']
            # update max_sid with message sid if applicable
            if msg_body.get('sid') is not None and msg_body['sid'] > max_sid:
                max_sid = msg_body['sid']
            msg_body['sid'] = max_sid
            to_process = {
                'Id': msg['MessageId'],
                'ReceiptHandle': msg['ReceiptHandle']
            }
            # every item gets deleted; original uuids get re-sent
            to_delete.append(to_process)
            if msg_uuid in seen_uuids and msg_body.get('fs_detail', '') != dedup_msg:
                deduplicated += 1
            else:
                # don't increment replaced count if we've seen the item before
                if msg_uuid not in seen_uuids:
                    replaced += 1
                else:
                    repeat_replaced += 1
                time.sleep(0.0001)  # slight sleep for time-based Id
                # add foursight uuid stamp
                msg_body['fs_detail'] = dedup_msg
                # add a slight delay to recycled messages, so that they are
                # not available for consumption for 2 seconds
                send_info = {
                    'Id': str(int(time.time() * 1000000)),
                    'MessageBody': json.dumps(msg_body),
                    'DelaySeconds': 2
                }
                to_send.append(send_info)
                seen_uuids.add(msg_uuid)
                send_uuids.add(msg_uuid)
        if to_send:
            res = client.send_message_batch(
                QueueUrl=queue_url,
                Entries=to_send
            )
            # undo deduplication if errors are detected
            res_failed = res.get('Failed', [])
            failed.extend(res_failed)
            if res_failed:
                # handle conservatively on error and don't delete
                for uuid in send_uuids:
                    if uuid in seen_uuids:
                        seen_uuids.remove(uuid)
                        replaced -= 1
                continue
            sent += len(to_send)
        if to_delete:
            res = client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=to_delete
            )
            failed.extend(res.get('Failed', []))
            deleted += len(to_delete)
        elapsed = round(time.time() - t0, 2)

    check.full_output = {
        'total_messages_covered': total_msgs,
        'uuids_covered': len(seen_uuids),
        'deduplicated': deduplicated,
        'replaced': replaced,
        'repeat_replaced': repeat_replaced,
        'time': elapsed,
        'problem_messages': problem_msgs,
        'exit_reason': exit_reason
    }
    # these are some standard things about the result that should always be true
    if replaced != len(seen_uuids) or (deduplicated + replaced + repeat_replaced) != total_msgs:
        check.status = 'FAIL'
        check.summary = 'Message totals do not add up. Report to Carl'
    if failed:
        if check.status != 'FAIL':
            check.status = 'WARN'
            check.summary = 'Queue deduplication encountered an error'
        check.full_output['failed'] = failed
    else:
        check.status = 'PASS'
        check.summary = 'Removed %s duplicates from %s secondary queue' % (deduplicated, connection.ff_env)
    check.description = 'Items on %s secondary queue were deduplicated. Started with approximately %s items; replaced %s items and removed %s duplicates. Covered %s unique uuids. Took %s seconds.' % (connection.ff_env, starting_count, replaced, deduplicated, len(seen_uuids), elapsed)

    return check


# @check_function()
def clean_up_travis_queues(connection, **kwargs):
    """
    Clean up old sqs queues based on the name ("travis-job")
    and the creation date. Only run on data for now
    """
    check = CheckResult(connection, 'clean_up_travis_queues')
    check.status = 'PASS'
    if connection.fs_env != 'data' or Stage.is_stage_prod() is False:
        check.summary = check.description = 'This check only runs on the data environment for Foursight prod'
        return check
    sqs_client = boto3.client('sqs')
    sqs = boto3.resource('sqs')
    queues = sqs.queues.all()
    num_deleted = 0
    for queue in queues:
        if 'travis-job' in queue.url:
            try:
                creation = queue.attributes['CreatedTimestamp']
            except sqs_client.exceptions.QueueDoesNotExist:
                continue
            if isinstance(creation, str):
                creation = float(creation)
            dt_creation = datetime.datetime.utcfromtimestamp(creation)
            queue_age = datetime.datetime.utcnow() - dt_creation
            # delete queues 3 days old or older
            if queue_age > datetime.timedelta(days=3):
                queue.delete()
                num_deleted += 1
    check.summary = 'Cleaned up %s old indexing queues' % num_deleted
    check.description = 'Cleaned up all indexing queues from Travis that are 3 days old or older. %s queues deleted.' % num_deleted
    return check


# @check_function()
# def manage_old_filebeat_logs(connection, **kwargs):
#     # import curator
#     check = CheckResult(connection, 'manage_old_filebeat_logs')
#
#     # temporary -- disable this check
#     check.status = 'PASS'
#     check.description = 'Not currently running this check'
#     return check

    # check.status = "WARNING"
    # check.description = "not able to get data from ES"
    #
    # # configure this thing
    # start_backup = 14
    # trim_backup = 30
    # timestring = '%Y.%m.%d'
    #
    # log_index = 'filebeat-'
    # #TODO: this probably needs to change when namespacing is implemented
    # snapshot = 'backup-%s-' % connection.ff_env
    # today = datetime.datetime.today().strftime(timestring)
    #
    # # run the check
    # client = es_utils.create_es_client(connection.ff_es, True)
    # # store backups in foursight s3, cause I know we have access to this..
    # # maybe this should change?
    # es_utils.create_snapshot_repo(client, snapshot[:-1], 'foursight-runs')
    #
    # # amazon es auto backups first 14 days, so we only need backup after that
    # ilo = es_utils.get_index_list(client, log_index, start_backup, timestring)
    #
    # if len(ilo.indices) > 0:
    #     try:
    #         new_snapshot = curator.Snapshot(ilo, repository=snapshot[:-1], name='%s%s' % (snapshot, today))
    #         new_snapshot.do_action()
    #         check.full_output = "Snapshot taken for %s indices" % len(ilo.indices)
    #     except curator.exceptions.FailedExecution as e:
    #         # snapshot already exists
    #         if "Invalid snapshot name" in str(e):
    #             check.full_output = "Snapshot already exists with same name for %s indices, so skipping." % len(ilo.indices)
    #         else:
    #             raise(e)
    #
    # # now trim further to only be indexes 30-days or older and delete
    # ilo = es_utils.get_index_list(client, log_index, trim_backup, timestring, ilo=ilo)
    #
    # if len(ilo.indices) > 0:
    #     cleanupIndices = curator.DeleteIndices(ilo)
    #     cleanupIndices.do_action()
    #     check.full_output += " Cleaned up %s old indices" % len(ilo.indices)
    #
    # check.status = "PASS"
    # check.description = 'Performed auto-backup to repository %s' % snapshot[:-1]
    # return check


@check_function()
def snapshot_rds(connection, **kwargs):
    check = CheckResult(connection, 'snapshot_rds')
    if Stage.is_stage_prod() is False:
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check
    rds_name = 'fourfront-production' if (env_utils.is_fourfront_env(connection.ff_env) and env_utils.is_stg_or_prd_env(connection.ff_env)) else connection.ff_env
    # snapshot ID can only have letters, numbers, and hyphens
    snap_time = datetime.datetime.strptime(kwargs['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%dT%H-%M-%S")
    snapshot_name = 'foursight-snapshot-%s-%s' % (rds_name, snap_time)
    client = boto3.client('rds', region_name=beanstalk_utils.REGION)
    res = client.create_db_snapshot(
             DBSnapshotIdentifier=snapshot_name,
             DBInstanceIdentifier=rds_name
    )
    if not res.get('DBSnapshot'):
        check.status = 'FAIL'
        check.summary = check.description = 'Something went wrong during snapshot creation'
        check.full_output = res
    else:
        check.status = 'PASS'
        # there is a datetime in the response that must be str formatted
        res['DBSnapshot']['InstanceCreateTime'] = str(res['DBSnapshot']['InstanceCreateTime'])
        check.full_output = res
        check.summary = 'Snapshot successfully created'
        check.description = 'Snapshot succesfully created with name: %s' % snapshot_name
    return check


# @check_function()
def process_download_tracking_items(connection, **kwargs):
    """
    Do a few things here, and be mindful of the 5min lambda limit.
    - Consolidate tracking items with download_tracking.range_query=True
    - Change remote_ip to geo_country and geo_city
    - If the user_agent looks to be a bot, set status=deleted
    - Change unused range query items to status=deleted
    """
    check = CheckResult(connection, 'process_download_tracking_items')
    # maybe handle this in check_setup.json
    if Stage.is_stage_prod() is False:
        check.full_output = 'Will not run on dev foursight.'
        check.status = 'PASS'
        return check
    # hold warning messages
    check.brief_output = {'cannot_parse_date_created': []}
    range_cache = {}
    # duration we want to consolidate range queries over
    # search older entries since range_consolidation_hrs * 2 to avoid duplication
    range_consolidation_hrs = 1
    cons_date = (datetime.datetime.utcnow() -
                 datetime.timedelta(hours=range_consolidation_hrs * 2)).strftime('%Y-%m-%dT%H\:%M')
    range_search_query = ''.join(['search/?type=TrackingItem&tracking_type=download_tracking',
                                  '&download_tracking.range_query=true&sort=-date_created',
                                  '&status=released&q=last_modified.date_modified:>=', cons_date])
    cons_query = ff_utils.search_metadata(range_search_query, key=connection.ff_keys)
    for tracking in cons_query:
        dl_info = tracking['download_tracking']
        user_agent = dl_info.get('user_agent', 'unknown_user_agent').lower()
        range_key = '//'.join([dl_info['remote_ip'], dl_info['filename'],
                               user_agent, dl_info['user_uuid']])
        parsed_date = parse_datetime_to_utc(tracking['date_created'])
        # check for date parsing error
        if parsed_date is None:
            continue
        if range_key in range_cache:
            range_cache[range_key].append(parsed_date)
        else:
            range_cache[range_key] = [parsed_date]
    del cons_query
    ip_cache = {}
    time_limit = 270  # 4.5 minutes
    # list of strings used to flag user_agent as a bot. By no means complete
    bot_agents = ['bot', 'crawl', 'slurp', 'spider', 'mediapartners', 'ltx71']
    # list of user_agents we always consider range_query=True
    range_query_agents = ['igv', 'java', 'python-requests']
    t0 = time.time()  # keep track of how start time

    # batch large groups of tracking items at once to save time with geocoder
    # for now, this function will process only <search_limit> results.
    # I would love to use a generator, but search results change as items are indexed...
    search_limit = 1000
    search_query = ''.join(['search/?type=TrackingItem&tracking_type=download_tracking',
                            '&download_tracking.geo_country=No+value',
                            '&status=in+review+by+lab&sort=-date_created&limit=', str(search_limit)])
    search_page = ff_utils.search_metadata(search_query, key=connection.ff_keys, page_limit=200)
    counts = {'proc': 0, 'deleted': 0, 'released': 0}

    page_ips = set([tracking['download_tracking']['remote_ip'] for tracking in search_page])
    # transform all IP addresses into GEO information with a persistent connection
    with requests.Session() as session:
        for track_ip in page_ips:
            if track_ip in ip_cache:
                continue
            geo = geocoder.ip(track_ip, session=session)
            geo_country = getattr(geo, 'country') or 'Unknown'
            geo_city = getattr(geo, 'city') or 'Unknown'
            geo_state = getattr(geo, 'state', None)
            if geo_state:
                geo_city = ', '.join([geo_city, geo_state])
            # cache the geo info in an arbitrary form
            ip_cache[track_ip] = '//'.join([geo_city, geo_country])

    # iterate over the individual tracking items
    for tracking in search_page:
        if round(time.time() - t0, 2) > time_limit:
            break
        dl_info = tracking['download_tracking']
        user_agent = dl_info.get('user_agent', 'unknown_user_agent').lower()
        # remove request_headers, which may contain sensitive information
        if 'request_headers' in dl_info:
            del dl_info['request_headers']
        geo_info = ip_cache[dl_info['remote_ip']]
        dl_info['geo_city'], dl_info['geo_country'] = geo_info.split('//')
        patch_body = {'status': 'released', 'download_tracking': dl_info}
        # delete items from bot user agents
        if (any(bot_str in user_agent for bot_str in bot_agents)
            and dl_info['user_uuid'] == 'anonymous'):
            patch_body['status'] = 'deleted'
        # set range_query=True for select user agents
        if (any(ua_str in user_agent for ua_str in range_query_agents)):
            dl_info['range_query'] = True
        # deduplicate range query requests by ip/filename/user_agent/user_uuid
        if patch_body['status'] != 'deleted' and dl_info['range_query'] is True:
            range_key = '//'.join([dl_info['remote_ip'], dl_info['filename'],
                                   user_agent, dl_info['user_uuid']])
            parsed_date = parse_datetime_to_utc(tracking['date_created'])
            if parsed_date is not None:
                if range_key in range_cache:
                    # for all reference range queries with this info, see if this one
                    # was created within one hour of it. if so, it is redundant and delete
                    for range_reference in range_cache[range_key]:
                        compare_date_low = range_reference - datetime.timedelta(hours=range_consolidation_hrs)
                        compare_date_high = range_reference + datetime.timedelta(hours=range_consolidation_hrs)
                        if parsed_date > compare_date_low and parsed_date < compare_date_high:
                            patch_body['status'] = 'deleted'
                            break
                    if patch_body['status'] != 'deleted':
                        range_cache[range_key].append(parsed_date)
                else:
                    # set the upper limit for for range queries to consolidate
                    range_cache[range_key] = [parsed_date]
            else:
                check.brief_output['cannot_parse_date_created'].append(tracking['uuid'])

        ff_utils.patch_metadata(patch_body, tracking['uuid'], key=connection.ff_keys)
        counts['proc'] += 1
        if patch_body['status'] == 'released':
            counts['released'] += 1
        else:
            counts['deleted'] += 1
    if any(check.brief_output.values()):
        check.status = 'WARN'
    else:
        check.status = 'PASS'
    check.summary = 'Successfully processed %s download tracking items' % counts['proc']
    check.description = '%s. Released %s items and deleted %s items' % (check.summary, counts['released'], counts['deleted'])
    return check


# @check_function()
def purge_download_tracking_items(connection, **kwargs):
    """
    This check was originally created to take in any search through kwargs.
    Changed to hardcode a search for tracking items, but it can easily
    adapted; as it is, already handles recording for any number of item types.
    Ensure search includes limit, field=uuid, and status=deleted
    """
    check = CheckResult(connection, 'purge_download_tracking_items')

    # Don't run if staging deployment is running
    # Only need to check if our env is data
    # XXX: Removing for now as we find the check can never run without this
    # if the staging deploy takes long enough or errors
    # if connection.fs_env == 'data':
    #     from ..app_utils import AppUtils
    #     staging_conn = AppUtils().init_connection('staging')
    #     staging_deploy = CheckResult(staging_conn, 'staging_deployment').get_primary_result()
    #     if staging_deploy['status'] != 'PASS':
    #         check.summary = 'Staging deployment is running - skipping'
    #         return check

    if Stage.is_stage_prod() is False:
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check

    time_limit = 270  # 4.5 minutes
    t0 = time.time()
    check.full_output = {}  # purged items by item type
    search = '/search/?type=TrackingItem&tracking_type=download_tracking&status=deleted&field=uuid&limit=300'
    search_res = ff_utils.search_metadata(search, key=connection.ff_keys)
    search_uuids = [res['uuid'] for res in search_res]
    client = es_utils.create_es_client(connection.ff_es, True)
    # a bit convoluted, but we want the frame=raw, which does not include uuid
    # use get_es_metadata to handle this. Use it as a generator
    for to_purge in ff_utils.get_es_metadata(search_uuids, es_client=client, is_generator=True,
                                             key=connection.ff_keys):
        if round(time.time() - t0, 2) > time_limit:
            break
        purge_properties = to_purge['properties']
        purge_properties['uuid'] = to_purge['uuid']  # add uuid to frame=raw
        try:
            purge_res = ff_utils.purge_metadata(to_purge['uuid'], key=connection.ff_keys)
        except Exception as exc:
            purge_status = 'error'
            purge_detail = str(exc)
        else:
            purge_status = purge_res['status']
            purge_detail = purge_properties if purge_status == 'success' else purge_res
        purge_record = {'uuid': to_purge['uuid'], 'result': purge_detail}
        if to_purge['item_type'] not in check.full_output:
            check.full_output[to_purge['item_type']] = {}
        if purge_status not in check.full_output[to_purge['item_type']]:
            check.full_output[to_purge['item_type']][purge_status] = []
        check.full_output[to_purge['item_type']][purge_status].append(purge_record)
    purge_out_str = '. '.join(['%s: %s' % (it, len(check.full_output[it]['success']))
                               for it in check.full_output if check.full_output[it].get('success')])
    check.description = 'Purged: ' + purge_out_str + '. Search used: %s' % search
    if any([it for it in check.full_output if check.full_output[it].get('error')]):
        check.status = 'WARN'
        check.summary = 'Some items failed to purge. See full output'
    else:
        check.status = 'PASS'
        check.summary = 'Items purged successfully'
    return check


@check_function()
def check_long_running_ec2s(connection, **kwargs):
    """
    Flag all ec2s that have been running for longer than 1 week (WARN) or 2 weeks
    (FAIL) if any contain any strings from `flag_names` in their
    names, or if they have no name.
    """
    check = CheckResult(connection, 'check_long_running_ec2s')
    if Stage.is_stage_prod() is False:
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check

    client = boto3.client('ec2')
    # flag instances that contain any of flag_names and have been running
    # longer than warn_time
    flag_names = ['awsem']
    warn_time = (datetime.datetime.now(datetime.timezone.utc) -
                 datetime.timedelta(days=7))
    fail_time = (datetime.datetime.now(datetime.timezone.utc) -
                 datetime.timedelta(days=14))
    ec2_res = client.describe_instances(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
    )
    check.full_output = []
    check.brief_output = {'one_week': [], 'two_weeks': []}
    for ec2_info in ec2_res.get('Reservations', []):
        instances = ec2_info.get('Instances', [])
        if not instances:
            continue
        # for multiple instance (?) just check if any of them require warnings
        for ec2_inst in instances:
            state = ec2_inst.get('State')
            created = ec2_inst.get('LaunchTime')
            if not state or not created:
                continue
            inst_name = [kv['Value'] for kv in ec2_inst.get('Tags', [])
                         if kv['Key'] == 'Name']
            other_tags = {kv['Key']: kv['Value'] for kv in ec2_inst.get('Tags', [])
                         if kv['Key'] != 'Name'}
            ec2_log = {
                'state': state['Name'], 'name': inst_name,
                'id': ec2_inst.get('InstanceId'),
                'type': ec2_inst.get('InstanceType'),
                'date_created_utc': created.strftime('%Y-%m-%dT%H:%M')
            }
            if not inst_name:
                flag_instance = True
                # include all other tags if Name tag is empty
                ec2_log['tags'] = other_tags
            elif any([wn for wn in flag_names if wn in ','.join(inst_name)]):
                flag_instance = True
            else:
                flag_instance = False
            # see if long running instances are associated with a deleted WFR
            if flag_instance and inst_name and created < warn_time:
                search_url = 'search/?type=WorkflowRunAwsem&awsem_job_id='
                search_url += '&awsem_job_id='.join([name[6:] for name in inst_name if name.startswith('awsem-')])
                wfrs = ff_utils.search_metadata(search_url, key=connection.ff_keys)
                if wfrs:
                    ec2_log['active workflow runs'] = [wfr['@id'] for wfr in wfrs]
                deleted_wfrs = ff_utils.search_metadata(search_url + '&status=deleted', key=connection.ff_keys)
                if deleted_wfrs:
                    ec2_log['deleted workflow runs'] = [wfr['@id'] for wfr in deleted_wfrs]
            # always add record to full_output; add to brief_output if
            # the instance is flagged based on 'Name' tag
            if created < fail_time:
                if flag_instance:
                    check.brief_output['two_weeks'].append(ec2_log)
                check.full_output.append(ec2_log)
            elif created < warn_time:
                if flag_instance:
                    check.brief_output['one_week'].append(ec2_log)
                check.full_output.append(ec2_log)

    if check.brief_output['one_week'] or check.brief_output['two_weeks']:
        num_1wk = len(check.brief_output['one_week'])
        num_2wk = len(check.brief_output['two_weeks'])
        check.summary = ''
        if check.brief_output['two_weeks']:
            check.status = 'FAIL'
            check.summary = '%s suspect EC2s running longer than 2 weeks' % num_2wk
        if check.brief_output['one_week']:
            if check.status != 'FAIL':
                check.status = 'WARN'
            if check.summary:
                check.summary += ' and %s others longer than 1 week' % num_1wk
            else:
                check.summary = '%s suspect EC2s running longer than 1 week' % num_1wk
        check.description = check.summary + '. Flagged because name is empty or contains %s. There are also %s non-flagged instances.' % (flag_names, len(check.full_output) - (num_1wk + num_2wk))
    else:
        check.status = 'PASS'
        check.summary = '%s EC2s running longer than 1 week' % (len(check.full_output))
    return check


@check_function(FS_dev='free', FF_hotseat='free', FF_mastertest='free', FF_webdev='free')
def say_my_name(connection, **kwargs):
    """List the person working on each environment."""
    check = CheckResult(connection, 'say_my_name')
    check.description = "Enter the new name or if you are done, use 'free' to clear your name"
    check.summary = ""
    check.brief_output = ""
    check.status = "PASS"
    output = {}
    # update with the new parameters ()
    for a_key in ['FS_dev', 'FF_hotseat', 'FF_mastertest', 'FF_webdev']:
        if kwargs.get(a_key):
            val = kwargs[a_key]
            if val.lower() == 'free':
                val = 'free'
            output[a_key] = val
        else:
            output[a_key] = 'free'
    sum = str(output)[1:-1].replace("'", "")
    check.summary = sum
    check.brief_output = sum
    return check
