from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res,
    basestring
)
from dcicutils import (
    ff_utils,
    es_utils,
    beanstalk_utils
)

import requests
import sys
import json
import datetime
import boto3
import time


@check_function()
def elastic_beanstalk_health(connection, **kwargs):
    """
    Check both environment health and health of individual instances
    """
    check = init_check_res(connection, 'elastic_beanstalk_health')
    full_output = {}
    eb_client = boto3.client('elasticbeanstalk')
    resp = eb_client.describe_environment_health(
        EnvironmentName=connection.ff_env,
        AttributeNames=['All']
    )
    resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    if resp_status >= 400:
        check.status = 'ERROR'
        check.description = 'Could not establish a connection to AWS (status %s).' % resp_status
        return check
    full_output['status'] = resp.get('Status')
    full_output['environment_name'] = resp.get('EnvironmentName')
    full_output['color'] = resp.get('Color')
    full_output['health_status'] = resp.get('HealthStatus')
    full_output['causes'] = resp.get('Causes')
    full_output['instance_health'] = []
    # now look at the individual instances
    resp = eb_client.describe_instances_health(
        EnvironmentName=connection.ff_env,
        AttributeNames=['All']
    )
    resp_status = resp.get('ResponseMetadata', {}).get('HTTPStatusCode', None)
    if resp_status >= 400:
        check.status = 'ERROR'
        check.description = 'Could not establish a connection to AWS (status %s).' % resp_status
        return check
    instances_health = resp.get('InstanceHealthList', [])
    for instance in instances_health:
        inst_info = {}
        inst_info['deploy_status'] = instance['Deployment']['Status']
        inst_info['deploy_version'] = instance['Deployment']['VersionLabel']
        # get version deployment time
        application_versions = eb_client.describe_application_versions(
            ApplicationName='4dn-web',
            VersionLabels=[inst_info['deploy_version']]
        )
        deploy_info = application_versions['ApplicationVersions'][0]
        inst_info['version_deployed_at'] = datetime.datetime.strftime(deploy_info['DateCreated'], "%Y-%m-%dT%H:%M:%S")
        inst_info['version_description'] = deploy_info['Description']
        inst_info['instance_deployed_at'] = datetime.datetime.strftime(instance['Deployment']['DeploymentTime'], "%Y-%m-%dT%H:%M:%S")
        inst_info['instance_launced_at'] = datetime.datetime.strftime(instance['LaunchedAt'], "%Y-%m-%dT%H:%M:%S")
        inst_info['id'] = instance['InstanceId']
        inst_info['color'] = instance['Color']
        inst_info['health'] = instance['HealthStatus']
        inst_info['causes'] = instance.get('causes', [])
        full_output['instance_health'].append(inst_info)
    if full_output['color'] == 'Grey':
        check.status = 'WARN'
        check.summary = check.description = 'EB environment is updating'
    elif full_output['color'] == 'Yellow':
        check.status = 'WARN'
        check.summary = check.description = 'EB environment is compromised; requests may fail'
    elif full_output['color'] == 'Red':
        check.status = 'FAIL'
        check.summary = check.description = 'EB environment is degraded; requests are likely to fail'
    else:
        check.summary = check.description = 'EB environment seems healthy'
        check.status = 'PASS'
    check.full_output = full_output
    return check


@check_function()
def status_of_elasticsearch_indices(connection, **kwargs):
    check = init_check_res(connection, 'status_of_elasticsearch_indices')
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
    check = init_check_res(connection, 'indexing_records')
    client = es_utils.create_es_client(connection.ff_es, True)
    res = client.search(index='indexing', doc_type='indexing', sort='uuid:desc', size=1000,
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
    check = init_check_res(connection, 'staging_deployment')
    return check


@check_function()
def fourfront_performance_metrics(connection, **kwargs):
    check = init_check_res(connection, 'fourfront_performance_metrics')
    full_output = {}  # contains ff_env, env_health, deploy_version, num instances, and performance
    performance = {}  # keyed by check_url
    # get information from elastic_beanstalk_health
    eb_check = init_check_res(connection, 'elastic_beanstalk_health')
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
            ff_resp = ff_utils.authorized_request(connection.ff_server + check_url, ff_env=connection.ff_env, timeout=1000)
        except Exception as e:
            performance[check_url]['error'] = str(e)
        if ff_resp and hasattr(ff_resp, 'headers') and 'X-stats' in ff_resp.headers:
            x_stats = ff_resp.headers['X-stats']
            if not isinstance(x_stats, basestring):
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


@check_function()
def secondary_queue_deduplication(connection, **kwargs):
    from ..utils import get_stage_info
    check = init_check_res(connection, 'secondary_queue_deduplication')
    # maybe handle this in check_setup.json
    if get_stage_info()['stage'] != 'prod':
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
    time_limit = 240  # 4 minutes
    t0 = time.time()
    sent = 0
    deleted = 0
    deduplicated = 0
    total_msgs = 0
    replaced = 0
    repeat_replaced = 0
    problem_msgs = []
    done = False
    elapsed = round(time.time() - t0, 2)
    failed = []
    seen_uuids = set()
    exit_reason = 'out of time'
    dedup_msg = 'Deduplicated: %s' % kwargs['uuid']
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
            WaitTimeSeconds=4  # 4 seconds of long polling
        )
        batch = recieved.get("Messages", [])
        if not batch:  # if no messages received from long polling, exit
            exit_reason = 'no messages left'
            break
        for i in range(len(batch)):
            try:
                msg_body = json.loads(batch[i]['Body'])
            except json.JSONDecodeError:
                problem_msgs.append(batch[i]['Body'])
                continue
            total_msgs += 1
            msg_uuid = msg_body['uuid']
            to_process = {
                'Id': batch[i]['MessageId'],
                'ReceiptHandle': batch[i]['ReceiptHandle']
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
                send_info = {
                    'Id': str(int(time.time() * 1000000)),
                    'MessageBody': json.dumps(msg_body)
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


@check_function()
def clean_up_travis_queues(connection, **kwargs):
    """
    Clean up old sqs queues based on the name ("travis-job")
    and the creation date. Only run on data for now
    """
    from ..utils import get_stage_info
    check = init_check_res(connection, 'clean_up_travis_queues')
    check.status = 'PASS'
    if connection.fs_env != 'data' or get_stage_info()['stage'] != 'prod':
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
            if isinstance(creation, basestring):
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


@check_function()
def manage_old_filebeat_logs(connection, **kwargs):
    # import curator
    check = init_check_res(connection, 'manage_old_filebeat_logs')

    # temporary -- disable this check
    check.status = 'PASS'
    check.description = 'Not currently running this check'
    return check

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
    from ..utils import get_stage_info
    check = init_check_res(connection, 'snapshot_rds')
    if get_stage_info()['stage'] != 'prod':
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check
    rds_name = 'fourfront-webprod' if 'webprod' in connection.ff_env else connection.ff_env
    # snapshot ID can only have letters, numbers, and hyphens
    snap_time = datetime.datetime.strptime(kwargs['uuid'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%dT%H-%M-%S")
    snapshot_name = 'foursight-snapshot-%s-%s' % (rds_name, snap_time)
    res = beanstalk_utils.create_db_snapshot(rds_name, snapshot_name)
    if res == 'Deleting':
        check.status = 'FAIL'
        check.summary = check.description = 'Something went wrong during snaphot creation'
    else:
        check.status = 'PASS'
        # there is a datetime in the response that must be str formatted
        res['DBSnapshot']['InstanceCreateTime'] = str(res['DBSnapshot']['InstanceCreateTime'])
        check.full_output = res
        check.summary = 'Snapshot successfully created'
        check.description = 'Snapshot succesfully created with name: %s' % snapshot_name
    return check


@check_function()
def process_download_tracking_items(connection, **kwargs):
    """
    Do a few things here, and be mindful of the 5min lambda limit.
    - Consolidate tracking items with download_tracking.range_query=True
    - Change remote_ip to geo_country and geo_city
    - If the user_agent looks to be a bot, set status=deleted
    - Change unused range query items to status=deleted
    """
    from ..utils import get_stage_info, parse_datetime_to_utc
    import geocoder
    check = init_check_res(connection, 'process_download_tracking_items')
    # maybe handle this in check_setup.json
    if get_stage_info()['stage'] != 'prod':
        check.full_output = 'Will not run on dev foursight.'
        check.status = 'PASS'
        return check
    range_cache = {}
    range_consolidation_hrs = 1  # duration we want to consolidate range queries over
    cons_date = (datetime.datetime.utcnow() -
                 datetime.timedelta(hours=range_consolidation_hrs)).strftime('%Y-%m-%dT%H\:%M')
    range_search_query = ''.join(['search/?type=TrackingItem&tracking_type=download_tracking',
                                  '&download_tracking.is_visualization=True&sort=-date_created',
                                  '&status=released&q=date_created:>=', cons_date])
    cons_query = ff_utils.search_metadata(range_search_query, key=connection.ff_keys, ff_env=connection.ff_env)
    for tracking in cons_query:
        dl_info = tracking['download_tracking']
        range_key = '//'.join([dl_info['remote_ip'], dl_info['filename'],
                               dl_info['user_agent'], dl_info['user_uuid']])
        parsed_date = parse_datetime_to_utc(tracking['date_created'])
        if range_key in range_cache:  # this shouldn't happen
            continue
        range_cache[range_key] = [parsed_date]
    del cons_query
    ip_cache = {}
    time_limit = 240  # 4 minutes
    # list of strings used to flag user_agent as a bot. By no means complete
    bot_strings = ['bot', 'crawl', 'slurp', 'spider', 'mediapartners', 'ltx71']
    # need some sort of time-based window to consolidate range_queries within?
    t0 = time.time()
    elapsed = round(time.time() - t0, 2)
    # do not search tracking items made in the last <UNIT OF TIME>
    date_str = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H\:%M')
    search_query = ''.join([connection.ff_server, 'search/?type=TrackingItem',
                            '&tracking_type=download_tracking&download_tracking.geo_country=No+value',
                            '&status=in+review+by+lab&sort=-date_created'])
                            # '&sort=-date_created&q=date_created:>=', date_str])
    # batch large groups of tracking items at once to save time with geocoder
    search_gen = ff_utils.get_search_generator(search_query, auth=connection.ff_keys,
                                               ff_env=connection.ff_env, page_limit=500)

    ### TEMP
    user_cache = {}
    format_exp_cache = {}

    for page in search_gen:
        elapsed = round(time.time() - t0, 2)
        if elapsed > time_limit:
            break
        page_ips = set([tracking['download_tracking']['remote_ip'] for tracking in page])
        # transform all IP addresses into GEO information with a persistent connection
        with requests.Session() as session:
            for track_ip in page_ips:
                if track_ip in ip_cache:
                    continue
                geo = geocoder.ip(track_ip, session=session)
                geo_country = getattr(geo, 'country', 'Unknown')
                geo_city = getattr(geo, 'city', 'Unknown')
                geo_state = getattr(geo, 'state', None)
                if geo_state:
                    geo_city = ', '.join([geo_city, geo_state])
                # cache the geo info in an arbitrary form
                ip_cache[track_ip] = '//'.join([geo_city, geo_country])
        # iterate over the individual tracking items
        for tracking in page:
            dl_info = tracking['download_tracking']
            geo_info = ip_cache[dl_info['remote_ip']]
            dl_info['geo_city'], dl_info['geo_country'] = geo_info.split('//')
            patch_body = {'status': 'released', 'download_tracking': dl_info}

            ### START TEMP
            # temporary code to upgrade the following on fields:
            # user_email -> user_uuid, add experiment_type and file_format
            if 'user_email' in dl_info:
                if dl_info['user_email'] == 'anonymous':
                    dl_info['user_uuid'] = 'anonymous'
                else:
                    if dl_info['user_email'] in user_cache:
                        dl_info['user_uuid'] = user_cache[dl_info['user_email']]
                    else:
                        user = ff_utils.get_metadata('/users/' + dl_info['user_email'],
                                                     key=connection.ff_keys, ff_env=connection.ff_env)
                        user_cache[dl_info['user_email']] = user['uuid']
                        dl_info['user_uuid'] = user['uuid']
                del dl_info['user_email']

            if 'file_format' not in dl_info or 'experiment_type' not in dl_info:
                file_acc = dl_info['filename'].split('.')[0]
                if file_acc in format_exp_cache:
                    dl_info['file_format'] = format_exp_cache[file_acc]['file_format']
                    dl_info['experiment_type'] = format_exp_cache[file_acc]['experiment_type']
                else:
                    file_data = ff_utils.get_metadata(file_acc, key=connection.ff_keys, ff_env=connection.ff_env)
                    dl_info['file_format'] = file_data['file_format']['file_format']
                    if file_data['experiments']:
                        exp_type = file_data['experiments'][0]['experiment_type']
                    else:
                        exp_type = None
                    dl_info['experiment_type'] = exp_type
                    format_exp_cache[file_acc] = {'file_format': file_data['file_format']['file_format'],
                                                  'experiment_type': exp_type}
            ### END TEMP

            if any(bot_str in dl_info['user_agent'].lower() for bot_str in bot_strings):
                patch_body['status'] = 'deleted'
            if patch_body['status'] != 'deleted' and dl_info['range_query'] is True:
                # TODO: this approach may be somewhat problematic because previous
                # range queries are not taken into account; may create duplicate
                # consolidated items if visualization takes place after check starts
                range_key = '//'.join([dl_info['remote_ip'], dl_info['filename'],
                                       dl_info['user_agent'], dl_info['user_uuid']])
                parsed_date = parse_datetime_to_utc(tracking['date_created'])
                if range_key in range_cache:
                    # for all reference range queries with this info, see if this one
                    # was created within one hour of it. if so, it is redundant and delete
                    for range_reference in range_cache[range_key]:
                        compare_date = range_reference - datetime.timedelta(hours=range_consolidation_hrs)
                        if parsed_date > compare_date:
                            patch_body['status'] = 'deleted'
                            break
                    if patch_body['status'] != 'deleted':
                        range_cache[range_key].append(parsed_date)
                        patch_body['is_visualization'] = True
                else:
                    # set the upper limit for for range queries to consolidate
                    range_cache[range_key] = [parsed_date]
                    patch_body['is_visualization'] = True
            print('\n\n PATCHING:\n%s\n' % patch_body)
        import pdb; pdb.set_trace()
        pass
