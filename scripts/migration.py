import app
import datetime
import boto3
import json
import sys
from chalicelib.es_connection import ESConnection

# XXX: To use this script, run 'python migration.py <env> <stage>' in the root
# directory of this repository.
# Set MIGRATE to false to trigger an s3 clean
# Set PRIMARY to true to keep primary results while doing a clean
# You can also do - 'python migration.py' whe MIGRATE is true to automatically
# migrate all checks from all environments into the appropriate ES index
# This script reall just refactors the functionality in RunResult.delete_results
# and calls the migration check in es_checks.py.

ENVS = ['mastertest', 'hotseat', 'webdev', 'staging', 'cgap', 'data']
STAGES = ['dev', 'prod']
ONE_WEEK_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=7)
MIGRATE = True # set this option based on what you want to do
PRIMARY = False # set to true if you want to keep primary results

def filename_to_datetime(key):
    try:
        name, tstamp = key.split('/')
        time, sec, _ = tstamp.split('.')
        return datetime.datetime.strptime('.'.join([time, sec]), '%Y-%m-%dT%H:%M:%S.%f')
    except:
        return None

def filt(k):
    t = filename_to_datetime(k)
    if t is None:
        return False
    else:
        return t < ONE_WEEK_AGO

def clean(env, stage):
    bucket_name = 'foursight-' + stage + '-' + env
    bucket = boto3.resource('s3').Bucket(bucket_name)
    client = boto3.client('s3')
    keys = []
    for obj in bucket.objects.all():
        keys.append(obj.key)

    keys_to_delete = list(filter(filt, keys))

    # filter on primary
    def is_not_primary(key):
        try:
            obj = json.loads(client.get_object(Bucket=bucket_name, Key=key)['Body'].read())
            return not obj['kwargs'].get('primary')
        except:
            return False

    # if primary is set to true in file then we keep them
    if PRIMARY:
        keys_to_delete = list(filter(is_not_primary, keys_to_delete))

    print("Total keys: %s" % len(keys))
    print("Keys to delete: %s" % len(keys_to_delete))
    for i in range(0, len(keys_to_delete), 1000):
        fmt = {'Objects': [{'Key': key} for key in keys_to_delete[i:i+1000]]}
        client.delete_objects(Bucket=bucket_name, Delete=fmt)

def migrate(env, stage):
    index_name = 'foursight-' + stage + '-' + env

    app.set_stage(stage)
    app.set_timeout(0)
    es = ESConnection(index=index_name)
    conn = app.init_connection(env)
    args = {'timeout': 1000000, 'check_name': None, 'called_by': None}
    migrate = app.run_check_or_action(conn, 'es_checks/migrate_checks_to_es', args)
    diff = app.run_check_or_action(conn, 'es_checks/elasticsearch_s3_count_diff', {})
    print(migrate)
    print(diff)

def main():
    if MIGRATE:
        if len(sys.argv) < 3:
            for env in ENVS:
                for stage in STAGES:
                    migrate(env, stage)
        else:
            migrate(sys.argv[1], sys.argv[2])
        exit(0)
    else:
        clean(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
