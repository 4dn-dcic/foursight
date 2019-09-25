from __future__ import print_function, unicode_literals
from .abstract_connection import AbstractConnection
import json
import requests
import boto3
import datetime

class S3Connection(AbstractConnection):
    def __init__(self, bucket_name):
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.cw = boto3.client('cloudwatch') # for s3 bucket stats
        self.bucket = bucket_name
        self.location = 'us-east-1'
        # create the bucket if it doesn't exist
        self.head_info = self.test_connection()
        self.status_code = self.head_info.get('ResponseMetadata', {}).get("HTTPStatusCode", 404)
        if self.status_code == 404:
            self.create_bucket()
            # get head_info again
            self.head_info = self.test_connection()
            self.status_code = self.head_info.get('ResponseMetadata', {}).get("HTTPStatusCode", 404)

    def put_object(self, key, value):
        try:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=value)
        except:
            return None
        else:
            return (key, value)

    def get_object(self, key):
        # return found bucket content or None on an error
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            body = response['Body'].read()
            return json.loads(body)
        except json.JSONDecodeError:
            return body
        except:
            return None

    def get_size(self):
        """
        Gets the number of keys stored on this s3 connection. This is a very slow
        operation since it has to enumerate all keys.
        """
        bucket = self.resource.Bucket(self.bucket)
        return sum(1 for _ in bucket.objects.all())

    def get_size_bytes(self):
        """
        Uses CloudWatch client to get the bucket size in bytes of this bucket.
        Start and EndTime represent the window on which the bucket size will be
        calculated. An average is taken across the entire window (Period=86400)
        Useful for checks - may need further configuration
        """
        now = datetime.datetime.utcnow()
        resp = self.cw.get_metric_statistics(Namespace='AWS/S3',
                                             MetricName='BucketSizeBytes',
                                             Dimensions=[
                                            {'Name': 'BucketName', 'Value': self.bucket},
                                            {'Name': 'StorageType', 'Value': 'StandardStorage'}],
                                            Statistics=['Average'],
                                            Period=86400,
                                            StartTime=(now-datetime.timedelta(days=1)).isoformat(),
                                            EndTime=now.isoformat())
        return resp['Datapoints']

    def list_all_keys_w_prefix(self, prefix, records_only=False):
        """
        List all s3 keys with the given prefix (should look like
        '<prefix>/'). If records_only == True, then add '20' to the end of
        the prefix to only find records that are in timestamp form (will
        exclude 'latest' and 'primary'.)
        s3 only returns up to 1000 results at once, hence the need for the
        for loop. NextContinuationToken shows if there are more results to
        return.

        Returns the list of keys.

        Also see list_all_keys()
        """
        if not self.bucket:
            return []
        all_keys = []
        # make sure prefix ends with a slash (bucket format)
        prefix = ''.join([prefix, '/']) if not prefix.endswith('/') else prefix
        # this will exclude 'primary' and 'latest' in records_only == True
        # use '2' because is is the first digit of year (in uuid)
        use_prefix = ''.join([prefix, '2' ])if records_only else prefix
        bucket = self.resource.Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix=use_prefix):
            all_keys.append(obj.key)

        # not sorted at this point
        return all_keys

    def list_all_keys(self):
        if not self.bucket:
            return []
        all_keys = []
        bucket = self.resource.Bucket(self.bucket)
        for obj in bucket.objects.all():
            all_keys.append(obj.key)
        return all_keys

    def delete_keys(self, key_list):
        # boto3 requires this setup
        to_delete = {'Objects' : [{'Key': key} for key in key_list]}
        return self.client.delete_objects(Bucket=self.bucket, Delete=to_delete)

    def test_connection(self):
        try:
            bucket_resp = self.client.head_bucket(Bucket=self.bucket)
        except:
            return {'ResponseMetadata': {'HTTPStatusCode': 404}}
        return bucket_resp

    def create_bucket(self, manual_bucket=None):
        # us-east-1 is default location
        # add CreateBucketConfiguration w/ Location key for a different region
        # echoes bucket name if successful, None otherwise
        bucket = manual_bucket if manual_bucket else self.bucket
        try:
            self.client.create_bucket(Bucket=bucket)
        except:
            return None
        else:
            return bucket
