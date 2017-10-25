from __future__ import print_function, unicode_literals
import requests
import boto3
from botocore.exceptions import ClientError

class S3Connection(object):
    def __init__(self, bucket_name):
        self.client = boto3.client('s3')
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
        self.client.put_object(Bucket=self.bucket, Key=key, Body=value)


    def get_object(self, key):
        # return found bucket content or None on an error
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            return None


    def list_keys_w_prefix(self, prefix):
        contents = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix).get('Contents', [])
        return [obj['Key'] for obj in contents]


    def list_all_keys(self):
        contents = self.client.list_objects_v2(Bucket=self.bucket).get('Contents', [])
        return [obj['Key'] for obj in contents]


    def delete_keys(self, key_list):
        # boto3 requires this setup
        to_delete = {'Objects' : [{'Key': key} for key in key_list]}
        self.client.delete_objects(Bucket=self.bucket, Delete=to_delete)


    def test_connection(self):
        try:
            bucket_resp = self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            return {'ResponseMetadata': {'HTTPStatusCode': 404}}
        return bucket_resp


    def create_bucket(self, manual_bucket=None):
        # us-east-1 is default location
        # add CreateBucketConfiguration w/ Location key for a different region
        bucket = manual_bucket if manual_bucket else self.bucket
        self.client.create_bucket(Bucket=bucket)
