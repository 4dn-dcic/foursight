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


    def list_objects(self, prefix):
        return [obj for obj in self.client.list_objects(Bucket=self.bucket, Prefix=prefix)]


    def test_connection(self):
        try:
            bucket_resp = self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            return {'ResponseMetadata': {'HTTPStatusCode': 404}}
        return bucket_resp


    def create_bucket(self):
        # us-east-1 is default location
        # add CreateBucketConfiguration w/ Location key for a different region
        self.client.create_bucket(Bucket=self.bucket)
