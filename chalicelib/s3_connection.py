from __future__ import print_function, unicode_literals
import requests
import boto3
import datetime

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
            return response['Body'].read()
        except:
            return None


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
        reached_end = False
        all_keys = []
        token = None # for the NextContinuationToken in s3 response
        # make sure prefix ends with a slash (bucket format)
        prefix = ''.join([prefix, '/']) if not prefix.endswith('/') else prefix
        # this will exclude 'primary' and 'latest' in records_only == True
        use_prefix = ''.join([prefix, '20' ])if records_only else prefix
        while not reached_end:
            try:
                # will limit to 1000 objects
                if token:
                    response = self.client.list_objects_v2(
                        Bucket=self.bucket,
                        Prefix=use_prefix,
                        ContinuationToken=token
                    )
                else:
                    response = self.client.list_objects_v2(
                        Bucket=self.bucket,
                        Prefix=use_prefix
                    )
                token = response.get('NextContinuationToken', None)
                contents = response.get('Contents', [])
            except:
                contents = []
                reached_end = True # bail
            all_keys.extend([obj['Key'] for obj in contents])
            if len(all_keys) > 0 and not token:
                reached_end = True
        # not sorted at this point
        return all_keys


    def list_all_keys(self):
        reached_end = False
        all_keys = []
        token = None
        while not reached_end:
            # will limit to 1000 objects
            response = self.client.list_objects_v2(Bucket=self.bucket)
            token = response.get('NextContinuationToken', None)
            contents = response.get('Contents', [])
            all_keys.extend([obj['Key'] for obj in contents])
            if not contents or (len(all_keys) > 0 and not token):
                reached_end = True
        return all_keys


    def delete_keys(self, key_list):
        # boto3 requires this setup
        to_delete = {'Objects' : [{'Key': key} for key in key_list]}
        self.client.delete_objects(Bucket=self.bucket, Delete=to_delete)


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
