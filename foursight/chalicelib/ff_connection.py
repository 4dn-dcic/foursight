from __future__ import print_function, unicode_literals
import requests
import json
import datetime
from .s3_connection import S3Connection

class FFConnection(object):
    def __init__(self, server, bucket, es):
        self.headers = {'content-type': 'application/json', 'accept': 'application/json'}
        self.server = server
        self.s3connection = S3Connection(bucket)
        self.es = es
        self.is_up = self.test_connection()
        self.auth = self.get_auth()


    def test_connection(self):
        # check connection
        try:
            head_resp = requests.head(self.server)
        except:
            return False
        return True if head_resp.status_code == 200 else False


    def get_auth(self):
        # authorization info is currently held in s3
        # this is probably (definitely) not the best way to go
        auth_res = json.loads(self.s3connection.get_object('auth'))
        if auth_res.get('ResponseMetadata') == 'ClientError':
            return ()
        else:
            key = auth_res.get('key')
            secret = auth_res.get('secret')
            return (key, secret) if key and secret else ()


    def get_auth_user(self):
        me_res = json.loads(requests.get(''.join([self.server,'me']), auth=self.auth))
        return me_res
