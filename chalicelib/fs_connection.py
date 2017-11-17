from __future__ import print_function, unicode_literals
import requests
import json
import datetime
from .s3_connection import S3Connection
from .ff_utils import fdn_connection
from wranglertools import fdnDCIC

class FSConnection(object):
    def __init__(self, environ, server, bucket, es):
        self.environment = environ
        self.headers = {'content-type': 'application/json', 'accept': 'application/json'}
        self.server = server
        self.s3_connection = S3Connection(bucket)
        self.es = es
        self.is_up = self.test_connection()
        self.ff_connection = self.get_ff_connection()


    def test_connection(self):
        # check connection
        try:
            head_resp = requests.head(self.server)
        except:
            return False
        return True if head_resp.status_code == 200 else False


    def get_ff_connection(self):
        # authorization info is currently held in s3
        # return a key that works with ff_utils/wranglertools.fdnDCIC
        auth_res = self.s3_connection.get_object('auth')
        if auth_res is None:
            return None
        else:
            auth_res = json.loads(auth_res)
            key = auth_res.get('key')
            secret = auth_res.get('secret')
            key_dict = {
                'default': {
                    'key': key,
                    'secret': secret,
                    'server': self.server
                }
            }
            return fdn_connection(key_dict)



    def get_auth_user(self):
        me_res = json.loads(requests.get(''.join([self.server,'me']), auth=self.auth))
        return me_res
