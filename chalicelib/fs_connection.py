from __future__ import print_function, unicode_literals
import requests
import json
import datetime
from .s3_connection import S3Connection
from wranglertools import fdnDCIC

class FSConnection(object):
    # def __init__(self, environ, server, bucket, es):
    def __init__(self, fs_environ, fs_environ_info):
        self.fs_environment = fs_environ
        self.ff = fs_environ_info.get('fourfront')
        self.ff_env = fs_environ_info.get('ff_env')
        self.s3_connection = S3Connection(fs_environ_info.get('bucket'))
        self.es = fs_environ_info.get('es')
        self.is_up = self.test_ff_connection()


    def test_ff_connection(self):
        # see if status == 200 for local_server
        # this won't catch many errors; is more meant to see if it exists
        try:
            head_resp = requests.head(self.ff)
        except:
            return False
        return True if head_resp.status_code == 200 else False
