from __future__ import print_function, unicode_literals
from .s3_connection import S3Connection

class FSConnection(object):
    def __init__(self, fs_environ, fs_environ_info):
        self.fs_environment = fs_environ
        self.ff = fs_environ_info.get('fourfront')
        self.ff_env = fs_environ_info.get('ff_env')
        self.s3_connection = S3Connection(fs_environ_info.get('bucket'))
        self.es = fs_environ_info.get('es')
