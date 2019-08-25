from __future__ import print_function, unicode_literals
from .s3_connection import S3Connection
from dcicutils.s3_utils import s3Utils

class FSConnection(object):
    """
    Contains the foursight (FS) and fourfront (FF) connections needed to
    communicate with both services. Contains fields that link to the FF keys,
    and s3 connection, as well as the FS s3_connection. They are:
    - fs_env: string FS environment (such as 'data' or 'webdev')
    - ff_server: string server name of the linked FF
    - ff_env: string EB enviroment name of FF (such as 'fourfront-webprod').
              This is kept up-to-date for data and staging
    - ff_s3: s3Utils connection to the FF environment (see dcicutils.s3_utils)
    - ff_keys: FF keys for the environment with 'key', 'secret' and 'server'
    - ff_es: string server of the elasticsearch for the FF
    - s3_connection: S3Connection object that is the s3 connection for FS

    If param test=True, then do not actually attempt to initate the FF connections
    """
    def __init__(self, fs_environ, fs_environ_info, test=False):
        # FOURSIGHT information
        self.fs_env = fs_environ
        self.s3_connection = S3Connection(fs_environ_info.get('bucket'))

        # FOURFRONT information
        self.ff_server = fs_environ_info['fourfront']
        self.ff_env = fs_environ_info['ff_env']
        self.ff_es = fs_environ_info['es']
        if not test:
            self.ff_s3 = s3Utils(env=self.ff_env)
            try:
                self.ff_keys = self.ff_s3.get_access_keys()
            except Exception as e:
                raise Exception('Could not initiate connection to Fourfront; it is probably a bad ff_env. '
                      'You gave: %s. Error message: %s' % (self.ff_env, str(e)))
            # ensure ff_keys has server, and make sure it does not end with '/'
            if 'server' not in self.ff_keys:
                server = self.ff_server[:-1] if self.ff_server.endswith('/') else self.ff_server
                self.ff_keys['server'] = server
        else:
            self.ff_s3 = None
            self.ff_keys = None
