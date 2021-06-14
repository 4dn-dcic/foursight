import boto3
import json
from chalicelib.vars import FOURSIGHT_PREFIX
from foursight_core.buckets import Buckets as Buckets_from_core
from dcicutils import env_utils


class Buckets(Buckets_from_core):
    """create and configure buckets for foursight"""

    prefix = FOURSIGHT_PREFIX
    envs = ['data', 'hotseat', 'mastertest', 'staging', 'webdev']

    def ff_env(self, env):
        if env_utils.is_stg_or_prd_env(env):  # data or staging
            return env
        else: 
            return 'fourfront-%s' % env

    def ff_url(self, env):
        if env == 'data':
            return 'https://data.4dnucleome.org/'
        elif env == 'staging':
            return 'https://staging.4dnucleome.org/'
        else:
            return 'http://%s.9wzadzju3p.us-east-1.elasticbeanstalk.com/' % self.ff_env(env)

    def es_url(self, env):
        # WARNING: the es url of data and staging may be swapped depending on which state it is currently.
        if env == 'data':
            return "https://search-fourfront-blue-6-8-cghd4hoobl45t6cmvku4xc4y4i.us-east-1.es.amazonaws.com"
        if env == 'staging':
            return "search-fourfront-green-6-8-qfcturjlmonunnuqzyxxsvnmbu.us-east-1.es.amazonaws.com"
        else:
            return "https://search-fourfront-testing-6-8-kncqa2za2r43563rkcmsvgn2fq.us-east-1.es.amazonaws.com"


def main():
    buckets = Buckets()
    buckets.create_buckets()
    buckets.configure_env_bucket()


if __name__ == '__main__':
    main()
