import sys
import datetime
import boto3
import json
import argparse
sys.path.append('..')
import app
from chalicelib_fourfront.vars import DEV_ENV


EPILOG = __doc__
ENVS = ['mastertest', 'hotseat', 'webdev', 'staging', 'data']
STAGES = ['dev', 'prod']


def setup_stage(stage):
    if not stage:
        app.set_stage('dev')
    else:
        if stage not in STAGES:
            print('Bad stage')
            exit(1)
        else:
            app.set_stage(stage)


def setup_env(env):
    if not env:
        connection = app.init_connection(DEV_ENV)
    else:
        if env not in ENVS:
            print('Bad env')
            exit(1)
        try:
            connection = app.init_connection(env)
        except:
            print('Could not establish connection to env: %s' % env)
            exit(1)
    return connection


def main():
    parser = argparse.ArgumentParser(
        description='Tests a check/action',
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('check', help='full check name')
    parser.add_argument('--env', help='env name, %s by default' % DEV_ENV)
    parser.add_argument('--stage', help='stage, dev or prod, dev by default')
    args = parser.parse_args()

    setup_stage(args.stage)
    connection = setup_env(args.env)

    # run the check
    # XXX: Configure using kwargs?
    try:
        res = app.run_check_or_action(connection, args.check, {})
    except:
        print('Failed to execute check name: %s' % args.check)
        exit(1)
    print(res)

if __name__ == '__main__':
    main()
