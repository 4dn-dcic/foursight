"""
Generate gitignored .chalice/config.json for deploy and then run deploy.
Takes on parameter for now: stage (either "dev" or "prod")
"""
from os.path import dirname
import argparse
from foursight_core.deploy import Deploy as Deploy_from_core


class Deploy(Deploy_from_core):

    CONFIG_BASE = Deploy_from_core.CONFIG_BASE
    CONFIG_BASE['app_name'] = 'foursight'

    config_dir = dirname(__file__)


def main():
    parser = argparse.ArgumentParser('chalice_deploy')
    parser.add_argument(
        "stage",
        type=str,
        choices=['dev', 'prod'],
        help="chalice deployment stage. Must be one of 'prod' or 'dev'")
    args = parser.parse_args()
    Deploy.build_config_and_deploy(args.stage)


if __name__ == '__main__':
    main()
