"""
Generate gitignored .chalice/config.json for deploy and then run deploy.
Takes on parameter for now: stage (either "dev" or "prod")
"""
from os.path import dirname
import argparse
from foursight_core.package import PackageDeploy as PackageDeploy_from_core
from foursight_core.package import main as main_from_core


class PackageDeploy(PackageDeploy_from_core):

    CONFIG_BASE = PackageDeploy_from_core.CONFIG_BASE
    CONFIG_BASE['app_name'] = 'foursight-fourfront'

    config_dir = dirname(dirname(__file__))


def main():
    main_from_core(package_deploy=PackageDeploy)


if __name__ == '__main__':
    main()
