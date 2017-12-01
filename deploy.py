"""
Generate gitignored .chalice/config.json for deploy and then run deploy.
Takes on parameter for now: stage (either "dev" or "prod")
"""

from __future__ import print_function, unicode_literals
import os
import argparse
import json
import subprocess

CONFIG_BASE = {
  "stages": {
    "dev": {
      "api_gateway_stage": "api",
      "autogen_policy": False,
      "lambda_memory_size": 256,
      "lambda_timeout": 300,
      "environment_variables": {
          "chalice_stage": "dev"
      }
    },
    "prod": {
      "api_gateway_stage": "api",
      "autogen_policy": False,
      "lambda_memory_size": 256,
      "lambda_timeout": 300,
      "environment_variables": {
          "chalice_stage": "prod"
      }
    }
  },
  "version": "2.0",
  "app_name": "foursight"
}


def build_config_and_deploy(stage):
    CONFIG_BASE['stages']['dev']['environment_variables']['SECRET'] = os.environ.get("SECRET", "")
    CONFIG_BASE['stages']['prod']['environment_variables']['SECRET'] = os.environ.get("SECRET", "")
    file_dir, _ = os.path.split(os.path.abspath(__file__))
    filename = os.path.join(file_dir, '.chalice/config.json')
    print(''.join(['Writing: ', filename]))
    with open(filename, 'w') as config_file:
        config_file.write(json.dumps(CONFIG_BASE))
    # actually deploy
    subprocess.call(['chalice', 'deploy', '--stage', stage])


def main():
    parser = argparse.ArgumentParser('chalice_deploy')
    parser.add_argument(
        "stage",
        type=str,
        choices=['dev', 'prod'],
        help="chalice deployment stage. Must be one of 'prod' or 'dev'")
    args = parser.parse_args()
    build_config_and_deploy(args.stage)


if __name__ == '__main__':
    main()
