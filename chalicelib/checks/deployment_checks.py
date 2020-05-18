import os
import shutil
import boto3
import datetime
import tempfile
from git import Repo

from ..run_result import CheckResult, ActionResult
from ..utils import check_function, action_function
from dcicutils.deployment_utils import EBDeployer
from dcicutils.env_utils import (
    FF_ENV_INDEXER, CGAP_ENV_INDEXER, is_indexer_env, is_fourfront_env, is_cgap_env
)
from dcicutils.beanstalk_utils import beanstalk_info, is_indexing_finished


def try_to_describe_indexer_env(env):
    """ Small helper that wraps beanstalk_info so we can recover from exceptions
        XXX: Fix beanstalk_info so it will not throw IndexError if you give it bad env
    """
    try:
        return beanstalk_info(env)
    except IndexError:
        return None  # env does not exist
    except Exception:
        raise  # something else happened we should (probably) raise


def clone_repo_to_temporary_dir(repo='https://github.com/4dn-dcic/fourfront.git', name='fourfront'):
    """ Clones the given repo (default fourfront) to a temporary directory whose
        absolute path is returned.
    """
    tempdir = tempfile.mkdtemp()
    Repo.clone_from(url=repo, to_path=tempdir, branch='master')
    return os.path.join(tempdir, './fourfront')


def cleanup_tempdir(tempdir):
    """ Wrapper for shutil.rmtree that cleans up the (repo dir) we just created """
    shutil.rmtree(tempdir)


@check_function(env=FF_ENV_INDEXER)  # XXX: Scope?
def indexer_server_status(connection, **kwargs):
    """ Checks the status of index servers (if any are up) """
    check = CheckResult(connection, 'indexer_server_status')
    check.action = 'terminate_indexer_server'
    env = kwargs.get('env')

    if not is_indexer_env(env):
        raise RuntimeError('Bad EB env passed to check, should be one of: %s, %s\n'
                           'But got: %s' % (FF_ENV_INDEXER, CGAP_ENV_INDEXER, env))

    # Get info about indexer env, check indexing status
    description = try_to_describe_indexer_env(env)
    if description is not None:
        indexing_finished, counts = is_indexing_finished(env)
        if indexing_finished is True:
            check.status = 'PASS'
            check.summary = 'Env %s has finished indexing and is ready to be terminated' % env
            check.action_message = 'Will terminate indexer env: %s' % env
            check.allow_action = True  # allow us to terminate
        else:
            check.status = 'WARN'
            check.allow_action = False
            check.summary = 'Env %s has not finished indexing with remaining counts %s' % (env, counts)
    else:
        check.status = 'PASS'
        check.summary = 'Did not find an online indexer with the given name: %s, so no status to check.' % env

    check.full_output = env  # full_output contains env we checked
    return check


@action_function()
def terminate_indexer_server(connection, **kwargs):
    """ Terminates the index_server stored in the full_output member of the associated check above """
    action = ActionResult(connection, 'terminate_indexer_server')
    related_indexer_server_status = action.get_associated_check_result(kwargs)
    env_to_terminate = related_indexer_server_status.get('full_output', None)

    if env_to_terminate:
        client = boto3.client('elasticbeanstalk')
        success = EBDeployer.terminate_indexer_env(client, env_to_terminate)
        if success:
            action.status = 'DONE'
            action.output = 'Successfully triggered termination of indexer env %s' % env_to_terminate
        else:
            action.status = 'FAIL'
            action.output = 'Encountered an error while issuing terminate call, please check the AWS EB Console'
    else:
        action.status = 'FAIL'
        action.output = 'Did not get an env_to_terminate from the associated check - bad action call?'

    return action


@check_function(env='fourfront-green', application_version=None)
def provision_indexer_environment(connection, **kwargs):
    """ Provisions an indexer environment for the given env. Note that only one indexer can be online
        per application (one for 4DN, one for CGAP).

        NOTE: env is an EB ENV NAME ('data', 'staging' are NOT valid).
    """
    check = CheckResult(connection, 'provision_indexer_environment')
    env = kwargs.get('env', None)
    application_version = kwargs.get('application_version', None)

    if application_version is None:
        check.status = 'ERROR'
        check.summary = 'Did not provide application_version to deploy indexer, which is required. ' \
                        'Get this information from the EB Console.'
        return check

    def _deploy_indexer(e, version):
        description = try_to_describe_indexer_env(e)
        if description is not None:
            check.status = 'ERROR'
            check.summary = 'Tried to spin up indexer env for %s when one already exists for this portal' % e
            return False
        else:
            return EBDeployer.deploy_indexer(e, version)

    if is_cgap_env(env) or is_fourfront_env(env):
        success = _deploy_indexer(env, application_version)
        if success:
            check.status = 'PASS'
            check.summary = 'Successfully triggered indexer-server provision for environment %s' % env
    else:
        check.status = 'FAIL'
        check.summary = 'Gave an unknown environment: %s' % env

    return check


@check_function(env=None, branch=None, application_version_name=None, repo=None)
def deploy_application_to_beanstalk(connection, **kwargs):
    """ Deploys application to beanstalk, given an env and a branch """
    check = CheckResult(connection, 'deploy_application_to_beanstalk')
    env = kwargs.get('env', 'fourfront-mastertest')  # by default
    branch = kwargs.get('branch', 'master')  # by default deploy master
    application_version_name = kwargs.get('application_version_name', None)
    repo = kwargs.get('repo', None)

    if application_version_name is None:  # if not specified, use branch+timestamp
        application_version_name = 'foursight-package-%s-%s' % (branch, datetime.datetime.utcnow())

    if repo is not None:
        repo_location = clone_repo_to_temporary_dir(repo)
    else:
        repo_location = clone_repo_to_temporary_dir()

    packaging_was_successful = EBDeployer.build_application_version(repo_location,
                                                                    application_version_name,
                                                                    branch=branch)
    if packaging_was_successful:
        try:
            deploy_succeeded = EBDeployer.deploy_new_version(env, repo_location, application_version_name)
            if deploy_succeeded:
                check.status = 'PASS'
                check.summary = 'Successfully deployed version %s to env %s' % (application_version_name, env)
            else:
                check.status = 'WARN'
                check.summary = 'Something went wrong when provisioning: %s' \
                                'Please check the AWS EB Console.' % deploy_succeeded
        except Exception as e:
            check.status = 'ERROR'
            check.summary = 'Error encountered while deploying: %s' % str(e)
    else:
        check.status = 'ERROR'
        check.summary = 'Could not package repository: %s' % packaging_was_successful

    cleanup_tempdir(os.path.join(repo_location, '../'))  # cleanup tempdir that houses repo
    return check
