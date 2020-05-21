import time
import boto3
import shutil
import datetime
import tempfile
from git import Repo

from ..run_result import CheckResult, ActionResult
from ..utils import check_function, action_function
from dcicutils.deployment_utils import EBDeployer
from dcicutils.env_utils import (
    FF_ENV_INDEXER, CGAP_ENV_INDEXER, is_fourfront_env, is_cgap_env,
    FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN
)
from dcicutils.beanstalk_utils import compute_ff_prd_env
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
    tempdir = tempfile.mkdtemp(prefix=name)
    Repo.clone_from(url=repo, to_path=tempdir)
    return tempdir


def cleanup_tempdir(tempdir):
    """ Wrapper for shutil.rmtree that cleans up the (repo dir) we just created """
    shutil.rmtree(tempdir)


@check_function(env='fourfront-hotseat')
def indexer_server_status(connection, **kwargs):
    """ Checks the status of index servers (if any are up) """
    check = CheckResult(connection, 'indexer_server_status')
    check.action = 'terminate_indexer_server'
    env = kwargs.get('env')
    indexer_env = FF_ENV_INDEXER if is_fourfront_env(env) else CGAP_ENV_INDEXER
    description = try_to_describe_indexer_env(indexer_env)  # verify an indexer is online
    if description is None:
        check.status = 'ERROR'
        check.summary = 'Did not locate corresponding indexer env: ' \
                        'given env %s does not have indexer %s' % (env, indexer_env)
        check.allow_action = False
        return check

    try:
        indexing_finished, counts = is_indexing_finished(env)
    except Exception as e:  # XXX: This should be handled in dcicutils -Will 5/18/2020
        check.status = 'ERROR'
        check.summary = 'Failed to get indexing status of given env: %s' % str(e)
        check.allow_action = False
        return check

    if indexing_finished is True:
        check.status = 'PASS'
        check.summary = 'Env %s has finished indexing and is ready to be terminated' % env
        check.action_message = 'Safe to terminate indexer env: %s since indexing is finished' % env
        check.allow_action = True  # allow us to terminate
    else:
        check.status = 'WARN'
        check.allow_action = True
        check.action_message = 'Will terminate indexer env %s when indexing is not done yet!' % env
        check.summary = 'Env %s has not finished indexing with remaining counts %s' % (env, ' '.join(counts))

    check.full_output = indexer_env  # full_output contains indexer_env we checked
    return check


@action_function(forced_env=None)
def terminate_indexer_server(connection, **kwargs):
    """ Terminates the index_server stored in the full_output member of the associated check above """
    action = ActionResult(connection, 'terminate_indexer_server')
    related_indexer_server_status = action.get_associated_check_result(kwargs)
    forced_env = kwargs.get('forced_env', None)
    env_to_terminate = related_indexer_server_status.get('full_output', None) if forced_env is None else forced_env

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


@check_function(env='fourfront-hotseat', application_version=None)
def provision_indexer_environment(connection, **kwargs):
    """ Provisions an indexer environment for the given env. Note that only one indexer can be online
        per application (one for 4DN, one for CGAP).

        IMPORTANT: env is an EB ENV NAME ('data', 'staging' are NOT valid).
        IMPORTANT: This ecosystem will break some integrated tests in dcicutils
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
        if is_fourfront_env(e):
            description = try_to_describe_indexer_env(FF_ENV_INDEXER)
        else:
            description = try_to_describe_indexer_env(CGAP_ENV_INDEXER)
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
            check.status = 'ERROR'
            check.summary = 'An error occurred on deployment. Check the AWS EB Console.'
    else:
        check.status = 'FAIL'
        check.summary = 'Gave an unknown environment: %s' % env

    return check


@check_function(env='fourfront-mastertest',
                branch='master',
                application_version_name=None, repo=None)
def deploy_application_to_beanstalk(connection, **kwargs):
    """ Deploys application to beanstalk under the given application_version name + a branch

        NOTE: CGAP requires kwargs!
    """
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

    try:
        packaging_was_successful = EBDeployer.build_application_version(repo_location, application_version_name,
                                                                        branch=branch)
        time.sleep(10)  # give EB some time to index the new template
        if packaging_was_successful:
            try:
                EBDeployer.deploy_new_version(env, repo_location, application_version_name)
                check.status = 'PASS'
                check.summary = 'Successfully deployed version %s to env %s' % (application_version_name, env)
            except Exception as e:
                check.status = 'ERROR'
                check.summary = 'Exception thrown while deploying: %s' % str(e)
        else:
            check.status = 'ERROR'
            check.summary = 'Could not package repository: %s' % packaging_was_successful
    except Exception as e:
        check.status = 'ERROR'
        check.summary = 'Exception thrown while building application version: %s' % str(e)
    finally:
        cleanup_tempdir(repo_location)

    return check


def who_is_ff_staging():
    """ Helper function that tells us who staging is. This should probably go in dcicutils. -Will """
    return ({FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN} - {compute_ff_prd_env()}).pop()


@check_function()
def deploy_ff_staging(connection, **kwargs):
    """ Deploys Fourfront master to whoever staging is.
        Runs as part of the 'deployment_checks' schedule on data ONLY.
    """
    import pdb; pdb.set_trace()
    return deploy_application_to_beanstalk(connection,
                                           env=who_is_ff_staging(),
                                           branch='master')