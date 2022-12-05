import time
import boto3  # PyCharm says nothing is arranging to load this - is that really so?
import shutil
import datetime
import tempfile

from botocore.exceptions import ClientError
from git import Repo
from dcicutils.ff_utils import get_metadata
from dcicutils.deployment_utils import EBDeployer
from dcicutils.beanstalk_utils import compute_ff_stg_env
from dcicutils.env_utils import is_fourfront_env, indexer_env_for_env
from dcicutils.misc_utils import ignored
from dcicutils.beanstalk_utils import compute_ff_prd_env, beanstalk_info

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


def try_to_describe_indexer_env(env):
    """
    Small helper that wraps beanstalk_info so we can easily ignore error for non-existent env.
    """
    # XXX: Should make beanstalk_info take something like if_does_not_exist= that can be 'error' or None,
    #      defaulting to 'error' so it's an incompatible change, but so we can pass None for our need.
    try:
        return beanstalk_info(env)
    except (IndexError, ClientError):
        return None  # Previously got IndexError if env does not exist, but now get a botocore.exceptions.ClientError
    except Exception:
        raise  # something else happened we should (probably) raise


def clone_repo_to_temporary_dir(repo='https://github.com/4dn-dcic/fourfront.git', name='fourfront'):
    """ Clones the given repo (default fourfront) to a temporary directory whose
        absolute path is returned.
        XXX: This code should be refactored into a contextmanager (probably in dcicutils).
             See PR292 comments.
    """
    tempdir = tempfile.mkdtemp(prefix=name)
    Repo.clone_from(url=repo, to_path=tempdir)
    return tempdir


def cleanup_tempdir(tempdir):
    """ Wrapper for shutil.rmtree that cleans up the (repo dir) we just created """
    shutil.rmtree(tempdir)


def are_counts_even(env):
    """ From dcicutils - to be ported over in this form at some point. """
    try:
        totals = get_metadata('/counts', ff_env=env)['db_es_total'].split()
    except Exception:
        # if we cant get counts page assume its False
        return False, {}

    # example value of split totals: ["DB:", "74048", "ES:", "74048"]
    db_total = totals[1]
    es_total = totals[3]
    if int(db_total) > int(es_total):
        return False, totals
    return True, totals


@check_function(env='fourfront-hotseat', action="terminate_indexer_server")
def indexer_server_status(connection, **kwargs):
    """ Checks the status of index servers (if any are up) """
    check = CheckResult(connection, 'indexer_server_status')
    check.action = 'terminate_indexer_server'
    env = kwargs.get('env')
    indexer_env = indexer_env_for_env(env)
    description = try_to_describe_indexer_env(indexer_env)  # verify an indexer is online
    if description is None:
        check.status = 'PASS'  # could have been terminated
        check.summary = 'Did not locate corresponding indexer env: ' \
                        'given env %s does not have indexer %s' % (env, indexer_env)
        check.allow_action = False
        return check

    try:
        indexing_finished, counts = are_counts_even(env)
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
        indexer_env = indexer_env_for_env(e)
        description = try_to_describe_indexer_env(indexer_env)
        if description is not None:
            check.status = 'ERROR'
            check.summary = 'Tried to spin up indexer env for %s when one already exists for this portal' % e
            return False
        else:
            return EBDeployer.deploy_indexer(e, version)

    if is_fourfront_env(env):
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


def _deploy_application_to_beanstalk(connection, **kwargs):
    """ Creates and returns a CheckResult for 'deploy_application_to_beanstalk'.

        NOTE: this does NOT have the @check_function decorator because I'd like
        to "call this check" in other checks. The decorator will effectively cast
        the resulting check to a dictionary, breaking nested checks. This could be
        fixed by refactoring "store_formatted_result". - Will 05/28/2020
    """
    check = CheckResult(connection, 'deploy_application_to_beanstalk')
    env = kwargs.get('env', 'fourfront-mastertest')  # by default
    branch = kwargs.get('branch', 'master')  # by default deploy master
    application_version_name = kwargs.get('application_version_name', None)

    # error if we try to deploy prod
    if env == compute_ff_prd_env():
        check.status = 'ERROR'
        check.summary = 'Tried to deploy production env %s from Foursight, aborting.' % env
        return check

    if application_version_name is None:  # if not specified, use branch+timestamp
        application_version_name = 'foursight-package-%s-%s' % (branch, datetime.datetime.utcnow())

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


@check_function(env='fourfront-mastertest',
                branch='master',
                application_version_name=None, repo=None)
def deploy_application_to_beanstalk(connection, **kwargs):
    """ Deploys application to beanstalk under the given application_version name + a branch
        Uses the above helper method, returning the (decorated, thus stored) check directly.

        NOTE: CGAP requires kwargs!
    """
    return _deploy_application_to_beanstalk(connection, **kwargs)


def deploy_env(connection, env_to_deploy, application_name, check, **kwargs):
    """ For a given beanstalk environment, with a configured check json and given application name,
        deploys the environment, or returns an error.
    """
    this_check = CheckResult(connection, check)
    helper_check = _deploy_application_to_beanstalk(connection,
                                                    env=env_to_deploy,
                                                    branch='master',
                                                    **kwargs)
    if helper_check.status == 'PASS':
        this_check.status = 'PASS'
        this_check.summary = ('Successfully deployed {what} master to {where}'
                              .format(what=application_name, where=env_to_deploy))
    else:
        this_check.status = 'ERROR'
        this_check.summary = 'Error occurred during deployment, see full_output'
        this_check.full_output = helper_check.summary  # should have error message
    return this_check


@check_function()
def deploy_ff_staging(connection, **kwargs):
    """ Deploys Fourfront master to whoever staging is.
        Runs as part of the 'deployment_checks' schedule on data ONLY.
    """
    return deploy_env(
        connection,
        env_to_deploy=compute_ff_stg_env(),
        application_name="Fourfront",
        check='deploy_ff_staging',
        **kwargs)
