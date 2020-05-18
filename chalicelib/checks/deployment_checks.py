import boto3

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

