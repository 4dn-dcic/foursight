from dcicutils.ecs_utils import ECSUtils
from .helpers.confchecks import *


@check_function()
def ecs_status(connection, **kwargs):
    """ ECS Status check reports metadata on the Clusters/Services running on
        ECS in the account where foursight has been orchestrated.
    """
    check = CheckResult(connection, 'ecs_status')
    full_output = {
        'ECSMeta': {
            'clusters': {}
        }
    }
    client = ECSUtils()
    cluster_arns = client.list_ecs_clusters()
    for cluster_arn in cluster_arns:
        if 'Fourfront' in cluster_arn:
            # TODO: fix dcicutils
            cluster_services = client.client.list_services(cluster=cluster_arn).get('serviceArns', [])
            full_output['ECSMeta']['clusters'][cluster_arn] = {
                'services': cluster_services
            }
    if not full_output['ECSMeta']['clusters']:
        check.status = 'WARN'
        check.summary = 'No clusters detected! Has ECS been orchestrated?'
    else:
        check.status = 'PASS'
        check.summary = 'See full output for ECS Metadata'
    check.full_output = full_output
    return check


@check_function(cluster_name=None)
def update_ecs_application_versions(connection, **kwargs):
    """ This check is intended to be run AFTER the user has finished pushing
        the relevant images to ECR. Triggers an update on all services for
        the CGAP cluster. If no cluster_name is passed, Foursight will infer
        one if there is only a single option - otherwise error is raised.

        Note that this check just kicks the process - it does not block until
        the cluster update has finished.
    """
    check = CheckResult(connection, 'update_ecs_application_versions')
    client = ECSUtils()
    cluster_name = kwargs.get('cluster_name')
    cluster_arns = client.list_ecs_clusters()
    if not cluster_name:
        cgap_candidate = list(filter(lambda arn: 'fourfront' in arn.lower(), cluster_arns))
        if not cgap_candidate:
            check.status = 'FAIL'
            check.summary = 'No clusters could be resolved from %s' % cluster_arns
        elif len(cgap_candidate) > 1:
            check.status = 'FAIL'
            check.summary = 'Ambiguous cluster setup (not proceeding): %s' % cgap_candidate
        else:
            client.update_all_services(cluster_name=cgap_candidate[0])
            check.status = 'PASS'
            check.summary = 'Triggered cluster update for %s - updating all services.' % cgap_candidate[0]
    else:
        if cluster_name not in cluster_arns:
            check.status = 'FAIL'
            check.summary = 'Given cluster name does not exist! Gave: %s, Resolved: %s' % (cluster_name, cluster_arns)
        else:
            client.update_all_services(cluster_name=cluster_name)
            check.status = 'PASS'
            check.summary = 'Triggered cluster update for %s - updating all services.' % cluster_name
    return check
