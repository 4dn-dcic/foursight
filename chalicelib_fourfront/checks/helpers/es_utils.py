import os
from typing import Optional
from dcicutils.es_utils import create_es_client
from dcicutils import ff_utils

def get_es_metadata(*args, **kwargs):
    if (kwargs.get("es_client", None) is None) and ((es_host_local := _get_es_host_local()) is not None):
        es_client = create_es_client(es_host_local, use_aws_auth=True)
        return ff_utils.get_es_metadata(*args, **kwargs, es_client=es_client)
    return ff_utils.get_es_metadata(*args, **kwargs)


def _get_es_host_local() -> Optional[str]:
    return os.environ.get("ES_HOST_LOCAL", None)
