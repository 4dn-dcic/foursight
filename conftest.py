from __future__ import print_function, unicode_literals
import chalice
import unittest
import datetime
import json
import os
import sys
import time
import boto3
import app
from chalicelib import (
    app_utils,
    check_utils,
    utils,
    run_result,
    fs_connection,
    s3_connection,
    es_connection
)
from dcicutils import s3_utils, ff_utils
from dateutil import tz
from contextlib import contextmanager
import pytest

@pytest.fixture(scope='session', autouse=True)
def setup():
    app.set_stage('test')  # set the stage info for tests
    test_client = boto3.client('sqs')  # purge test queue
    queue_url = utils.get_sqs_queue().url
    try:
        test_client.purge_queue(
            QueueUrl=queue_url
        )
    except test_client.exceptions.PurgeQueueInProgress:
        print('Cannot purge test queue; purge already in progress')
