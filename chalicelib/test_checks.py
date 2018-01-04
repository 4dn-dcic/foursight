from __future__ import print_function, unicode_literals
from .utils import check_function, init_check_res, build_dummy_result
from collections import OrderedDict
import requests
import sys
import json
import datetime
import boto3

def test_function_unused():
    return


# meant to raise an error on execution by dividing by 0
@check_function()
def test_check_error(connection, **kwargs):
    bad_op = 10 * 1/0
    return bad_op
