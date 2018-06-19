from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
import requests
import sys
import json
import datetime
import boto3
import random
import time

def test_function_unused():
    return


# meant to raise an error on execution by dividing by 0
@check_function()
def test_check_error(connection, **kwargs):
    bad_op = 10 * 1/0
    return bad_op


@action_function()
def test_action_error(connection, **kwargs):
    bad_op = 10 * 1/0
    return bad_op


# silly check that stores random numbers in a list
@check_function()
def test_random_nums(connection, **kwargs):
    check = init_check_res(connection, 'test_random_nums')
    check.status = 'IGNORE'
    check.action = 'add_random_test_nums'
    check.allow_action = True
    output = []
    for i in range(random.randint(1,20)):
        output.append(random.randint(1,100))
    check.full_output = output
    check.description = 'A test check'
    # sleep for 2 secs because this is used to test timing out
    time.sleep(2)
    return check


# same as above
@check_function()
def test_random_nums_2(connection, **kwargs):
    check = init_check_res(connection, 'test_random_nums_2')
    check.status = 'IGNORE'
    output = []
    for i in range(random.randint(1,20)):
        output.append(random.randint(1,100))
    check.full_output = output
    check.description = 'A test check as well'
    return check


@action_function(offset=0)
def add_random_test_nums(connection, **kwargs):
    action = init_action_res(connection, 'add_random_test_nums')
    check = init_check_res(connection, 'test_random_nums')
    # output includes primary and latest results, to compare
    check_latest = check.get_latest_result()
    nums_latest = check_latest.get('full_output', [])
    total_latest = sum(nums_latest) + kwargs.get('offset', 0)
    check_primary = check.get_primary_result()
    nums_primary = check_primary.get('full_output', [])
    total_primary = sum(nums_primary) + kwargs.get('offset', 0)
    action.output = {'latest': total_latest, 'primary': total_primary}
    action.status = 'DONE'
    action.description = 'A test action'
    return action
