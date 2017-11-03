from __future__ import print_function
import unittest
import datetime
import json
from chalicelib.checksuite import CheckSuite
from chalicelib.ff_connection import FFConnection


class TestCheckSuite(unittest.TestCase):
    connection = FFConnection('test', None, None, None)
    suite = CheckSuite(connection)

    def test_connection_fields(self):
        self.assertTrue(self.connection.environment == 'test')
        self.assertTrue(self.connection.is_up == False)
        self.assertTrue(self.connection.s3connection.status_code == 404)

    def test_checksuite_basics(self):
        check_res = json.loads(self.suite.status_of_servers())
        self.assertTrue(check_res.get('status') == 'FAIL')
        self.assertTrue(check_res.get('name') == 'status_of_servers')

    def test_checkresult(self):
        test_check = self.suite.init_check('test_check', description='Unittest check')
        self.assertTrue(test_check.s3connection.status_code == 404)
        self.assertTrue(test_check.get_latest_check() is None)
        self.assertTrue(test_check.get_closest_check(1) is None)
        self.assertTrue(test_check.title == 'Test Check')
        formatted_res = test_check.format_result(datetime.datetime.utcnow())
        self.assertTrue(formatted_res.get('status') == 'PEND')
        self.assertTrue(formatted_res.get('title') == 'Test Check')
        self.assertTrue(formatted_res.get('description') == 'Unittest check')
        check_res = json.loads(test_check.store_result())
        self.assertTrue(check_res.get('status') == 'ERROR')
        self.assertTrue(check_res.get('name') == formatted_res.get('name'))
        self.assertTrue(check_res.get('description') == "Malformed status; look at Foursight check definition.")
        self.assertTrue(check_res.get('brief_output') == formatted_res.get('brief_output') == None)


if __name__ == '__main__':
    unittest.main()
