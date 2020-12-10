from conftest import *


class TestS3Connection():
    environ = DEV_ENV
    app_utils_obj = app_utils.AppUtils()
    conn = app_utils_obj.init_connection(environ)

    def test_s3_conn_fields(self):
        s3_conn = self.conn.connections['s3']
        assert (s3_conn.bucket)
        assert (s3_conn.location)
        assert (s3_conn.status_code != 404)

    def test_test_s3_conn_methods(self):
        # clean up after yourself
        test_s3_conn = s3_connection.S3Connection(FOURSIGHT_PREFIX + '-test-s3')
        test_key = 'test/' + ff_utils.generate_rand_accession()
        test_value = {'abc': 123}
        assert (test_s3_conn.status_code != 404)
        put_res = test_s3_conn.put_object(test_key, json.dumps(test_value))
        assert (put_res is not None)
        get_res = test_s3_conn.get_object(test_key)
        assert (get_res == test_value)
        n_keys = test_s3_conn.get_size()
        assert n_keys == 1
        prefix_keys = test_s3_conn.list_all_keys_w_prefix('test/')
        assert test_s3_conn.get_size_bytes() is not None
        assert (len(prefix_keys) > 0)
        assert (test_key in prefix_keys)
        all_keys = test_s3_conn.list_all_keys()
        assert (len(all_keys) == len(prefix_keys))
        test_s3_conn.delete_keys(all_keys)
        # now there should be 0
        all_keys = test_s3_conn.list_all_keys()
        assert (len(all_keys) == 0)
        n_keys = test_s3_conn.get_size()
        assert n_keys == 0
