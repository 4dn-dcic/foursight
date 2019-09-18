from conftest import *

class TestESConnection():
    environ = 'mastertest'
    conn = app_utils.init_connection(environ)
    index = 'unit_test_index'
    try:
        es = es_connection.ESConnection(index)
    except:
        es = None # tests should be marked as skip

    @staticmethod
    def uuid(check):
        return check['data']['name'] + '/' + check['data']['uuid']

    def test_basic_indexing(self):
        """
        Creates a test index, indexes a few check items, verifies they are
        there, deletes the index. These operations should all succeed.
        """
        assert self.es.test_connection()
        self.es.create_index(self.index)
        check = utils.load_json(__file__, 'test_checks/check1.json')
        uuid = self.uuid(check)
        assert self.es.put_object(uuid, check)
        obj = self.es.get_object(uuid)
        assert (obj['data']['name'] + '/' + obj['data']['uuid']) == uuid
        self.es.delete_keys([uuid])
        self.es.refresh_index()
        assert self.es.get_object(uuid) == None
        assert self.es.get_size_bytes() > 0
        assert self.es.delete_index(self.index)

    def test_indexing_methods(self):
        """
        Creates a test index, indexes a few check items, uses additional methods
        to interact with the index, such as list_all_keys, get_all_objects
        """
        self.es.create_index(self.index)
        assert self.es.index_exists(self.index)
        check1 = utils.load_json(__file__, 'test_checks/check1.json')
        check2 = utils.load_json(__file__, 'test_checks/check2.json')
        check3 = utils.load_json(__file__, 'test_checks/check3.json')
        assert self.es.put_object(self.uuid(check1), check1)
        assert self.es.put_object(self.uuid(check2), check2)
        self.es.refresh_index()
        keys = self.es.list_all_keys()
        assert self.uuid(check1) in keys
        assert self.uuid(check2) in keys
        assert self.uuid(check3) not in keys
        assert self.es.put_object(self.uuid(check3), check3)
        self.es.refresh_index()
        objs = self.es.get_all_objects()
        assert len(objs) == 3
        self.es.delete_keys([self.uuid(check1), self.uuid(check2)])
        self.es.refresh_index()
        keys = self.es.list_all_keys()
        assert len(keys) == 1
        assert self.uuid(check3) in keys
        assert self.es.delete_index(self.index)

    def test_indexing_failures(self):
        """
        Tests some failure cases with indexing
        """
        self.es.create_index(self.index)
        assert not self.es.create_index(self.index)
        assert not self.es.index_exists('i_dont_exist')
        check1 = utils.load_json(__file__, 'test_checks/check1.json')
        assert self.es.put_object(self.uuid(check1), check1)
        assert not self.es.put_object(self.uuid(check1), check1)
        self.es.refresh_index()
        assert len(self.es.list_all_keys_w_prefix('page_children_routes')) == 1
        assert len(self.es.list_all_keys_w_prefix('pag3_children_routes')) == 0
        assert self.es.delete_index(self.index)
