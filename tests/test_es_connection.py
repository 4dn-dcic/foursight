from conftest import *


class TestESConnection():
    environ = DEV_ENV
    app_utils_obj = app_utils.AppUtils()
    conn = app_utils_obj.init_connection(environ)
    index = 'unit_test_index'
    try:
        es = es_connection.ESConnection(index, host=HOST)
        es.delete_index(index)
    except:
        es = None # tests should be marked as skip

    @staticmethod
    def uuid(check):
        return check['name'] + '/' + check['uuid']

    def test_Elasticsearch_Exception(self):
        """
        Tests creating an ES exception
        """
        ex = es_connection.ElasticsearchException()
        assert ex.message == "No error message given, this shouldn't happen!"
        ex = es_connection.ElasticsearchException('test message')
        assert ex.message == 'test message'

    def test_basic_indexing(self):
        """
        Creates a test index, indexes a few check items, verifies they are
        there, deletes the index. These operations should all succeed.
        """
        assert self.es.test_connection()
        self.es.create_index(self.index)
        check = self.es.load_json(__file__, 'test_checks/check1.json')
        uuid = self.uuid(check)
        self.es.put_object(uuid, check)
        obj = self.es.get_object(uuid)
        assert (obj['name'] + '/' + obj['uuid']) == uuid
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
        check1 = self.es.load_json(__file__, 'test_checks/check1.json')
        check2 = self.es.load_json(__file__, 'test_checks/check2.json')
        check3 = self.es.load_json(__file__, 'test_checks/check3.json')
        self.es.put_object(self.uuid(check1), check1)
        self.es.put_object(self.uuid(check2), check2)
        self.es.refresh_index()
        keys = self.es.list_all_keys()
        assert self.uuid(check1) in keys
        assert self.uuid(check2) in keys
        assert self.uuid(check3) not in keys
        self.es.put_object(self.uuid(check3), check3)
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
        assert not self.es.index_exists('i_dont_exist')
        check1 = self.es.load_json(__file__, 'test_checks/check1.json')
        self.es.put_object(self.uuid(check1), check1)
        assert not self.es.put_object(self.uuid(check1), check1)
        self.es.refresh_index()
        assert len(self.es.list_all_keys_w_prefix('page_children_routes')) == 1
        assert len(self.es.list_all_keys_w_prefix('pag3_children_routes')) == 0
        fail_check = self.es.load_json(__file__, 'test_checks/fail_check.json')
        assert not self.es.put_object(self.uuid(fail_check), fail_check)
        assert self.es.delete_index(self.index)

    def test_result_history(self):
        """
        Indexes some items, checks that we get them when we use history search
        """
        self.es.create_index(self.index)
        check1 = self.es.load_json(__file__, 'test_checks/check1.json')
        check2 = self.es.load_json(__file__, 'test_checks/check2.json')
        check3 = self.es.load_json(__file__, 'test_checks/check3.json')
        check4 = self.es.load_json(__file__, 'test_checks/check4.json')
        self.es.put_object(self.uuid(check1), check1)
        self.es.put_object(self.uuid(check2), check2)
        self.es.put_object(self.uuid(check3), check3)
        self.es.put_object(self.uuid(check4), check4)
        self.es.refresh_index()
        assert self.es.get_size() == 4
        res = self.es.get_result_history('page_children_routes', 0, 25)
        assert len(res) == 3
        res = self.es.get_result_history('check_status_mismatch', 0, 25)
        assert len(res) == 1
        self.es.delete_index(self.index)

    @pytest.mark.parametrize('type', ['primary', 'latest']) # needs latest as well
    def test_get_checks(self, type):
        """ Indexes some items, get primary result """
        self.es.create_index(self.index)
        check1 = self.es.load_json(__file__, 'test_checks/check1.json')
        check2 = self.es.load_json(__file__, 'test_checks/check2.json')
        check3 = self.es.load_json(__file__, 'test_checks/check3.json')
        check4 = self.es.load_json(__file__, 'test_checks/check4.json')
        self.es.put_object(self.uuid(check1), check1)
        self.es.put_object(self.uuid(check2), check2)
        self.es.put_object('page_children_routes/' + type + '.json', check3)
        self.es.put_object('check_status_mismatch/' + type + '.json', check4)
        self.es.refresh_index()
        if type == 'primary':
            res = self.es.get_main_page_checks()
        else:
            res = self.es.get_main_page_checks(primary=False)
        assert len(res) == 2
        checks_to_get = ['page_children_routes']
        if type == 'primary':
            res = self.es.get_main_page_checks(checks=checks_to_get)
        else:
            res = self.es.get_main_page_checks(checks=checks_to_get, primary=False)
        assert len(res) == 1
        self.es.delete_keys(['page_children_routes/'+ type + '.json',
                             'check_status_mismatch/' + type + '.json'])
        self.es.refresh_index()
        if type == 'primary':
            res = self.es.get_main_page_checks()
        else:
            res = self.es.get_main_page_checks(primary=False)
        assert len(res) == 0
        self.es.delete_index(self.index)

    def test_search_failures(self):
        """
        Tests some errors for search
        """
        with pytest.raises(es_connection.ElasticsearchException):
            self.es.search(None)
