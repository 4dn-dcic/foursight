from conftest import *

class TestESConnection():
    environ = 'mastertest'
    conn = app_utils.init_connection(environ)
    index = 'unit_test_index'
    es = es_connection.ESConnection(index)

    @pytest.mark.skip
    def test_basic_indexing(self):
        """
        Creates a test index, indexes a few check-like items, verifies they are
        there, deletes the index. These operations should all succeed.
        """
        assert self.es.test_connection()
        self.es.create_index(self.index)
        check = {'name': 'items_created_in_the_past_day', 'title': 'Items Created In The Past Day',
                 'description': 'No items have been created in the past day.', 'status': 'PASS',
                 'uuid': '2018-01-16T19:14:34.025445','brief_output': None,'full_output': {},
                 'admin_output': None, 'ff_link': None}
        assert self.es.put_object(check['uuid'], check)
        obj = self.es.get_object(check['uuid'])
        assert obj['uuid'] == check['uuid']
        self.es.delete_keys([check['uuid']])
        self.es.refresh_index()
        with pytest.raises(Exception):
            self.es.get_object(check['uuid'])
        assert self.es.delete_index(self.index)
