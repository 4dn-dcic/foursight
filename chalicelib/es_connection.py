from .abstract_connection import AbstractConnection
from elasticsearch import Elasticsearch

class ESConnection(AbstractConnection):
    """
    ESConnection right now is a stub that will eventually implement the same
    functionality as defined in the AbstractConnection class.

    ESConnection is intended to work with only a single index.
    """
    def __init__(self, index=None, doc_type='result'):
        self.es = Elasticsearch(['http://localhost:9200']) # use local es for now
        self.index = index
        if index and not self.index_exists(index):
            self.create_index(index)
        self.doc_type = doc_type

    def index_exists(self, name):
        """
        Checks if the given index name exists
        """
        return self.es.indices.exists(index=name)

    def create_index(self, name):
        """
        Creates an ES index called name. Returns true in success
        """
        try:
            res = self.es.indices.create(index=name)
            return True
        except:
            return False

    def delete_index(self, name):
        """
        Deletes the given index name from this es
        """
        try:
            self.es.indices.delete(index=name, ignore=[400, 404])
        except:
            return False
        return True

    def refresh_index(self):
        """
        Refreshes the index - sometimes necessary to see that a deletion happened
        """
        self.es.indices.refresh(index=self.index)

    def put_object(self, key, value):
        """
        Index a new item into es. Returns true in success
        """
        if not self.index:
            return False
        try:
            res = self.es.index(index=self.index, id=key, doc_type=self.doc_type, body=value)
            return res['result'] == 'created'
        except:
            return False

    def get_object(self, key):
        """
        Gets object with uuid=key from es. Returns None if not found or no index
        has been specified.
        """
        if not self.index:
            return None
        try:
            return self.es.get(index=self.index, doc_type=self.doc_type, id=key)['_source']
        except:
            return None

    def get_size(self):
        """
        Returns the number of items indexed on this es instance. Returns -1 in
        failure.
        """
        try:
            return self.es.count(self.index).get('count')
        except:
            return -1

    def list_all_keys(self, full=False):
        """
        Generic search on es that will return all ids of indexed items
        full is an optional argument that, if specified, will give the full data
        instead of just the _ids
        """
        if not self.index:
            return []
        doc = {
            'size': 10000,
            'query': {
                'match_all' : {}
            }
        }
        if not full:
            res = self.es.search(index=self.index, doc_type=self.doc_type, body=doc,
                                 filter_path=['hits.hits._id'])
            return [obj['_id'] for obj in res['hits']['hits']] if (len(res) > 0) else []
        else:
            res = self.es.search(index=self.index, doc_type=self.doc_type, body=doc,
                                 filter_path=['hits.hits.*'])
            return [obj['_source'] for obj in res['hits']['hits']] if len(res) > 0 else []

    def get_all_objects(self):
        """
        Calls list_all_keys with full=True to get all the objects
        """
        return self.list_all_keys(full=True)

    def delete_keys(self, key_list):
        """
        Deletes all uuids in key_list from es. If key_list is large this will be
        a slow operation, but probably still not as slow as s3
        """
        for key in key_list:
            res = self.es.delete(index=self.index, doc_type=self.doc_type, id=key)

    def test_connection(self):
        """
        Hits health route on es to verify that it is up
        """
        return self.es.ping()
