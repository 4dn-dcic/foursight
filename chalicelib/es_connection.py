from .abstract_connection import AbstractConnection
from elasticsearch import Elasticsearch, RequestsHttpConnection
from dcicutils import es_utils
from .utils import load_json
import json
import time

# configure ES info from here
HOST = 'https://search-foursight-fourfront-ylxn33a5qytswm63z52uytgkm4.us-east-1.es.amazonaws.com'

class ESConnection(AbstractConnection):
    """
    ESConnection is a handle to a remote ElasticSearch instance on AWS.
    All Foursight connections make use of the same ES instance but have separate
    indices for each one, such as 'foursight-dev-cgap', 'foursight-dev-data' etc

    ESConnection is intended to work with only a single index.

    Implements the AbstractConnection 'interface'
    """
    def __init__(self, index=None, doc_type='result'):
        self.es = es_utils.create_es_client(HOST, use_aws_url=True)
        self.index = index
        self.mapping = self.load_mapping()
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
            res = self.es.indices.create(index=name,body={
                "settings": {
                    "index.mapper.dynamic": False,
                    "index.mapping.ignore_malformed": True
                },
                "mappings": self.mapping['mappings']
            })
            return True
        except Exception as e:
            print('Index creation failed! Error: %s' % str(e))
            return False

    def load_mapping(self, fname='mapping.json'):
        """
        Loads ES mapping from 'mapping.json' or another relative path from this
        file location.
        """
        return load_json(__file__, fname)

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
        Refreshes the index, then waits 3 seconds
        """
        self.es.indices.refresh(index=self.index)
        time.sleep(3)

    def put_object(self, key, value):
        """
        Index a new item into es. Returns true in success
        """
        if not self.index:
            return False
        try:
            res = self.es.index(index=self.index, id=key, doc_type=self.doc_type, body=value)
            return res['result'] == 'created'
        except Exception as e:
            print('Failed to add object id: %s with error: %s' % (key, str(e)))
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

    def get_size_bytes(self):
        """
        Returns number of bytes stored on this es instance
        """
        if not self.index:
            return 0
        resp = self.es.indices.stats(index=self.index, metric='store')
        return resp['_all']['total']['store']['size_in_bytes']

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

    def list_all_keys_w_prefix(self, prefix):
        """
        Lists all id's in this ES that have the given prefix.
        """
        return [_id for _id in self.list_all_keys() if prefix in _id]

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
        query = {
            'query': {
                'terms': {'_id': key_list}
            }
        }
        self.es.delete_by_query(index=self.index, body=query)

    def test_connection(self):
        """
        Hits health route on es to verify that it is up
        """
        return self.es.ping()
