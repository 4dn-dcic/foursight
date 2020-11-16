from .abstract_connection import AbstractConnection
from elasticsearch import (
    Elasticsearch,
    TransportError,
    RequestError,
    ConnectionTimeout
    )
from elasticsearch_dsl import Search
from dcicutils import es_utils
from .utils import load_json
from .check_utils import create_placeholder_check
import datetime
import json
import time

# configure ES info from here
HOST = 'https://search-foursight-fourfront-ylxn33a5qytswm63z52uytgkm4.us-east-1.es.amazonaws.com'
SEARCH_SIZE = 10000

class ElasticsearchException(Exception):
    """ Generic exception for an elasticsearch failure """
    def __init__(self, message=None):
        if message is None:
            self.message = "No error message given, this shouldn't happen!"
        else:
            self.message = message
        super().__init__(message)

class ESConnection(AbstractConnection):
    """
    ESConnection is a handle to a remote ElasticSearch instance on AWS.
    All Foursight connections make use of the same ES instance but have separate
    indices for each one, such as 'foursight-dev-data' etc

    ESConnection is intended to work with only a single index.

    Implements the AbstractConnection 'interface'
    """
    def __init__(self, index=None, doc_type='result'):
        self.es = es_utils.create_es_client(HOST, use_aws_url=True)
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
            mapping = self.load_mapping()
            self.es.indices.create(index=name,body={
                "settings": {
                    "index.mapper.dynamic": False
                },
                "mappings": mapping.get('mappings')
            }, ignore=400)
            return True
        except Exception as e:
            raise ElasticsearchException(str(e))

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
            return 0

    def get_size_bytes(self):
        """
        Returns number of bytes stored on this es instance
        """
        if not self.index:
            return 0
        resp = self.es.indices.stats(index=self.index, metric='store')
        return resp['_all']['total']['store']['size_in_bytes']

    def search(self, search, key='_source'):
        """
        Inner function that passes doc as a search parameter to ES. Based on the
        execute_search method in Fourfront
        """
        if not self.index:
            return []
        err_msg = None
        try:
            res = search.execute().to_dict()
        except ConnectionTimeout as exc:
            err_msg = 'The search failed due to a timeout. Please try a different query.'
        except RequestError as exc:
            try:
                err_detail = str(exc.info['error']['root_cause'][0]['reason'])
            except:
                err_detail = str(exc)
            err_msg = 'The search failed due to a request error: ' + err_detail
        except Exception as exc:
            err_msg = 'Search failed. Error: %s' % str(exc)
        if err_msg:
            raise ElasticsearchException(message=err_msg)
        return [obj[key] for obj in res['hits']['hits']] if len(res['hits']['hits']) > 0 else []

    def get_result_history(self, prefix, start, limit):
        """
        ES handle to implement the get_result_history functionality of RunResult
        """
        doc = {
            'from': start,
            'size': limit,
            'sort': {
                'uuid': {'order': 'desc'}
            },
            'query': {
                'bool': {
                    'must_not': [
                        {'term': {'_id': prefix + '/primary.json'}},
                        {'term': {'_id': prefix + '/latest.json'}}
                    ],
                    'filter': {
                        # use MATCH so our 'prefix' is analyzed like the source field 'name', see mapping
                        'match': {'name': prefix}
                    }
                }
            }
        }
        search = Search(using=self.es, index=self.index)
        search.update_from_dict(doc)
        return self.search(search)

    def get_main_page_checks(self, checks=None, primary=True):
        """
        Gets all checks for the main page. If primary is true then all checks will
        be primary, otherwise we use latest.
        Only gets SEARCH_SIZE number of results, most recent first.
        """
        if primary:
            t = 'primary'
        else:
            t = 'latest'
        doc = {
            'size': SEARCH_SIZE,
            'query': {
                'bool': {
                    'must': {
                        'wildcard': {
                            '_uid': '*' + t + '.json'
                        }
                    },
                    'filter': {
                        'term': {'type': 'check'}
                    }
                }
            },
            'sort': {
                'uuid': {
                    'order': 'desc'
                }
            }
        }
        search = Search(using=self.es, index=self.index)
        search.update_from_dict(doc)
        raw_result = self.search(search)
        if checks is not None:
            # figure out which checks we didn't find, add a placeholder check so
            # that check is still rendered on the UI
            raw_result = list(filter(lambda res: res['name'] in checks, raw_result))
            found_checks = set(res['name'] for res in raw_result)
            for check_name in checks:
                if check_name not in found_checks:
                    raw_result.append(create_placeholder_check(check_name))
        return raw_result

    def list_all_keys(self):
        """
        Generic search on es that will return all ids of indexed items
        Only gets SEARCH_SIZE number of results, most recent first.
        """
        doc = {
            'size': SEARCH_SIZE,
            'query': {
                'match_all' : {}
            },
            'sort': {
                'uuid': {
                    'order': 'desc'
                }
            }
        }
        search = Search(using=self.es, index=self.index)
        search.update_from_dict(doc)
        return self.search(search, key='_id')

    def list_all_keys_w_prefix(self, prefix):
        """
        Lists all id's in this ES that have the given prefix.
        Only gets SEARCH_SIZE number of results, most recent first.
        """
        doc = {
            'size': SEARCH_SIZE,
            'query': {
                'bool': {
                    'filter': {
                        'term': {'name': prefix}
                    }
                }
            },
            'sort': {
                'uuid': {
                    'order': 'desc'
                }
            }
        }
        search = Search(using=self.es, index=self.index)
        search.update_from_dict(doc)
        return self.search(search, key='_id')

    def get_all_objects(self):
        """
        Calls list_all_keys with full=True to get all the objects
        Only gets SEARCH_SIZE number of results, most recent first.
        """
        doc = {
            'size': SEARCH_SIZE,
            'query': {
                'match_all' : {}
            },
            'sort': {
                'uuid': {
                    'order': 'desc'
                }
            }
        }
        search = Search(using=self.es, index=self.index)
        search.update_from_dict(doc)
        return self.search(search)

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
        try:
            res = self.es.delete_by_query(index=self.index, body=query)
            return res['deleted']
        except Exception as e:
            return 0

    def test_connection(self):
        """
        Hits health route on es to verify that it is up
        """
        return self.es.ping()
