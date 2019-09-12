from .abstract_connection import AbstractConnection
from elasticsearch import Elasticsearch

class ESConnection(AbstractConnection):
    """
    ESConnection right now is a stub that will eventually implement the same
    functionality as defined in the AbstractConnection class.
    """

    def __init__(self):
        self.es = Elasticsearch()

    def put_object(self, key, value):
        """
        Adds a new object to es
        """
        pass

    def get_object(self, key):
        """
        Gets object with uuid=key from es
        """
        pass

    def list_all_keys(self):
        """
        Generic search on es that will return all uuids
        """
        pass

    def get_all_objects(self):
        """
        Same as the above method but instead of just uuids it has all the data
        """
        pass

    def delete_keys(self, key_list):
        """
        Deletes all uuids in key_list from es
        """
        pass

    def test_connection(self):
        """
        Hits health route on es to verify that it is up
        """
        pass
