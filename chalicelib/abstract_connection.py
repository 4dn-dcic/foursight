class AbstractConnection(object):
    """
    AbstractConnection is an 'abstract' representation of the methods a
    connection subclass should implement. There will be others that are specific
    to the type of connection but this collection should be consistent across
    all connection types. For all intents and purposes this is an interface.
    """

    def __init__(self):
        pass

    def put_object(self, key, value):
        """
        Generic put operation. Key is typically the filename, value is the actual
        data to be stored.
        """
        pass

    def get_object(self, key):
        """
        Generic get operation. Key is the filename, returns the data object that
        is stored on this connection.
        """
        pass

    def list_all_keys(self):
        """
        Lists all the keys stored on this connection.
        """
        pass

    def get_all_objects(self):
        """
        Returns a dictionary of key,value pairs stored on this connection.
        """
        pass

    def delete_keys(self, key_list):
        """
        Deletes the given keys in key_list from this connection
        """
        pass

    def test_connection(self):
        """
        Tests that this connection is reachable
        """
        pass
