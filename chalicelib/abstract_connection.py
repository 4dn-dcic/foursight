class AbstractConnection(object):
    """
    AbstractConnection is an 'abstract' representation of the methods a
    connection subclass should implement. There will be others that are specific
    to the type of connection but this collection should be consistent across
    all connection types. For all intents and purposes this is an interface.
    """

    def put_object(self, key, value):
        """
        Generic put operation. Key is typically the filename, value is the actual
        data to be stored.
        """
        raise NotImplementedError

    def get_object(self, key):
        """
        Generic get operation. Key is the filename, returns the data object that
        is stored on this connection.
        """
        raise NotImplementedError

    def get_size(self):
        """
        Returns the number of items stored on this connection
        """
        raise NotImplementedError

    def get_size_bytes(self):
        """
        Returns number of bytes stored on this connection
        """
        raise NotImplementedError

    def list_all_keys(self):
        """
        Lists all the keys stored on this connection.
        """
        raise NotImplementedError

    def list_all_keys_w_prefix(self):
        """
        Given a prefix, return all keys that have that prefix.
        """
        raise NotImplementedError

    def get_all_objects(self):
        """
        Returns an array of the data values stored on this connection.
        """
        raise NotImplementedError

    def delete_keys(self, key_list):
        """
        Deletes the given keys in key_list from this connection
        """
        raise NotImplementedError

    def test_connection(self):
        """
        Tests that this connection is reachable
        """
        raise NotImplementedError
