import requests
import json

class FF_Connection(object):
    def __init__(self, server):
        self.headers = {'content-type': 'application/json', 'accept': 'application/json'}
        self.server = server
        self.isUp = self.test_connection()

    def test_connection(self):
        # check connection
        try:
            head_resp = requests.head(self.server)
        except:
            return False
        return True if head_resp.status_code == 200 else False
