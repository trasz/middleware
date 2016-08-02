import json
import requests


class Client(object):
    def __init__(self, uri, base_path=None):
        self.auth = ('root', 'freenas')
        self.base_path = base_path or ''
        self.uri = uri

    def get(self, path, params=None):
        r = requests.get(
            self.uri + self.base_path + path,
            auth=self.auth,
            headers={'Content-Type': "application/json"},
        )
        return r

    def post(self, path, data=None):
        r = requests.post(
            self.uri + self.base_path + path,
            auth=self.auth,
            headers={'Content-Type': "application/json"},
            data=json.dumps(data or ''),
        )
        return r
