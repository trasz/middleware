import json
import requests


class Client(object):
    def __init__(self, uri, base_path=None):
        self.auth = ('root', 'freenas')
        self.base_path = base_path or ''
        self.uri = uri

    def request(self, method, path, params=None, data=None):
        r = requests.request(
            method,
            self.uri + self.base_path + path,
            params=params,
            data=json.dumps(data or ''),
            headers={'Content-Type': "application/json"},
            auth=self.auth,
        )
        return r

    def get(self, path, params=None):
        return self.request('GET', path, params=params)

    def post(self, path, data=None):
        return self.request('POST', path, data=data)

    def delete(self, path):
        return self.request('DELETE', path)
