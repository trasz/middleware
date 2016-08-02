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
