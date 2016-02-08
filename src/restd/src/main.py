import falcon
import json
import gevent
import signal
import sys

from freenas.dispatcher.client import Client
from gevent.wsgi import WSGIServer

from base import CRUDBase


# TODO: Some sort of plugins loading?
class UserCRUD(CRUDBase):
     namespace = 'user'


class JSONTranslator(object):

    def process_request(self, req, resp):
        if req.content_length in (None, 0):
            # Nothing to do
            return

        body = req.stream.read()
        if not body:
            return

        if 'application/json' not in req.content_type:
            return

        try:
            req.context = json.loads(body.decode('utf-8'))

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect or not encoded as '
                                   'UTF-8.')


class RESTApi(object):

    def __init__(self):
        self._threads = []
        self.api = falcon.API(middleware=[
            JSONTranslator(),
        ])
        self.dispatcher = Client()
        self.dispatcher.connect('unix:')
        self.dispatcher.login_service('restd')

        UserCRUD(self, self.dispatcher)

        gevent.signal(signal.SIGINT, self.die)

    def __call__(self, environ, start_response):
        if 'HTTP_X_REAL_IP' in environ:
            environ['PATH_INFO'] = environ.get('PATH_INFO', '').replace('/api/v2.0', '', 1)
        return self.api.__call__(environ, start_response)

    def run(self):
        server4 = WSGIServer(('', 8889), self)
        self._threads = [gevent.spawn(server4.serve_forever)]
        gevent.joinall(self._threads)

    def die(self, *args):
        gevent.killall(self._threads)
        sys.exit(0)


def main():
    api = RESTApi()
    api.run()


if __name__ == '__main__':
    main()
