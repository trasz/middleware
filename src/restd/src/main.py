import base64
import binascii
import errno
import falcon
import gevent
import glob
import importlib.machinery
import json
import logging
import os
import signal
import sys
import time

from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcException
from freenas.utils import configure_logging
from gevent.wsgi import WSGIServer

from base import CRUDBase


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


class AuthMiddleware(object):

    def process_request(self, req, resp):
        auth = req.get_header("Authorization")

        if auth is None or not auth.startswith('Basic '):
            raise falcon.HTTPUnauthorized(
                'Authorization token required',
                'Provide a Basic Authentication header'
            )
        try:
            username, password = base64.b64decode(auth[6:]).decode('utf8').split(':', 1)
        except binascii.Error:
            raise
            raise falcon.HTTPUnauthorized(
                'Invalid Authorization token',
                'Provide a valid Basic Authentication header',
            )

        try:
            client = Client()
            client.connect('unix:')
            client.login_user(username, password)
            req.context['client'] = client
        except RpcException as e:
            if e.code == errno.EACCES:
                raise falcon.HTTPUnauthorized(
                    'Invalid credentials',
                    'Verify your credentials and try again.',
                )
            raise falcon.HTTPUnauthorized('Unknown authentication error', str(e))


class RESTApi(object):

    def __init__(self):
        self.logger = logging.getLogger('restd')
        self._threads = []
        self.api = falcon.API(middleware=[
            AuthMiddleware(),
            JSONTranslator(),
        ])

        gevent.signal(signal.SIGINT, self.die)

    def init_dispatcher(self):
        def on_error(reason, **kwargs):
            if reason in (ClientError.CONNECTION_CLOSED, ClientError.LOGOUT):
                self.logger.warning('Connection to dispatcher lost')
                self.connect()

        self.dispatcher = Client()
        self.dispatcher.on_error(on_error)
        self.connect()

    def load_plugins(self):
        pluginsdir = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
        for i in glob.glob1(pluginsdir, "*.py"):
            loader = importlib.machinery.SourceFileLoader(i.split('.')[0], os.path.join(pluginsdir, i))
            mod = loader.load_module()
            mod._init(self)

    def connect(self):
        while True:
            try:
                self.dispatcher.connect('unix:')
                self.dispatcher.login_service('restd')
                return
            except (OSError, RpcException) as err:
                self.logger.warning('Cannot connect to dispatcher: {0}, retrying in 1 second'.format(str(err)))
                time.sleep(1)

    def __call__(self, environ, start_response):
        if 'HTTP_X_REAL_IP' in environ:
            environ['PATH_INFO'] = environ.get('PATH_INFO', '').replace('/api/v2.0', '', 1)
        return self.api.__call__(environ, start_response)

    def register_crud(self, klass):
        klass(self, self.dispatcher)

    def run(self):
        self.init_dispatcher()
        self.load_plugins()

        server4 = WSGIServer(('', 8889), self)
        self._threads = [gevent.spawn(server4.serve_forever)]
        gevent.joinall(self._threads)

    def die(self, *args):
        gevent.killall(self._threads)
        sys.exit(0)


def main():

    configure_logging('/var/log/restd.log', 'DEBUG')

    api = RESTApi()
    api.run()


if __name__ == '__main__':
    main()
