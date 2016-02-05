import falcon
import gevent
import signal
import sys

from freenas.dispatcher.client import Client
from gevent.wsgi import WSGIServer


class RESTApi(object):

    def __init__(self):
        self._threads = []
        self.api = falcon.API()
        self.dispatcher = Client()
        self.dispatcher.connect('unix:')
        self.dispatcher.login_service('restd')

        gevent.signal(signal.SIGINT, self.die)

    def __call__(self, environ, start_response):
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
