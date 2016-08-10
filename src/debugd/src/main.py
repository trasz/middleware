#
# Copyright 2016 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#####################################################################

import os
import pty
import signal
import enum
import uuid
import setproctitle
import time
import threading
import logging
import argparse
import select
import socket
import tarfile
import bsd
import msock.channel
import msock.client
from datetime import datetime
from threading import Condition
from freenas.dispatcher.rpc import RpcContext, RpcService, generator
from freenas.dispatcher.fd import MSockChannelSerializer
from freenas.dispatcher.bridge import Bridge
from freenas.dispatcher.client import Client
from freenas.dispatcher.server import Server
from freenas.utils.permissions import get_type


CORES_DIR = '/var/db/system/cores'
DEFAULT_CONFIGFILE = '/usr/local/etc/debugd.conf'
DEFAULT_SOCKET_ADDRESS = 'unix:///var/run/debugd.sock'
SUPPORT_PROXY_ADDRESS = 'tcp://10.20.0.32:8080'  # XXX


class ConnectionState(enum.Enum):
    OFFLINE = 'offline'
    CONNECTING = 'connecting'
    CONNECTED = 'connected'


class DebugService(RpcService):
    def __init__(self, context):
        self.context = context

    def shell(self, args, fd):
        self.context.run_job(ShellConnection(args, fd))

    def download(self, path, fd):
        job = FileConnection(path, False, fd)
        size = job.filesize
        self.context.run_job(job)
        return size

    def upload(self, path, fd):
        job = FileConnection(path, True, fd)
        self.context.run_job(job)

    def tail(self, path, scrollback_size, fd):
        self.context.run_job(TailConnection(path, fd))

    def dashboard(self, fd):
        self.context.run_job(DashboardConnection(fd))

    @generator
    def listdir(self, directory):
        for i in os.listdir(directory):
            path = os.path.join(directory, i)
            stat = os.lstat(path)
            yield {
                'type': get_type(stat),
                'size': stat.st_size,
                'created_at': datetime.utcfromtimestamp(stat.st_ctime),
                'modified_at': datetime.utcfromtimestamp(stat.st_mtime),
                'name': i
            }

    @generator
    def get_process_list(self):
        procs = bsd.getprocs(bsd.ProcessLookupPredicate.PROC)
        for i in procs:
            try:
                yield {
                    'pid': i.pid,
                    'command': i.command,
                    'argv': list(i.argv),
                    'env': list(i.env)
                }
            except:
                continue

    @generator
    def get_core_files(self):
        for i in os.listdir(CORES_DIR):
            try:
                path = os.path.join(CORES_DIR, i)
                proc = bsd.opencore(path)
                stat = os.stat(path)
                yield {
                    'pid': proc.pid,
                    'command': proc.command,
                    'argv': proc.argv,
                    'created_at': datetime.utcfromtimestamp(stat.st_ctime),
                    'size': stat.st_size
                }
            except OSError:
                continue

    def trace_process(self, pid, fd):
        self.context.jobs.append(ShellConnection(['/usr/bin/truss', '-p', str(pid)], fd))

    def proxy_dispatcher_rpc(self, fd):
        br = Bridge()
        br.start('unix:', 'fd://{0}'.format(fd.fd))


class ControlService(RpcService):
    def __init__(self, context):
        self.context = context

    def connect(self, discard=False):
        pass

    def disconnect(self):
        pass

    def status(self):
        return {
            'state': self.context.state,
            'server': None,
            'connected_at': None,
            'jobs': [j.__getstate__() for j in self.context.jobs]
        }

    def upload(self, filename, description, fd):
        pass


class Job(object):
    def __init__(self):
        self.context = None
        self.created_at = datetime.now()

    def describe(self):
        raise NotImplementedError()

    def close(self):
        if self.context:
            self.context.jobs.remove(self)

    def __getstate__(self):
        return {
            'created_at': self.created_at,
            'description': self.describe()
        }


class ShellConnection(Job):
    def __init__(self, args, fd):
        super(ShellConnection, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Attempting to start shell session of {0}'.format(args))
        self.args = args
        self.fd = fd.fd

        try:
            self.pid, self.master = pty.fork()

            if self.pid == 0:
                os.execv(args[0], args)

            self.logger.info('Spawned {0} as pid {1}'.format(args, self.pid))
            self.reader_thread = threading.Thread(target=self.worker, daemon=True, name='shell reader thread')
            self.reader_thread.start()
        except OSError as err:
            self.logger.error('Cannot start shell connection: {0}'.format(str(err)))
            self.close()

    def describe(self):
        return "Shell: {0}".format(' '.join(self.args))

    def worker(self):
        while True:
            r, _, _ = select.select([self.fd, self.master], [], [])
            for fd in r:
                data = os.read(fd, 1024)
                if data == b'':
                    self.logger.debug('Closing shell session')
                    os.close(self.fd if fd == self.master else self.master)
                    self.close()
                    return

                os.write(self.fd if fd == self.master else self.master, data)


class FileConnection(Job):
    def __init__(self, path, upload, fd):
        super(FileConnection, self).__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.upload = upload
        self.path = path
        self.fd = fd.fd
        self.file = open(path, 'rb' if upload else 'wb')
        self.reader_thread = threading.Thread(target=self.worker, daemon=True, name='file reader thread')
        self.reader_thread.start()

    @property
    def filesize(self):
        stat = os.stat(self.path)
        return stat.st_size

    def describe(self):
        return "File {0}: {1}".format('upload' if self.upload else 'download', self.path)

    def worker(self):
        while True:
            data = os.read(self.file.fileno() if self.upload else self.fd, 1024)
            if data == b'':
                os.close(self.fd)
                self.file.close()
                self.close()
                return

            os.write(self.fd if self.upload else self.file.fileno(), data)


class TailConnection(Job):
    def __init__(self, path, fd):
        super(TailConnection, self).__init__()
        self.path = path
        self.fd = fd

    def describe(self):
        return "File watch: {0}".format(self.path)

    def worker(self):
        pass


class DashboardConnection(Job):
    def __init__(self, fd):
        super(DashboardConnection, self).__init__()
        self.fd = fd.fd


class Context(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.msock = msock.client.Client()
        self.rpc_fd = -1
        self.connection_id = None
        self.jobs = []
        self.state = ConnectionState.OFFLINE
        self.cv = Condition()
        self.rpc = RpcContext()
        self.client = Client()
        self.server = Server()

    def start(self, sockpath):
        signal.signal(signal.SIGUSR2, lambda signo, frame: self.connect())
        self.server.rpc = RpcContext()
        self.server.rpc.register_service_instance('control', ControlService(self))
        self.server.start(sockpath)
        threading.Thread(target=self.server.serve_forever, name='server thread', daemon=True).start()

    def connect(self, discard=False):
        if discard:
            self.connection_id = None

        self.set_state(ConnectionState.CONNECTING)
        self.msock.connect(SUPPORT_PROXY_ADDRESS)
        self.logger.info('Connecting to {0}'.format(SUPPORT_PROXY_ADDRESS))
        self.connection_id = uuid.uuid4()
        self.rpc_fd = self.msock.create_channel(0).fileno()
        time.sleep(1)  # FIXME
        self.client.connect('fd://{0}'.format(self.rpc_fd))
        self.client.channel_serializer = MSockChannelSerializer(self.msock)
        self.client.standalone_server = True
        self.client.enable_server()
        self.client.register_service('debug', DebugService(self))
        self.client.call_sync('server.login', str(self.connection_id), socket.gethostname(), 'none')
        self.set_state(ConnectionState.CONNECTED)

    def run_job(self, job):
        self.jobs.append(job)
        job.context = self
        job.start()

    def set_state(self, state):
        with self.cv:
            self.state = state
            self.cv.notify_all()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', metavar='SOCKET', default=DEFAULT_SOCKET_ADDRESS, help='Socket address to listen on')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    context = Context()
    context.start(args.s)
    context.connect()
    setproctitle.setproctitle('debugd')

    if not os.path.isdir(CORES_DIR):
        os.mkdir(CORES_DIR)

    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
