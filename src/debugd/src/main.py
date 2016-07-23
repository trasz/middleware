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
import setproctitle
import time
import threading
import logging
import argparse
import msock.channel
import msock.client
from freenas.dispatcher.rpc import RpcContext
from freenas.dispatcher.server import Server


SUPPORT_PROXY_ADDRESS = 'tcp://127.0.0.1:5678'  # XXX


class ShellChannel(msock.channel.Channel):
    def __init__(self, connection, id, metadata):
        super(ShellChannel, self).__init__(connection, id, metadata)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pid, self.master = pty.fork()

        if self.pid == 0:
            os.execv(metadata['path'], metadata['args'])

        self.logger.info('Spawned {0} as pid {1}'.format(metadata['path'], self.pid))
        self.reader_thread = threading.Thread(target=self.reader, daemon=True, name='shell reader thread')
        self.reader_thread.start()

    def on_data(self, data):
        os.write(self.master, data)

    def reader(self):
        while True:
            data = os.read(self.master, 1024)
            if data == b'':
                self.logger.debug('Closing shell session')
                self.close()
                return

            self.write(data)


class FileTransferChannel(msock.channel.Channel):
    def __init__(self, connection, id, metadata):
        super(FileTransferChannel, self).__init__(connection, id, metadata)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.upload = metadata['direction']
        self.file = open(metadata['path'])
        if self.upload:
            self.reader_thread = threading.Thread(target=self.reader, daemon=True, name='file reader thread')
            self.reader_thread.start()

    def on_data(self, data):
        if not self.upload:
            if not data:
                self.file.close()

            self.file.write(data)

    def reader(self):
        while True:
            data = os.read(self.file.fileno(), 1024)
            if data == b'':
                self.file.close()
                self.close()
                return

            self.write(data)


class ControlRPCChannel(msock.channel.Channel):
    def __init__(self, context, connection, id, metadata):
        super(ControlRPCChannel, self).__init__(connection, id, metadata)
        self.context = connection

    def on_data(self, data):
        pass


class DispatcherRPCChannel(msock.channel.Channel):
    pass


class Context(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.msock = msock.client.Client()
        self.msock.on_channel_created = self.on_channel_created
        self.msock.on_channel_destroyed = self.on_channel_destroyed
        self.msock.channel_factory = self.channel_factory
        self.rpc = RpcContext()
        self.server = Server(self)

    def start(self):
        signal.signal(signal.SIGUSR2, lambda signo, frame: self.connect())

    def channel_factory(self, id, metadata):
        if metadata['type'] == 'shell':
            return ShellChannel(self.msock, id, metadata)

        if metadata['type'] == 'file':
            return FileTransferChannel(self.msock, id, metadata)

        if metadata['type'] == 'dispatcher':
            return DispatcherRPCChannel(self.msock, id, metadata)

        if metadata['type'] == 'rpc':
            return ControlRPCChannel(self, self.msock, id, metadata)

        raise ValueError('Unknown channel type')

    def on_channel_created(self, chan):
        pass

    def on_channel_destroyed(self, chan):
        pass

    def connect(self):
        self.msock.connect(SUPPORT_PROXY_ADDRESS)
        self.logger.info('Connecting to {0}'.format(SUPPORT_PROXY_ADDRESS))


def main():
    parser = argparse.ArgumentParser()
    logging.basicConfig(level=logging.DEBUG)
    context = Context()
    context.start()
    setproctitle.setproctitle('debugd')
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
