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
import socket
import struct
import array
import errno
import logging
from freenas.utils import xrecvmsg, xsendmsg


class UnixSocketServer(object):
    class UnixSocketHandler(object):
        def __init__(self, server, dispatcher, connfd, address):
            import types
            self.dispatcher = dispatcher
            self.connfd = connfd
            self.address = address
            self.server = server
            self.handler = types.SimpleNamespace()
            self.handler.client_address = ("unix", 0)
            self.handler.server = server
            self.conn = None
            self.wlock = RLock()

        def send(self, message, fds=None):
            if fds is None:
                fds = []

            with self.wlock:
                data = message.encode('utf-8')
                header = struct.pack('II', 0xdeadbeef, len(data))
                try:
                    fd = self.connfd.fileno()
                    ancdata = []
                    if fd == -1:
                        return

                    if fds:
                        ancdata.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [i.fd for i in fds])))

                    wait_write(fd, 10)
                    xsendmsg(self.connfd, header + data, ancdata)
                    for i in fds:
                        if i.close:
                            try:
                                os.close(i.fd)
                            except OSError:
                                pass

                except (OSError, ValueError, socket.timeout) as err:
                    self.server.logger.info('Send failed: {0}; closing connection'.format(str(err)))
                    if err.errno != errno.EBADF:
                        self.connfd.shutdown(socket.SHUT_RDWR)

        def handle_connection(self):
            self.conn = ServerConnection(self, self.dispatcher)
            self.conn.on_open()

            while True:
                try:
                    fds = array.array('i')
                    header, ancdata = xrecvmsg(
                        self.connfd, 8,
                        socket.CMSG_SPACE(MAXFDS * fds.itemsize) + socket.CMSG_SPACE(CMSGCRED_SIZE)
                    )

                    if header == b'' or len(header) != 8:
                        if len(header) > 0:
                            self.server.logger.info('Short read (len {0})'.format(len(header)))
                        break

                    magic, length = struct.unpack('II', header)
                    if magic != 0xdeadbeef:
                        self.server.logger.info('Message with wrong magic dropped (magic {0:x})'.format(magic))
                        break

                    msg, _ = xrecvmsg(self.connfd, length)
                    if msg == b'' or len(msg) != length:
                        self.server.logger.info('Message with wrong length dropped; closing connection')
                        break

                    for cmsg_level, cmsg_type, cmsg_data in ancdata:
                        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_CREDS:
                            pid, uid, euid, gid = struct.unpack('iiii', cmsg_data[:struct.calcsize('iiii')])
                            self.handler.client_address = ('unix', pid)
                            self.conn.credentials = {
                                'pid': pid,
                                'uid': uid,
                                'euid': euid,
                                'gid': gid
                            }

                        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                            fds.fromstring(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])

                except (OSError, ValueError) as err:
                    self.server.logger.info('Receive failed: {0}; closing connection'.format(str(err)), exc_info=True)
                    break

                self.conn.on_message(msg, fds=fds)

            self.close()

        def close(self):
            if self.conn:
                self.conn.on_close('Bye bye')
                self.conn = None
                try:
                    self.connfd.shutdown(socket.SHUT_RDWR)
                    self.connfd.close()
                except OSError:
                    pass

    def __init__(self, path, handler):
        self.path = path
        self.handler = handler
        self.sockfd = None
        self.backlog = 50
        self.logger = logging.getLogger('UnixSocketServer')
        self.connections = []

    def broadcast_event(self, event, args):
        for i in self.connections:
            i.emit_event(event, args)

    def serve_forever(self):
        try:
            if os.path.exists(self.path):
                os.unlink(self.path)

            self.sockfd = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sockfd.bind(self.path)
            os.chmod(self.path, 775)
            self.sockfd.listen(self.backlog)
        except OSError as err:
            self.logger.error('Cannot start socket server: {0}'.format(str(err)))
            return

        while True:
            try:
                fd, addr = self.sockfd.accept()
            except OSError as err:
                self.logger.error('accept() failed: {0}'.format(str(err)))
                continue

            handler = self.UnixSocketHandler(self, self.dispatcher, fd, addr)
            gevent.spawn(handler.handle_connection)
