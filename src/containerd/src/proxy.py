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

import gevent
import socket
import logging
from gevent.lock import RLock


BUFSIZE = 4096


class ReverseProxyServer(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.proxies = {}
        self.rlock = RLock()

    def unix_to_tcp_proxy(self, listen, target):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', listen))
        s.listen(1)

        def reader(cfd, sfd):
            buffer = bytearray(BUFSIZE)
            while True:
                try:
                    n = sfd.recv_into(buffer)
                    if n == 0:
                        break

                    cfd.sendall(buffer[:n])
                except OSError:
                    break

        def writer(cfd, sfd):
            buffer = bytearray(BUFSIZE)
            while True:
                try:
                    n = cfd.recv_into(buffer)
                    if n == 0:
                        break

                    sfd.sendall(buffer[:n])
                except OSError:
                    break

        while True:
            cfd, addr = s.accept()
            self.logger.debug('New client {0} on {1}'.format(addr, listen))

            sfd = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            sfd.connect(target)
            gevent.spawn(reader, cfd, sfd)
            gevent.spawn(writer, cfd, sfd)

    def add_proxy(self, listen, target, timeout=300):
        with self.rlock:
            self.logger.debug('Adding proxy from {0} to 0.0.0.0:{1}'.format(target, listen))
            worker = gevent.spawn(self.unix_to_tcp_proxy, listen, target)
            self.proxies[listen] = worker

    def remove_proxy(self, listen):
        with self.rlock:
            self.logger.debug('Adding proxy to 0.0.0.0:{0}'.format(listen))
            worker = self.proxies.get(listen)
            if not worker:
                return

            gevent.kill(worker)
            del self.proxies[listen]
