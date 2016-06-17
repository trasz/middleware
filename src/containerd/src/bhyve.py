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

import threading
import socket
import nv
from gevent.event import AsyncResult


class Call(object):
    def __init__(self):
        self.id = None
        self.result = AsyncResult()


class BhyveException(OSError):
    pass


class BhyveClient(object):
    def __init__(self, path):
        self.path = path
        self.thread = None
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        self.calls = {}

    def connect(self):
        self.socket.connect(self.path)

    def disconnect(self):
        self.socket.disconnect()

    def call(self, service, method, args):
        call = Call()
        call.id = max(self.calls.keys())

        msg = nv.NVList({
            'id': id,
            'service': service,
            'method': method,
            'args': args
        })

        msg.send(self.socket)
        return call.result.wait()

    def recv_thread(self):
        while True:
            msg = nv.NVList.recv(self.socket)
            if not msg:
                break

            if 'event' in msg:
                pass

            if 'error' in msg:
                call = self.calls[msg['id']]
                error = msg['error']

                if error == 0:
                    call.result.set(msg['response'])
                    continue
                else:
                    call.resul.set_exception(BhyveException())
                    continue
