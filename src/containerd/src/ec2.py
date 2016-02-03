#
# Copyright 2015 iXsystems, Inc.
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
import ipaddress
import logging
import gevent
from gevent.wsgi import WSGIServer


class EC2Metadata(object):
    def __init__(self, context, vm):
        self.context = context
        self.vm = vm

    def __getitem__(self, item):
        if item == 'instance-id':
            return str(self.vm.id)

        if item == 'local-hostname':
            return self.vm.name

        if item == 'user-data':
            return self.vm.config['cloud_init']

        return ''


class EC2MetadataServer(object):
    def __init__(self, context):
        self.context = context
        self.logger = logging.getLogger('EC2MetadataServer')
        self.server = None
        self.thread = None

    def request(self, environ, start_response):
        addr = ipaddress.ip_address(environ['REMOTE_ADDR'])
        vm = self.context.vm_by_mgmt_ip(addr)
        self.logger.info('Request from {0} (vm {1})'.format(addr, vm.name))
        metadata = EC2Metadata(self.context, vm)

        if environ['PATH_INFO'].startswith('/2009-04-04/meta-data/'):
            content = metadata[os.path.basename(environ['PATH_INFO'])]

        elif environ['PATH_INFO'] == '/2009-04-04/user-data':
            content = metadata['user-data']

        else:
            start_response('500', [])
            return []

        start_response('200 OK', [
             ('Content-Type', 'text/cloud-config')
        ])
        return [content.encode('utf-8')]

    def find_vm(self, address):
        pass

    def start(self):
        self.server = WSGIServer(('169.254.169.254', 80), self.request)
        self.thread = gevent.spawn(self.server.serve_forever)
