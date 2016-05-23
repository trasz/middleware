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

import errno
import logging

from datastore.config import ConfigNode
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from task import Task, Provider, TaskException, ValidationException

logger = logging.getLogger('WebDAVPlugin')


@description('Provides info about WebDAV service configuration')
class WebDAVProvider(Provider):
    @private
    @accepts()
    @returns(h.ref('service-webdav'))
    def get_config(self):
        return ConfigNode('service.webdav', self.configstore).__getstate__()


@private
@description('Configure WebDAV service')
@accepts(h.ref('service-webdav'))
class WebDAVConfigureTask(Task):
    def describe(self, share):
        return 'Configuring WebDAV service'

    def verify(self, webdav):
        errors = ValidationException()
        node = ConfigNode('service.webdav', self.configstore).__getstate__()
        node.update(webdav)

        if node['http_port'] == node['https_port']:
            errors.add((0, 'http_port'), 'HTTP and HTTPS ports cannot be the same')

        if 'HTTPS' in node['protocol'] and not node['certificate']:
            errors.add((0, 'certificate'), 'SSL protocol specified without choosing a certificate')

        if node['certificate']:
            cert = self.dispatcher.call_sync('crypto.certificate.query', [('id', '=', node['certificate'])])
            if not cert:
                errors.add((0, 'certificate'), 'SSL Certificate not found.')

        if errors:
            raise errors

        return ['system']

    def run(self, webdav):
        try:
            node = ConfigNode('service.webdav', self.configstore)
            node.update(webdav)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'services')
        except RpcException as e:
            raise TaskException(
                errno.ENXIO, 'Cannot reconfigure WebDAV: {0}'.format(str(e))
            )

        return 'RESTART'


def _depends():
    return ['CryptoPlugin', 'ServiceManagePlugin']


def _init(dispatcher, plugin):
    # Register schemas
    plugin.register_schema_definition('service-webdav', {
        'type': 'object',
        'properties': {
            'type': {'enum': ['service-webdav']},
            'enable': {'type': 'boolean'},
            'protocol': {
                'type': ['array'],
                'items': {
                    'type': 'string',
                    'enum': ['HTTP', 'HTTPS'],
                },
            },
            'http_port': {
                'type': 'integer',
                'minimum': 1,
                'maximum': 65535
            },
            'https_port': {
                'type': 'integer',
                'minimum': 1,
                'maximum': 65535
            },
            'password': {'type': 'string'},
            'authentication': {'type': 'string', 'enum': [
                'BASIC',
                'DIGEST',
            ]},
            'certificate': {'type': ['string', 'null']},
        },
        'additionalProperties': False,
    })

    # Register providers
    plugin.register_provider("service.webdav", WebDAVProvider)

    # Register tasks
    plugin.register_task_handler("service.webdav.update", WebDAVConfigureTask)
