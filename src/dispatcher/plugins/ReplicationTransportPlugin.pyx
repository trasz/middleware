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

import cython
import libzfs
import errno
import socket
import logging
import os
from datastore.config import ConfigNode
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from resources import Resource
from task import Task, Provider, TaskException, ValidationException

logger = logging.getLogger('ReplicationTransportPlugin')


@private
@description('Set up a TCP connection for replication purposes')
@accepts(h.ref('replication-transport'))
class TransportCreateTask(Task):
    def describe(self, transport):
        return 'Setting up a replication transport'

    def verify(self, transport):

        return ['system']

    def run(self, afp):
        return


def _init(dispatcher, plugin):
    # Register schemas
    plugin.register_schema_definition('replication-transport', {
        'type': 'object',
        'properties': {
            'server_address': {'type': 'string'},
            'server_port': {'type': 'integer'},
            'buffer_size': {'type': 'integer'},
            'auth_token_size': {'type': 'integer'},
            'transport_plugins': {
                'type': ['array', 'null'],
                'items': {'$ref': 'replication-transport-plugin'},
            }
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('replication-transport-plugin', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'params': {'type': 'object'}
        },
        'additionalProperties': False
    })

    # Register tasks
    plugin.register_task_handler("replication.transport.create", TransportCreateTask)
