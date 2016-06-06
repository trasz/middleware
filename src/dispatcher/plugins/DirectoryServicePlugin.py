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
from task import TaskException

from freenas.dispatcher.rpc import (
    RpcException,
    accepts,
    description,
    returns,
    SchemaHelper as h
)
from task import (
    query,
    Provider,
    Task,
    VerifyException
)

from freenas.utils import normalize

logger = logging.getLogger('DirectoryServicePlugin')


class DirectoryServicesProvider(Provider):
    @query('directoryservice')
    def query(self, filter=None, params=None):
        def extend(directory):
            return directory

        return self.datastore.query('directories', *(filter or []), callback=extend, **(params or {}))


class DirectoryServicesConfigureTask(Task):
    def verify(self, updated_params):
        return ['system']

    def run(self, updated_params):
        node = ConfigNode('network', self.configstore)
        node.update(updated_params)

        try:
            self.dispatcher.call_sync('dscached.management.reload_config')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure directory services: {0}'.format(str(e)))


@accepts(
    h.ref('directory'),
    h.required('name', 'plugin'),
    h.forbidden('immutable')
)
@returns(str)
class DirectoryServiceCreateTask(Task):
    def verify(self, directory):
        return ['system']

    def run(self, directory):
        try:
            params = self.dispatcher.call_sync(
                'dscached.management.normalize_parameters',
                directory['plugin'],
                directory.get('parameters', {})
            )
        except RpcException as err:
            raise TaskException(err.code, err.message)

        normalize(directory, {
            'enabled': False,
            'enumerate': True,
            'immutable': False,
            'uid_range': None,
            'gid_range': None,
            'parameters': params
        })

        if directory['plugin'] == 'winbind':
            normalize(directory, {
                'uid_range': [100000, 999999],
                'gid_range': [100000, 999999]
            })

        self.id = self.datastore.insert('directories', directory)
        self.dispatcher.call_sync('dscached.management.configure_directory', self.id)
        self.dispatcher.dispatch_event('directory.changed', {
            'operation': 'create',
            'ids': [self.id]
        })

        return self.id

    def rollback(self, directory):
        if hasattr(self, 'id'):
            self.datastore.remove('directories', self.id)


@accepts(str, h.ref('directory'))
class DirectoryServiceUpdateTask(Task):
    def verify(self, id, updated_params):
        return ['system']

    def run(self, id, updated_params):
        directory = self.datastore.get_by_id('directories', id)
        if directory['immutable']:
            raise TaskException(errno.EPERM, 'Directory {0} is immutable'.format(directory['name']))

        directory.update(updated_params)
        self.datastore.update('directories', id, directory)
        self.dispatcher.call_sync('dscached.management.configure_directory', id)
        self.dispatcher.dispatch_event('directory.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str)
class DirectoryServiceDeleteTask(Task):
    def verify(self, id):
        pass

    def run(self, id):
        directory = self.datastore.get_by_id('directories', id)
        if directory['immutable']:
            raise TaskException(errno.EPERM, 'Directory {0} is immutable'.format(directory['name']))

        self.datastore.delete('directories', id)
        self.dispatcher.call_sync('dscached.management.configure_directory', id)
        self.dispatcher.dispatch_event('directory.changed', {
            'operation': 'delete',
            'ids': [id]
        })


def _init(dispatcher, plugin):
    plugin.register_schema_definition('directoryservice-config', {
        'type': 'object',
        'properties': {
            'search_order': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'cache_ttl': {'type': 'integer'},
            'cache_enumerations': {'type': 'boolean'},
            'cache_lookups': {'type': 'boolean'}
        }
    })

    plugin.register_schema_definition('directory',  {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'priority': {'type': 'integer'},
            'plugin': {'type': 'string'},
            'enabled': {'type': 'boolean'},
            'enumerate': {'type': 'boolean'},
            'uid_range': {
                'type': ['array', 'null'],
                'items': [
                    {'type': 'integer'},
                    {'type': 'integer'}
                ]
            },
            'gid_range': {
                'type': ['array', 'null'],
                'items': [
                    {'type': 'integer'},
                    {'type': 'integer'}
                ]
            },
            'parameters': {
                'type': 'object'
            },
            'status': {
                'type': 'object'
            }
        }
    })

    plugin.register_provider('directory', DirectoryServicesProvider)
    plugin.register_event_type('directory.changed')
    plugin.register_task_handler('directory.create', DirectoryServiceCreateTask)
    plugin.register_task_handler('directory.update', DirectoryServiceUpdateTask)
    plugin.register_task_handler('directory.delete', DirectoryServiceDeleteTask)
