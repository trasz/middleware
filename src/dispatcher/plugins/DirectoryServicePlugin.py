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
from freenas.dispatcher.rpc import RpcException, accepts, returns, SchemaHelper as h, generator
from task import Provider, Task, TaskDescription, TaskException, query


from freenas.utils import normalize, query as q

logger = logging.getLogger('DirectoryServicePlugin')


class DirectoryServicesProvider(Provider):
    @query('directory')
    @generator
    def query(self, filter=None, params=None):
        def extend(directory):
            directory['status'] = self.dispatcher.call_sync('dscached.management.get_status', directory['id'])
            return directory

        return q.query(
            self.datastore.query('directories', callback=extend),
            *(filter or []),
            stream=True,
            **(params or {})
        )


@accepts(h.ref('directoryservice-config'))
class DirectoryServicesConfigureTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating directory services settings"

    def describe(self, updated_params):
        return TaskDescription(self.early_describe())

    def verify(self, updated_params):
        return ['system']

    def run(self, updated_params):
        node = ConfigNode('directory', self.configstore)
        node.update(updated_params)

        try:
            self.dispatcher.call_sync('dscached.management.reload_config')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure directory services: {0}'.format(str(e)))


@accepts(
    h.ref('directory'),
    h.required('name', 'type'),
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
                directory['type'],
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

        if directory['type'] == 'winbind':
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
        return ['system']

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
            'name': {'type': 'string'},
            'priority': {'type': 'integer'},
            'type': {
                'type': 'string',
                'enum': ['file', 'local', 'winbind', 'freeipa', 'nis']
            },
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
    plugin.register_task_handler('directoryservice.update', DirectoryServicesConfigureTask)
