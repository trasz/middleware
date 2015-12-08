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
import os
import uuid
from task import Provider, Task, VerifyException, TaskException, query
from freenas.dispatcher.rpc import RpcException
from freenas.dispatcher.rpc import SchemaHelper as h, description, accepts, returns, private
from freenas.utils import first_or_default, normalize


@query('container')
class ContainerProvider(Provider):
    def query(self, filter=None, params=None):
        def extend(obj):
            obj['status'] = self.dispatcher.call_sync('containerd.management.get_status', obj['id'])
            return obj

        return self.datastore.query('containers', *(filter or []), callback=extend, **(params or {}))

    def get_disk_path(self, container_id, disk_name):
        container = self.datastore.get_by_id('containers', container_id)
        if not container:
            return None

        disk = first_or_default(lambda d: d['name'] == disk_name, container['devices'])
        if not disk:
            return None

        if disk['type'] == 'DISK':
            return os.path.join('/dev/zvol', container['target'], 'vm', container['name'], disk_name)

        if disk['type'] == 'CDROM':
            return disk['properties']['path']

    def get_default_interface(self):
        return self.configstore.get('container.default_nic')


class ContainerBaseTask(Task):
    def init_dataset(self, container):
        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        container_ds = os.path.join(root_ds, container['name'])

        if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', root_ds)], {'single': True}):
            # Create VM root
            self.join_subtasks(self.run_subtask('volume.dataset.create', pool, root_ds, 'FILESYSTEM'))

        self.join_subtasks(self.run_subtask('volume.dataset.create', pool, container_ds, 'FILESYSTEM'))

    def create_device(self, container, res):
        if res['type'] == 'DISK':
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            ds_name = os.path.join(container_ds, res['name'])
            self.join_subtasks(self.run_subtask(
                'volume.dataset.create',
                container['target'],
                ds_name,
                'VOLUME',
                {'volsize': res['properties']['size']}
            ))

    def update_device(self, container, old_res, new_res):
        pass

    def delete_device(self, container, res):
        pass


@accepts(h.ref('container'))
class ContainerCreateTask(ContainerBaseTask):
    def verify(self, container):
        if not self.dispatcher.call_sync('volume.query', [('name', '=', container['target'])], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(container['target']))

        return ['zpool:{0}'.format(container['target'])]

    def run(self, container):
        normalize(container, {
            'config': {},
            'devices': []
        })

        normalize(container['config'], {
            'memsize': 512,
            'ncpus': 1
        })

        self.init_dataset(container)
        for res in container['devices']:
            if 'id' not in res:
                res['id'] = str(uuid.uuid4())

            self.create_device(container, res)

        self.datastore.insert('containers', container)


@accepts(str, h.ref('container'))
class ContainerUpdateTask(ContainerBaseTask):
    def verify(self, id, updated_params):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        container = self.datastore.get_by_id('containers', id)

        if 'devices' in updated_params:
            for res in updated_params['devices']:
                existing = first_or_default(lambda i: i['name'] == res['name'], container['devices'])
                if existing:
                    self.update_device(container, existing, res)
                else:
                    self.create_device(container, res)

        container.update(updated_params)
        self.datastore.update('containers', id, container)


@accepts(str)
class ContainerDeleteTask(Task):
    def verify(self, id):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id):
        container = self.datastore.get_by_id('containers', id)
        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        container_ds = os.path.join(root_ds, container['name'])

        try:
            self.join_subtasks(self.run_subtask('volume.dataset.delete', pool, container_ds, True))
        except RpcException as err:
            if err.code != errno.ENOENT:
                raise err

        self.datastore.delete('containers', id)


@accepts(str)
class ContainerStartTask(Task):
    def verify(self, id):
        return ['system']

    def run(self, id):
        self.dispatcher.call_sync('containerd.management.start_container', id)


@accepts(str)
class ContainerStopTask(Task):
    def verify(self, id):
        return ['system']

    def run(self, id):
        self.dispatcher.call_sync('containerd.management.stop_container', id)


def _init(dispatcher, plugin):
    plugin.register_schema_definition('container', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'target': {'type': 'string'},
            'type': {
                'type': 'string',
                'enum': ['JAIL', 'VM', 'DOCKER']
            },
            'config': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'memsize': {'type': 'integer'},
                    'ncpus': {'type': 'integer'},
                    'bootloader': {
                        'type': 'string',
                        'enum': ['BHYVELOAD', 'GRUB']
                    },
                    'boot_device': {'type': ['string', 'null']},
                    'boot_partition': {'type': ['string', 'null']},
                }
            },
            'devices': {
                'type': 'array',
                'items': {'$ref': 'container-device'}
            }
        }
    })

    plugin.register_schema_definition('container-device', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'name': {'type': 'string'},
            'type': {
                'type': 'string',
                'enum': ['DISK', 'CDROM', 'NIC']
            },
            'properties': {'type': 'object'}
        },
        'requiredProperties': ['name', 'type', 'properties']
    })

    plugin.register_schema_definition('container-device-nic', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'mode': {
                'type': 'string',
                'enum': ['BRIDGED', 'NAT', 'HOSTONLY']
            },
            'bridge': {'type': ['string', 'null']}
        }
    })

    plugin.register_schema_definition('container-device-disk', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'size': {'type': 'integer'}
        }
    })

    plugin.register_schema_definition('container-device-cdrom', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'path': {'type': 'string'}
        }
    })

    plugin.register_provider('container', ContainerProvider)
    plugin.register_task_handler('container.create', ContainerCreateTask)
    plugin.register_task_handler('container.update', ContainerUpdateTask)
    plugin.register_task_handler('container.delete', ContainerDeleteTask)
    plugin.register_task_handler('container.start', ContainerStartTask)
    plugin.register_task_handler('container.stop', ContainerStopTask)

    plugin.register_event_type('container.changed')
