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

from task import Provider, Task, ProgressTask, TaskException, TaskDescription
from cache import EventCacheStore
from freenas.utils.query import wrap


containers = None


class DockerHostProvider(Provider):
    def query(self, filter=None, params=None):
        def extend(obj):
            ret = {
                'id': obj['id'],
                'name': obj['name'],
                'state': 'DOWN',
                'status': None
            }

            try:
                ret['status'] = self.dispatcher.call_sync('containerd.docker.get_host_status', obj['id'])
                ret['state'] = 'UP'
            except:
                pass

            return ret

        results = self.datastore.query('vms', ('config.docker_host', '=', True), callback=extend)
        return wrap(results).query(*(filter or []), **(params or {}))


class DockerContainerProvider(Provider):
    def query(self, filter=None, params=None):
        containers = wrap(self.dispatcher.call_sync('containerd.docker.query_containers'))
        return containers.query(*(filter or []), **(params or {}))


class DockerImagesProvider(Provider):
    def query(self, filter=None, params=None):
        containers = wrap(self.dispatcher.call_sync('containerd.docker.query_images'))
        return containers.query(*(filter or []), **(params or {}))


class DockerContainerCreateTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Creating a Docker container"

    def describe(self, container):
        return TaskDescription("Creating Docker container {name}".format(name=container['names'][0]))

    def verify(self, container):
        return []

    def run(self, container):
        # Check if we have required image
        pass

        container['name'] = container['names'][0]
        self.dispatcher.call_sync('containerd.docker.create', container)


class DockerContainerDeleteTask(ProgressTask):
    pass


class DockerContainerStartTask(Task):
    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.start', id)


class DockerContainerStopTask(Task):
    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.stop', id)


class DockerImagePullTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Pulling docker image"

    def describe(self, name, hostid):
        return TaskDescription("Pulling docker image {name}".format(name=name))

    def verify(self, name, hostid):
        return []

    def run(self, name, hostid):
        for i in self.dispatcher.call_sync('containerd.docker.pull', name, hostid, timeout=3600):
            if 'progressDetail' in i and 'current' in i['progressDetail']:
                percentage = i['progressDetail']['current'] / i['progressDetail']['total'] * 100
                self.set_progress(percentage, '{0} layer {1}'.format(i['status'], i['id']))


def _depends():
    return ['VMPlugin']


def _init(dispatcher, plugin):
    containers = EventCacheStore(dispatcher, 'docker.container')

    plugin.register_provider('docker.host', DockerHostProvider)
    plugin.register_provider('docker.container', DockerContainerProvider)
    plugin.register_provider('docker.image', DockerImagesProvider)

    plugin.register_task_handler('docker.container.create', DockerContainerCreateTask)
    plugin.register_task_handler('docker.container.delete', DockerContainerDeleteTask)
    plugin.register_task_handler('docker.container.start', DockerContainerStartTask)
    plugin.register_task_handler('docker.container.stop', DockerContainerStopTask)

    plugin.register_task_handler('docker.image.pull', DockerImagePullTask)

    plugin.register_event_type('docker.host.changed')
    plugin.register_event_type('docker.container.changed')

    plugin.register_schema_definition('docker', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'command': {'type': 'string'},
            'image': {'type': 'string'},
            'host': {'type': 'string'},
            'hostname': {'type': ['string', 'null']},
            'memory_limit': {'type': ['integer', 'null']},
            'expose_ports': {'type': 'boolean'},
            'environment': {
                'type': 'object',
                'additionalProperties': {'type': 'string'}
            },
            'ports': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'protocol': {'$ref': 'docker-port-protocol'},
                        'container_port': {
                            'type': 'integer',
                            'minimum': 0,
                            'maximum': 65535
                        },
                        'host_port': {
                            'type': 'integer',
                            'minimum': 0,
                            'maximum': 65535
                        }
                    }
                }
            },
            'volumes': {
                'type': 'array',
                'items': {'$ref': 'docker-volume'}
            }
        }
    })

    plugin.register_schema_definition('docker-volume', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'path': {'type': 'string'},
            'readonly': {'type': 'boolean'}
        }
    })

    plugin.register_schema_definition('docker-port-protocol', {
        'type': 'string',
        'enum': ['TCP', 'UDP']
    })
