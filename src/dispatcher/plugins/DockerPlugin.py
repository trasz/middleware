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
import dockerhub
import logging
from task import Provider, Task, ProgressTask, TaskDescription, TaskException
from cache import EventCacheStore
from datastore.config import ConfigNode
from freenas.utils import normalize, query as q
from freenas.dispatcher.rpc import generator, accepts, returns, SchemaHelper as h, RpcException

logger = logging.getLogger(__name__)

containers = None
containers_query = 'containerd.docker.query_containers'
images = None
images_query = 'containerd.docker.query_images'


class DockerProvider(Provider):
    @returns(h.ref('docker-config'))
    def get_config(self):
        return ConfigNode('container.docker', self.configstore).__getstate__()


class DockerHostProvider(Provider):
    @generator
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
            except RpcException:
                pass

            return ret

        with self.dispatcher.get_lock('vms'):
            results = self.datastore.query('vms', ('config.docker_host', '=', True), callback=extend)
        return q.query(results, *(filter or []), stream=True, **(params or {}))


class DockerContainerProvider(Provider):
    @generator
    def query(self, filter=None, params=None):
        return containers.query(*(filter or []), stream=True, **(params or {}))


class DockerImagesProvider(Provider):
    @generator
    def query(self, filter=None, params=None):
        return images.query(*(filter or []), stream=True, **(params or {}))

    @generator
    def search(self, term):
        hub = dockerhub.DockerHub()
        for i in hub.search(term):
            yield {
                'name': i['repo_name'],
                'description': i['short_description'],
                'star_count': i['star_count'],
                'pull_count': i['pull_count']
            }

    def readme(self, repo_name):
        hub = dockerhub.DockerHub()
        try:
            return hub.get_repository(repo_name).get('full_description')
        except ValueError:
            return None

    def get_hub_image(self, name):
        pass


@accepts(h.ref('docker-config'))
class DockerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating Docker global configuration"

    def describe(self, container):
        return TaskDescription("Updating Docker global configuration")

    def verify(self, updated_params):
        return ['system']

    def run(self, updated_params):
        node = ConfigNode('container.docker', self.configstore)
        node.update(updated_params)


class DockerContainerCreateTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Creating a Docker container"

    def describe(self, container):
        return TaskDescription("Creating Docker container {name}".format(name=container['names'][0]))

    def verify(self, container):
        return []

    def run(self, container):
        normalize(container, {
            'hostname': None,
            'memory_limit': None,
            'volumes': [],
            'ports': [],
            'expose_ports': False,
        })

        # Check if we have required image
        pass

        container['name'] = container['names'][0]
        self.dispatcher.call_sync('containerd.docker.create', container)


class DockerContainerDeleteTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Deleting a Docker container"

    def describe(self, id):
        return TaskDescription("Deleting Docker container {name}".format(name=id))

    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.delete', id)


class DockerContainerStartTask(Task):
    @classmethod
    def early_describe(cls):
        return "Starting container"

    def describe(self, id):
        return TaskDescription("Starting container {name}".format(name=id))

    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.start', id)


class DockerContainerStopTask(Task):
    @classmethod
    def early_describe(cls):
        return "Stopping container"

    def describe(self, id):
        return TaskDescription("Stopping container {name}".format(name=id))

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


class DockerImageDeleteTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Deleting docker image"

    def describe(self, name, hostid):
        return TaskDescription("Deleting docker image {name}".format(name=name))

    def verify(self, name, hostid):
        return []

    def run(self, name, hostid):
        try:
            self.dispatcher.call_sync('containerd.docker.delete_image', name, hostid)
        except RpcException as err:
            raise TaskException('Failed to remove image {0}: {1}'.format(name, err))


def _depends():
    return ['VMPlugin']


def _init(dispatcher, plugin):
    global containers
    global images

    containers = EventCacheStore(dispatcher, 'docker.container')
    images = EventCacheStore(dispatcher, 'docker.image')

    def on_host_event(args):
        if images.ready and containers.ready:
            if args['operation'] == 'create':
                for host_id in args['ids']:
                    new_images = list(dispatcher.call_sync(
                        images_query,
                        [('host', '=', host_id)], {'select': 'id'}
                    ))
                    new_containers = list(dispatcher.call_sync(
                        containers_query,
                        [('host', '=', host_id)], {'select': 'id'}
                    ))

                    if new_images:
                        sync_cache(images, images_query, new_images)
                    if new_containers:
                        sync_cache(containers, containers_query, new_containers)

                    logger.debug('Docker host {0} started'.format(host_id))

            elif args['operation'] == 'delete':
                for host_id in args['ids']:
                    images.remove_many(images.query(('host', '=', host_id), select='id'))
                    containers.remove_many(containers.query(('host', '=', host_id), select='id'))

                    logger.debug('Docker host {0} stopped'.format(host_id))

            dispatcher.dispatch_event('docker.host.changed', {
                'operation': 'update',
                'ids': args['ids']
            })

    def vm_pre_destroy(args):
        host = dispatcher.datastore.query(
            'vms',
            ('config.docker_host', '=', True),
            ('id', '=', args['name']),
            single=True
        )
        if host:
            logger.debug('Docker host {0} deleted'.format(host['name']))
            dispatcher.dispatch_event('docker.host.changed', {
                'operation': 'delete',
                'ids': [args['name']]
            })

    def on_vm_change(args):
        if args['operation'] == 'create':
            for id in args['ids']:
                host = dispatcher.datastore.query(
                    'vms',
                    ('config.docker_host', '=', True),
                    ('id', '=', id),
                    single=True
                )
                if host:
                    logger.debug('Docker host {0} created'.format(host['name']))
                    dispatcher.dispatch_event('docker.host.changed', {
                        'operation': 'create',
                        'ids': [id]
                    })

    def on_image_event(args):
        logger.trace('Received Docker image event: {0}'.format(args))
        if args['ids']:
            sync_cache(images, images_query, args['ids'])

    def on_container_event(args):
        logger.trace('Received Docker container event: {0}'.format(args))
        if args['ids']:
            sync_cache(containers, containers_query, args['ids'])

    def sync_caches():
        interval = dispatcher.configstore.get('container.cache_refresh_interval')
        while True:
            gevent.sleep(interval)
            if images.ready and containers.ready:
                logger.trace('Syncing Docker caches')
                sync_cache(images, images_query)
                sync_cache(containers, containers_query)

    def sync_cache(cache, query, ids=None):
        if ids:
            objects = dispatcher.call_sync(query, [('id', 'in', ids)])
        else:
            objects = dispatcher.call_sync(query)

        cache.update(**{i['id']: i for i in objects})

    def init_cache():
        logger.trace('Initializing Docker caches')
        sync_cache(images, images_query)
        images.ready = True
        sync_cache(containers, containers_query)
        containers.ready = True

    plugin.register_provider('docker', DockerProvider)
    plugin.register_provider('docker.host', DockerHostProvider)
    plugin.register_provider('docker.container', DockerContainerProvider)
    plugin.register_provider('docker.image', DockerImagesProvider)

    plugin.register_task_handler('docker.container.create', DockerContainerCreateTask)
    plugin.register_task_handler('docker.container.delete', DockerContainerDeleteTask)
    plugin.register_task_handler('docker.container.start', DockerContainerStartTask)
    plugin.register_task_handler('docker.container.stop', DockerContainerStopTask)

    plugin.register_task_handler('docker.image.pull', DockerImagePullTask)
    plugin.register_task_handler('docker.image.delete', DockerImageDeleteTask)

    plugin.register_event_type('docker.host.changed')
    plugin.register_event_type('docker.container.changed')
    plugin.register_event_type('docker.image.changed')

    plugin.register_event_handler('containerd.docker.host.changed', on_host_event)
    plugin.register_event_handler('containerd.docker.container.changed', on_container_event)
    plugin.register_event_handler('containerd.docker.image.changed', on_image_event)
    plugin.register_event_handler('vm.changed', on_vm_change)
    plugin.register_event_handler('plugin.service_registered',
                                  lambda a: init_cache() if a['service-name'] == 'containerd.docker' else None)

    plugin.attach_hook('vm.pre_destroy', vm_pre_destroy)

    plugin.register_schema_definition('docker-config', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'default_host': {'type': ['string', 'null']}
        }
    })

    plugin.register_schema_definition('docker', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'command': {'type': 'string'},
            'image': {'type': 'string'},
            'host': {'type': ['string', 'null']},
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

    plugin.register_schema_definition('docker-image', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'names': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'size': {'type': 'integer'},
            'host': {'type': ['string', 'null']}
        }
    })

    plugin.register_schema_definition('docker-hub-image', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'namespace': {'type': 'string'},
            'description': {'type': 'string'},
            'full_description': {'type': 'string'},
            'pull_count': {'type': 'integer'},
            'star_count': {'type': 'integer'},
            'updated_at': {'type': 'datetime'},
        }
    })

    plugin.register_schema_definition('docker-volume', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'container_path': {'type': 'string'},
            'host_path': {'type': 'string'},
            'readonly': {'type': 'boolean'}
        }
    })

    plugin.register_schema_definition('docker-port-protocol', {
        'type': 'string',
        'enum': ['TCP', 'UDP']
    })

    if 'containerd.docker' in dispatcher.call_sync('discovery.get_services'):
        init_cache()

    gevent.spawn(sync_caches)
