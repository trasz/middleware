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

import os
import copy
import errno
import gevent
import dockerhub
import logging
from task import Provider, Task, ProgressTask, TaskDescription, TaskException, query, TaskWarning
from cache import EventCacheStore
from datastore.config import ConfigNode
from freenas.utils import normalize, query as q
from freenas.dispatcher.rpc import generator, accepts, returns, SchemaHelper as h, RpcException, description


logger = logging.getLogger(__name__)

containers = None
containers_query = 'containerd.docker.query_containers'
images = None
images_query = 'containerd.docker.query_images'


@description('Provides information about Docker configuration')
class DockerConfigProvider(Provider):
    @description('Returns Docker general configuration')
    @returns(h.ref('docker-config'))
    def get_config(self):
        return ConfigNode('container.docker', self.configstore).__getstate__()


@description('Provides information about Docker container hosts')
class DockerHostProvider(Provider):
    @description('Returns current status of Docker hosts')
    @query('docker-host')
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


@description('Provides information about Docker containers')
class DockerContainerProvider(Provider):
    @description('Returns current status of Docker containers')
    @query('docker-container')
    @generator
    def query(self, filter=None, params=None):
        return containers.query(*(filter or []), stream=True, **(params or {}))

    @description('Requests authorization token for a container console')
    @accepts(str)
    @returns(str)
    def request_serial_console(self, id):
        return self.dispatcher.call_sync('containerd.console.request_console', id)


@description('Provides information about Docker container images')
class DockerImagesProvider(Provider):
    @description('Returns current status of cached Docker container images')
    @query('docker-image')
    @generator
    def query(self, filter=None, params=None):
        return images.query(*(filter or []), stream=True, **(params or {}))

    @description('Returns a result of searching Docker Hub for a specified term - part of image name')
    @accepts(str)
    @returns(h.array(h.ref('docker-hub-image')))
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

    @description('Returns a full description of specified Docker container image')
    @accepts(str)
    @returns(str)
    def readme(self, repo_name):
        hub = dockerhub.DockerHub()
        try:
            return hub.get_repository(repo_name).get('full_description')
        except ValueError:
            return None

    def get_templates(self):
        return {
            'busybox': {
                'names': ['my_busybox'],
                'image': 'busybox:latest',
                'command': ['/bin/sh'],
                'interactive': True
            },
            'ubuntu': {
                'names': ['my_ubuntu'],
                'image': 'ubuntu:latest',
                'command': ['/bin/bash'],
                'interactive': True
            },
            'alpine': {
                'names': ['my_alpine'],
                'image': 'alpine:latest',
                'command': ['/bin/sh'],
                'interactive': True
            },
            'nginx': {
                'names': ['my_nginx'],
                'image': 'nginx:latest',
                'ports': [
                    {
                        'container_port': 80,
                        'host_port': 8080
                    }
                ],
                'expose_ports': True
            },
            'plex': {
                'names': ['my_plex'],
                'expose_ports': True,
                'image': 'timhaak/plex',
                'environment': ['RUN_AS_ROOT=TRUE'],
                'volumes': [
                    {
                        'host_path': 'plex/library',
                        'container_path': '/config',
                        'readonly': False
                    },
                    {
                        'host_path': 'plex/media',
                        'container_path': '/data',
                        'readonly': False
                    }
                ],
                'ports': [
                    {
                        'host_port': 1900,
                        'container_port': 1900,
                        'protocol': 'UDP'
                    },
                    {
                        'host_port': 32469,
                        'container_port': 32469,
                        'protocol': 'UDP'
                    },
                    {
                        'host_port': 32400,
                        'container_port': 32400,
                        'protocol': 'UDP'
                    },
                    {
                        'host_port': 5353,
                        'container_port': 5353,
                        'protocol': 'UDP'
                    },
                    {
                        'host_port': 32400,
                        'container_port': 32400,
                        'protocol': 'TCP'
                    },
                    {
                        'host_port': 32469,
                        'container_port': 32469,
                        'protocol': 'TCP'
                    }
                ]
            },
            'mineos': {
                'names': [
                    'my_mineos'
                ],
                'ports': [
                    {
                        'container_port': 8443,
                        'host_port': 8443,
                        'protocol': 'TCP'
                    },
                    {
                        'container_port': 25565,
                        'host_port': 25565,
                        'protocol': 'TCP'
                    }
                ],
                'environment': [
                    'PASSWORD=freenas'
                ],
                'image': 'yujiod/minecraft-mineos',
                'expose_ports': True,
            },
            'debian': {
                'image': 'debian:latest',
                'interactive': True,
                'names': [
                    'my_debian'
                ]
            },
            'archlinux': {
                'image': 'base/archlinux',
                'command': [
                    '/bin/sh'
                ],
                'interactive': True,
                'names': [
                    'my_arch'
                ]
            }
        }


class DockerBaseTask(ProgressTask):
    def get_default_host(self):
        hostid = self.dispatcher.call_sync('docker.config.get_config').get('default_host')
        if not hostid:
            hostid = self.datastore.query(
                'vms',
                ('config.docker_host', '=', True),
                single=True,
                select='id'
            )
            if hostid:
                self.join_subtasks(self.run_subtask('docker.update', {'default_host': hostid}))
                return hostid

            host_name = 'docker_host_' + str(self.dispatcher.call_sync(
                'vm.query', [('name', '~', 'docker_host_')], {'count': True}
            ))

            biggest_volume = self.dispatcher.call_sync(
                'volume.query',
                [],
                {'sort': ['properties.size.parsed'], 'single': True, 'select': 'id'}
            )
            if not biggest_volume:
                raise TaskException(errno.ENOENT, 'No pools available. Docker host could not be created.')

            self.join_subtasks(self.run_subtask('vm.create', {
                'name': host_name,
                'template': {'name': 'boot2docker'},
                'target': biggest_volume
            }))

            hostid = self.dispatcher.call_sync(
                'vm.query',
                [('name', '=', host_name)],
                {'single': True, 'select': 'id'}
            )

            self.join_subtasks(self.run_subtask('vm.start', hostid))

        return hostid

    def check_host_state(self, hostid):
        host = self.dispatcher.call_sync(
            'docker.host.query',
            [('id', '=', hostid)],
            {'single': True},
            timeout=300
        )

        if host['state'] == 'DOWN':
            raise TaskException(errno.EHOSTDOWN, 'Docker host {0} is down'.format(host['name']))


@description('Updates Docker general configuration settings')
@accepts(h.ref('docker-config'))
class DockerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating Docker global configuration'

    def describe(self, container):
        return TaskDescription('Updating Docker global configuration')

    def verify(self, updated_params):
        return ['system']

    def run(self, updated_params):
        node = ConfigNode('container.docker', self.configstore)
        node.update(updated_params)
        state = node.__getstate__()
        if 'api_forwarding' in updated_params:
            try:
                if state['api_forwarding_enable']:
                    self.dispatcher.call_sync('containerd.docker.set_api_forwarding', state['api_forwarding'])
                else:
                    self.dispatcher.call_sync('containerd.docker.set_api_forwarding', None)
            except RpcException as err:
                self.add_warning(
                    TaskWarning(err.code, err.message)
                )


@description('Creates a Docker container')
@accepts(h.ref('docker-container'))
class DockerContainerCreateTask(DockerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Creating a Docker container'

    def describe(self, container):
        return TaskDescription('Creating Docker container {name}'.format(name=container['names'][0]))

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

        if not container.get('host'):
            container['host'] = self.get_default_host()

        self.check_host_state(container['host'])

        image = self.dispatcher.call_sync(
            'docker.image.query',
            [('host', '=', container['host']), ('image', '=', container['image'])],
            {'single': True}
        )

        if not image:
            self.join_subtasks(self.run_subtask('docker.image.pull', container['image'], container['host']))

        parent_dir = container.pop('parent_directory', None)
        if parent_dir:
            templates = self.dispatcher.call_sync('docker.image.get_templates')
            for k, t in templates.items():
                if t['image'] == container['image']:
                    container['volumes'] = copy.deepcopy(t.get('volumes', []))
                    for v in container['volumes']:
                        v['host_path'] = os.path.join(parent_dir, v['host_path'])
                        try:
                            os.makedirs(v['host_path'])
                        except FileExistsError:
                            pass
                        except OSError as err:
                            raise TaskException(
                                errno.EACCES,
                                'Parent directory {0} could not be created: {1}'.format(parent_dir, err)
                            )
                    break
            else:
                raise TaskException(
                    errno.EINVAL,
                    'Template for container {0} does not exist'.format(container['image'])
                )

        container['name'] = container['names'][0]
        self.dispatcher.call_sync('containerd.docker.create', container)


@description('Deletes a Docker container')
@accepts(str)
class DockerContainerDeleteTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Deleting a Docker container'

    def describe(self, id):
        return TaskDescription('Deleting Docker container {name}'.format(name=id))

    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.delete', id)


@description('Starts a Docker container')
@accepts(str)
class DockerContainerStartTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Starting container'

    def describe(self, id):
        return TaskDescription('Starting container {name}'.format(name=id))

    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.start', id)


@description('Stops a Docker container')
@accepts(str)
class DockerContainerStopTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Stopping container'

    def describe(self, id):
        return TaskDescription('Stopping container {name}'.format(name=id))

    def verify(self, id):
        return []

    def run(self, id):
        self.dispatcher.call_sync('containerd.docker.stop', id)


@description('Pulls a selected container image from Docker Hub and caches it on specified Docker host')
@accepts(str, h.one_of(str, None))
class DockerImagePullTask(DockerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Pulling docker image'

    def describe(self, name, hostid):
        return TaskDescription('Pulling docker image {name}'.format(name=name))

    def verify(self, name, hostid):
        return []

    def run(self, name, hostid):
        if not hostid:
            hostid = self.get_default_host()

        self.check_host_state(hostid)

        for i in self.dispatcher.call_sync('containerd.docker.pull', name, hostid, timeout=3600):
            if 'progressDetail' in i and 'current' in i['progressDetail']:
                percentage = i['progressDetail']['current'] / i['progressDetail']['total'] * 100
                self.set_progress(percentage, '{0} layer {1}'.format(i['status'], i['id']))


@description('Removes previously cached container image from a Docker host')
@accepts(str, h.one_of(str, None))
class DockerImageDeleteTask(DockerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Deleting docker image'

    def describe(self, name, hostid):
        return TaskDescription('Deleting docker image {name}'.format(name=name))

    def verify(self, name, hostid):
        return []

    def run(self, name, hostid):
        self.check_host_state(hostid)
        try:
            self.dispatcher.call_sync('containerd.docker.delete_image', name, hostid)
        except RpcException as err:
            raise TaskException(errno.EACCES, 'Failed to remove image {0}: {1}'.format(name, err))


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
                    default_host = dispatcher.call_sync('docker.config.get_config').get('default_host')
                    if not default_host:
                        dispatcher.call_task_sync('docker.update', {'default_host': host['id']})
                        logger.info('Docker host {0} set automatically as default Docker host'.format(host['name']))
                    dispatcher.dispatch_event('docker.host.changed', {
                        'operation': 'create',
                        'ids': [id]
                    })

        elif args['operation'] == 'delete':
            if dispatcher.call_sync('docker.config.get_config').get('default_host') in args['ids']:
                host = dispatcher.datastore.query(
                    'vms',
                    ('config.docker_host', '=', True),
                    single=True,
                )

                if host:
                    logger.info(
                        'Old default host deleted. Docker host {0} set automatically as default Docker host'.format(
                            host['name']
                        )
                    )
                    host_id = host['id']
                else:
                    logger.info(
                        'Old default host deleted. There are no Docker hosts left to take the role of default host.'
                    )
                    host_id = None

                dispatcher.call_task_sync('docker.update', {'default_host': host_id})

    def on_image_event(args):
        logger.trace('Received Docker image event: {0}'.format(args))
        if args['ids']:
            if args['operation'] == 'delete':
                images.remove_many(args['ids'])
            else:
                sync_cache(images, images_query, args['ids'])

    def on_container_event(args):
        logger.trace('Received Docker container event: {0}'.format(args))
        if args['ids']:
            if args['operation'] == 'delete':
                containers.remove_many(args['ids'])
            else:
                sync_cache(containers, containers_query, args['ids'])

    def sync_caches():
        interval = dispatcher.configstore.get('container.cache_refresh_interval')
        while True:
            gevent.sleep(interval)
            if images.ready and containers.ready:
                logger.trace('Syncing Docker caches')
                try:
                    sync_cache(images, images_query)
                    sync_cache(containers, containers_query)
                except RpcException:
                    pass

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

    plugin.register_provider('docker.config', DockerConfigProvider)
    plugin.register_provider('docker.host', DockerHostProvider)
    plugin.register_provider('docker.container', DockerContainerProvider)
    plugin.register_provider('docker.image', DockerImagesProvider)

    plugin.register_task_handler('docker.update', DockerUpdateTask)

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
            'default_host': {'type': ['string', 'null']},
            'api_forwarding': {'type': ['string', 'null']},
            'api_forwarding_enable': {'type': 'boolean'}
        }
    })

    plugin.register_schema_definition('docker-host', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'state': {'$ref': 'docker-host-status'},
            'status': {
                'oneOf': [{'$ref': 'docker-host-status'}, {'type': 'null'}]
            }
        }
    })

    plugin.register_schema_definition('docker-host-state', {
        'type': 'string',
        'enum': ['UP', 'DOWN']
    })

    plugin.register_schema_definition('docker-host-status', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'unique_id': {'type': 'string'},
            'hostname': {'type': 'string'},
            'os': {'type': 'string'},
            'mem_total': {'type': 'integer'}
        }
    })

    plugin.register_schema_definition('docker-container', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'names': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'command': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'image': {'type': 'string'},
            'host': {'type': ['string', 'null']},
            'hostname': {'type': ['string', 'null']},
            'status': {'type': ['string', 'null']},
            'memory_limit': {'type': ['integer', 'null']},
            'expose_ports': {'type': 'boolean'},
            'interactive': {'type': 'boolean'},
            'environment': {
                'type': 'array',
                'items': {'type': 'string'}
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
            },
            'parent_directory': {'type': 'string'}
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
            'host': {'type': ['string', 'null']},
            'created_at': {'type': 'string'}
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

    if not dispatcher.call_sync('docker.config.get_config').get('default_host'):
        host_id = dispatcher.datastore.query(
            'vms',
            ('config.docker_host', '=', True),
            single=True,
            select='id'
        )
        if host_id:
            dispatcher.call_task_sync('docker.update', {'default_host': host_id})

    gevent.spawn(sync_caches)
