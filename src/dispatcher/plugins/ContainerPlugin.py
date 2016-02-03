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

import ipaddress
import errno
import os
import random
import json
import tempfile
import hashlib
import gzip
import pygit2
import urllib.request
import urllib.parse
import urllib.error
import shutil
from task import Provider, Task, ProgressTask, VerifyException, TaskException, query, TaskWarning
from freenas.dispatcher.rpc import RpcException
from freenas.dispatcher.rpc import SchemaHelper as h, description, accepts, returns, private
from freenas.utils import first_or_default, normalize, deep_update
from utils import save_config, load_config, delete_config
from freenas.utils.query import wrap


VM_OUI = '00:a0:98'  # NetApp


class ContainerProvider(Provider):
    @query('container')
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

    def generate_mac(self):
        return VM_OUI + ':' + ':'.join('{0:02x}'.format(random.randint(0, 255)) for _ in range(0, 3))

    def get_default_interface(self):
        return self.configstore.get('container.default_nic')

    @description("Returns list of supported container types")
    def supported_types(self):
        result = ['JAIL', 'VM', 'DOCKER']

        return result


class VMTemplateProvider(Provider):
    @query('container')
    def query(self, filter=None, params=None):
        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm-templates')
        templates = []
        for root, dirs, files in os.walk(templates_dir):
            if 'template.json' in files:
                with open(os.path.join(root, 'template.json'), encoding='utf-8') as template:
                    try:
                        templates.append(json.loads(template.read()))
                    except ValueError:
                        pass

        return wrap(templates).query(*(filter or []), **(params or {}))


class ContainerBaseTask(Task):
    def init_dataset(self, container):
        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        container_ds = os.path.join(root_ds, container['name'])

        if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', root_ds)], {'single': True}):
            # Create VM root
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'pool': pool,
                'name': root_ds
            }))
        try:
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'pool': pool,
                'name': container_ds
            }))
        except RpcException:
            raise TaskException(
                errno.EACCES,
                'Dataset of the same name as {0} already exists. Maybe you meant to import an VM?'.format(container_ds)
            )

    def create_device(self, container, res):
        if res['type'] == 'DISK':
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            container_dir = self.dispatcher.call_sync('volume.get_dataset_path', container['target'], container_ds)
            ds_name = os.path.join(container_ds, res['name'])
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'pool': container['target'],
                'name': ds_name,
                'type': 'VOLUME',
                'volsize': res['properties']['size']
            }))

            if res['properties'].get('source'):
                source = res['properties']['source']
                self.join_subtasks(self.run_subtask(
                    'container.download_image',
                    source['url'],
                    source['sha256'],
                    container_dir,
                    os.path.join('/dev/zvol', ds_name)
                ))

        if res['type'] == 'NIC':
            normalize(res['properties'], {
                'link_address': self.dispatcher.call_sync('container.generate_mac')
            })

        if res['type'] == 'VOLUME':
            mgmt_net = ipaddress.ip_interface(self.configstore.get('container.network.management'))
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            ds_name = os.path.join(container_ds, res['name'])
            opts = {}

            if res['properties']['access_method'] == 'NFS':
                opts['sharenfs'] = {'value': '-network={0}'.format(str(mgmt_net.network))}
                if not self.configstore.get('service.nfs.enable'):
                    self.join_subtasks(self.run_subtask('service.configure', 'nfs', {'enable': True}))

            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'pool': container['target'],
                'name': ds_name,
                'properties': opts
            }))

    def update_device(self, container, old_res, new_res):
        if new_res['type'] == 'DISK':
            pass

    def delete_device(self, container, res):
        if res['type'] in ('DISK', 'VOLUME'):
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            ds_name = os.path.join(container_ds, res['name'])
            self.join_subtasks(self.run_subtask(
                'volume.dataset.delete',
                container['target'],
                ds_name,
                True
            ))


@accepts(h.ref('container'))
class ContainerCreateTask(ContainerBaseTask):
    def verify(self, container):
        if not self.dispatcher.call_sync('volume.query', [('name', '=', container['target'])], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(container['target']))

        return ['zpool:{0}'.format(container['target'])]

    def run(self, container):
        if container.get('template'):
            self.join_subtasks(self.run_subtask('vm_template.fetch'))
            template = self.dispatcher.call_sync(
                'vm_template.query',
                [('template.name', '=', container['template'].get('name'))],
                {'single': True}
            )

            if template is None:
                raise TaskException(errno.ENOENT, 'Template {0} not found'.format(container['template'].get('name')))

            deep_update(container, template)
        else:
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
            self.create_device(container, res)

        id = self.datastore.insert('containers', container)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'create',
            'ids': [id]
        })

        container = self.datastore.get_by_id('containers', id)
        save_config(
            self.dispatcher.call_sync(
                'volume.resolve_path',
                container['target'],
                os.path.join('vm', container['name'])
            ),
            'vm-{0}'.format(container['name']),
            container
        )

        return id


@accepts(str, str)
class ContainerImportTask(ContainerBaseTask):
    def verify(self, name, volume):
        if not self.dispatcher.call_sync('volume.query', [('name', '=', volume)], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(volume))

        return ['zpool:{0}'.format(volume)]

    def run(self, name, volume):
        try:
            container = load_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    volume,
                    os.path.join('vm', name)
                ),
                'vm-{0}'.format(name)
            )
        except FileNotFoundError:
            raise TaskException(errno.ENOENT, 'There is no {0} on {1} volume to be imported.'. format(name, volume))
        except ValueError:
            raise VerifyException(
                errno.EINVAL,
                'Cannot read configuration file. File is not a valid JSON file'
            )

        id = self.datastore.insert('containers', container)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'create',
            'ids': [id]
        })

        return id


@accepts(str, h.ref('container'))
class ContainerUpdateTask(ContainerBaseTask):
    def verify(self, id, updated_params):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        container = self.datastore.get_by_id('containers', id)
        try:
            delete_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    container['target'],
                    os.path.join('vm', container['name'])
                ),
                'vm-{0}'.format(container['name'])
            )
        except (RpcException, FileNotFoundError):
            pass

        if 'devices' in updated_params:
            for res in updated_params['devices']:
                existing = first_or_default(lambda i: i['name'] == res['name'], container['devices'])
                if existing:
                    self.update_device(container, existing, res)
                else:
                    self.create_device(container, res)

        container.update(updated_params)
        self.datastore.update('containers', id, container)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })

        container = self.datastore.get_by_id('containers', id)
        save_config(
            self.dispatcher.call_sync(
                'volume.resolve_path',
                container['target'],
                os.path.join('vm', container['name'])
            ),
            'vm-{0}'.format(container['name']),
            container
        )


@accepts(str)
class ContainerDeleteTask(Task):
    def verify(self, id):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id):
        container = self.datastore.get_by_id('containers', id)
        try:
            delete_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    container['target'],
                    os.path.join('vm', container['name'])
                ),
                'vm-{0}'.format(container['name'])
            )
        except (RpcException, FileNotFoundError):
            pass

        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        container_ds = os.path.join(root_ds, container['name'])

        try:
            self.join_subtasks(self.run_subtask('volume.dataset.delete', pool, container_ds, True))
        except RpcException as err:
            if err.code != errno.ENOENT:
                raise err

        self.datastore.delete('containers', id)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
class ContainerStartTask(Task):
    def verify(self, id):
        return ['system']

    def run(self, id):
        self.dispatcher.call_sync('containerd.management.start_container', id)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, bool)
class ContainerStopTask(Task):
    def verify(self, id, force=False):
        return ['system']

    def run(self, id, force=False):
        self.dispatcher.call_sync('containerd.management.stop_container', id, force)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, str)
class DownloadImageTask(ProgressTask):
    BLOCKSIZE = 65536

    def verify(self, url, sha256, vmdir, destination):
        return []

    def run(self, url, sha256, vmdir, destination):
        def progress_hook(nblocks, blocksize, totalsize):
            self.set_progress((nblocks * blocksize) / float(totalsize) * 100)

        self.set_progress(0, 'Downloading image')
        path, headers = urllib.request.urlretrieve(url, tempfile.mktemp(dir=vmdir), progress_hook)
        hasher = hashlib.sha256()

        self.set_progress(100, 'Verifying checksum')
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(self.BLOCKSIZE), b""):
                hasher.update(chunk)

        if hasher.hexdigest() != sha256:
            raise TaskException(errno.EINVAL, 'Invalid SHA256 checksum')

        self.set_progress(100, 'Copying image to virtual disk')
        with open(destination, 'wb') as dst:
            with gzip.open(path, 'rb') as src:
                for chunk in iter(lambda: src.read(self.BLOCKSIZE), b""):
                    dst.write(chunk)


class VMTemplateFetchTask(ProgressTask):
    def verify(self):
        return []

    def run(self):
        def clean_clone(url, path):
            try:
                shutil.rmtree(path)
            except shutil.Error:
                raise TaskException(errno.EACCES, 'Cannot remove templates directory: {0}'.format(path))
            except FileNotFoundError:
                pass
            try:
                pygit2.clone_repository(url, path)
            except pygit2.GitError:
                self.add_warning(TaskWarning(
                    errno.EACCES,
                    'Cannot update template cache. Result is outdated. Check networking.'))
                return

        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm-templates')

        progress = 0
        self.set_progress(progress, 'Downloading templates')
        template_sources = self.datastore.query('container.template_sources')

        if len(template_sources):
            progress_per_source = 100 / len(template_sources)
            for source in template_sources:
                if source.get('driver', '') == 'git':
                    source_path = os.path.join(templates_dir, source['id'])

                    if os.path.exists(os.path.join(source_path, '.git')):
                        try:
                            repo = pygit2.init_repository(source_path, False)

                            for remote in repo.remotes:
                                if remote.name == 'origin':
                                    remote.fetch()
                                    remote_master_id = repo.lookup_reference('refs/remotes/origin/master').target
                                    merge_result, _ = repo.merge_analysis(remote_master_id)

                                    if merge_result & pygit2.GIT_MERGE_ANALYSIS_UP_TO_DATE:
                                        continue
                                    elif merge_result & pygit2.GIT_MERGE_ANALYSIS_FASTFORWARD:
                                        repo.checkout_tree(repo.get(remote_master_id))
                                        master_ref = repo.lookup_reference('refs/heads/master')
                                        master_ref.set_target(remote_master_id)
                                        repo.head.set_target(remote_master_id)
                                    else:
                                        clean_clone(source['url'], source_path)
                        except pygit2.GitError:
                            self.add_warning(TaskWarning(
                                errno.EACCES,
                                'Cannot update template cache. Result is outdated. Check networking.'))
                            return
                    else:
                        clean_clone(source['url'], source_path)

                self.set_progress(progress + progress_per_source, 'Finished operation for {0}'.format(source['id']))

        self.set_progress(100, 'Templates downloaded')


def _init(dispatcher, plugin):
    plugin.register_schema_definition('container', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'target': {'type': 'string'},
            'template': {
                'type': ['object', 'none'],
                'properties': {
                    'name': {'type': 'string'}
                }
            },
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
                    'boot_directory': {'type': ['string', 'null']},
                    'cloud_init': {'type': ['string', 'null']},
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
                'enum': ['BRIDGED', 'NAT', 'HOSTONLY', 'MANAGEMENT']
            },
            'link_address': {'type': 'string'},
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

    def volume_pre_detach(args):
        for vm in dispatcher.call_sync('container.query'):
            if vm['target'] == args['name']:
                dispatcher.call_task_sync('container.stop', vm['id'])
        return True

    def volume_pre_destroy(args):
        for vm in dispatcher.call_sync('container.query'):
            if vm['target'] == args['name']:
                dispatcher.call_task_sync('container.delete', vm['id'])
        return True

    plugin.register_provider('container', ContainerProvider)
    plugin.register_task_handler('container.create', ContainerCreateTask)
    plugin.register_task_handler('container.import', ContainerImportTask)
    plugin.register_task_handler('container.update', ContainerUpdateTask)
    plugin.register_task_handler('container.delete', ContainerDeleteTask)
    plugin.register_task_handler('container.start', ContainerStartTask)
    plugin.register_task_handler('container.stop', ContainerStopTask)
    plugin.register_task_handler('container.download_image', DownloadImageTask)

    plugin.register_provider('vm_template', VMTemplateProvider)
    plugin.register_task_handler('vm_template.fetch', VMTemplateFetchTask)

    plugin.attach_hook('volume.pre_destroy', volume_pre_destroy)
    plugin.attach_hook('volume.pre_detach', volume_pre_detach)

    plugin.register_event_type('container.changed')
