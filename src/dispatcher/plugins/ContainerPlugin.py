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
import hashlib
import gzip
import pygit2
import urllib.request
import urllib.parse
import urllib.error
import shutil
from task import Provider, Task, ProgressTask, VerifyException, TaskException, query, TaskWarning, TaskDescription
from freenas.dispatcher.rpc import RpcException
from freenas.dispatcher.rpc import SchemaHelper as h, description, accepts
from freenas.utils import first_or_default, normalize, deep_update, process_template
from utils import save_config, load_config, delete_config
from freenas.utils.query import wrap


VM_OUI = '00:a0:98'  # NetApp


BLOCKSIZE = 65536


@description('Provides information about containers')
class ContainerProvider(Provider):
    @query('container')
    def query(self, filter=None, params=None):
        def extend(obj):
            obj['status'] = self.dispatcher.call_sync('containerd.management.get_status', obj['id'])
            return obj

        return self.datastore.query('containers', *(filter or []), callback=extend, **(params or {}))

    def get_container_root(self, container_id):
        container = self.datastore.get_by_id('containers', container_id)
        if not container:
            return None

        pass # XXX

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

    def get_volume_path(self, container_id, volume_name):
        container = self.datastore.get_by_id('containers', container_id)
        if not container:
            return None

        vol = first_or_default(lambda v: v['name'] == volume_name, container['devices'])
        if not vol:
            return None

        if vol['properties']['auto']:
            return os.path.join(self.get_container_root(container_id), vol['name'])

        return vol['properties']['destination']

    @description('Returns container if provided dataset is its root')
    def get_dependent(self, dataset, extend=True):
        path_parts = dataset.split('/')
        if len(path_parts) != 3:
            return None
        if path_parts[1] != 'vm':
            return None

        arguments = (
            [('name', '=', path_parts[2]), ('target', '=', path_parts[0])],
            {'single': True}
        )

        if extend:
            return self.dispatcher.call_sync('container.query', *arguments)
        else:
            return self.datastore.query('containers', *arguments)

    def generate_mac(self):
        return VM_OUI + ':' + ':'.join('{0:02x}'.format(random.randint(0, 255)) for _ in range(0, 3))

    def get_default_interface(self):
        return self.configstore.get('container.default_nic')

    @description("Returns list of supported container types")
    def supported_types(self):
        result = ['JAIL', 'VM', 'DOCKER']

        return result


@description('Provides information about container templates')
class ContainerTemplateProvider(Provider):
    @query('container')
    def query(self, filter=None, params=None):
        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_templates')
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_image_cache')
        templates = []
        for root, dirs, files in os.walk(templates_dir):
            if 'template.json' in files:
                with open(os.path.join(root, 'template.json'), encoding='utf-8') as template_file:
                    try:
                        template = json.loads(template_file.read())
                        template['template']['path'] = root
                        template['template']['cached'] = False
                        if os.path.isdir(os.path.join(cache_dir, template['template']['name'])):
                            template['template']['cached'] = True

                        templates.append(template)
                    except ValueError:
                        pass

        return wrap(templates).query(*(filter or []), **(params or {}))


class ContainerBaseTask(Task):
    def init_dataset(self, container):
        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        self.container_ds = os.path.join(root_ds, container['name'])

        if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', root_ds)], {'single': True}):
            # Create VM root
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': pool,
                'id': root_ds
            }))
        try:
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': pool,
                'id': self.container_ds
            }))
        except RpcException:
            raise TaskException(
                errno.EACCES,
                'Dataset of the same name as {0} already exists. Maybe you meant to import an VM?'.format(self.container_ds)
            )

    def init_files(self, container):
        template = container.get('template')
        if not template:
            return

        if template.get('files'):
            source_root = os.path.join(template['path'], 'files')
            dest_root = self.dispatcher.call_sync('volume.get_dataset_path', self.container_ds)
            files_root = os.path.join(dest_root, 'files')

            os.mkdir(files_root)

            for root, dirs, files in os.walk(source_root):
                r = os.path.relpath(root, source_root)

                for f in files:
                    name, ext = os.path.splitext(f)
                    if ext == '.in':
                        process_template(os.path.join(root, f), os.path.join(files_root, r, name), **{
                            'VM_ROOT': files_root
                        })
                    else:
                        shutil.copy(os.path.join(root, f), os.path.join(files_root, r, f))

                for d in dirs:
                    os.mkdir(os.path.join(files_root, r, d))

            if template.get('fetch'):
                for f in template['fetch']:
                    urllib.request.urlretrieve(f['url'], os.path.join(files_root, f['dest']))

    def create_device(self, container, res):
        if res['type'] == 'DISK':
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            ds_name = os.path.join(container_ds, res['name'])
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': container['target'],
                'id': ds_name,
                'type': 'VOLUME',
                'volsize': res['properties']['size']
            }))

            normalize(res['properties'], {
                'mode': 'AHCI'
            })

            if res['properties'].get('source'):
                self.join_subtasks(self.run_subtask(
                    'container.image.install',
                    container['template']['name'],
                    res['name'],
                    os.path.join('/dev/zvol', ds_name)
                ))

        if res['type'] == 'NIC':
            normalize(res['properties'], {
                'link_address': self.dispatcher.call_sync('container.generate_mac')
            })

        if res['type'] == 'VOLUME':
            properties = res['properties']
            mgmt_net = ipaddress.ip_interface(self.configstore.get('container.network.management'))
            container_ds = os.path.join(container['target'], 'vm', container['name'])
            opts = {}

            if properties['type'] == 'NFS':
                opts['sharenfs'] = {'value': '-network={0}'.format(str(mgmt_net.network))}
                if not self.configstore.get('service.nfs.enable'):
                    self.join_subtasks(self.run_subtask('service.update', 'nfs', {'enable': True}))

            if properties['type'] == 'VT9P':
                if properties.get('auto'):
                    self.join_subtasks(self.run_subtask('volume.dataset.create', {
                        'volume': container['target'],
                        'id': os.path.join(container_ds, res['name'])
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
                ds_name
            ))


@accepts(h.ref('container'))
@description('Creates a container')
class ContainerCreateTask(ContainerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Creating container'

    def describe(self, container):
        return TaskDescription('Creating container {name}', name=container.get('name', ''))

    def verify(self, container):
        if not self.dispatcher.call_sync('volume.query', [('id', '=', container['target'])], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(container['target']))

        return ['zpool:{0}'.format(container['target'])]

    def run(self, container):
        if container.get('template'):
            self.join_subtasks(self.run_subtask('container.template.fetch'))
            template = self.dispatcher.call_sync(
                'container.template.query',
                [('template.name', '=', container['template'].get('name'))],
                {'single': True}
            )

            if template is None:
                raise TaskException(errno.ENOENT, 'Template {0} not found'.format(container['template'].get('name')))

            template['config']['memsize'] = template['config']['memsize'] * 1024 * 1024

            result = {}
            for key in container:
                if container[key]:
                    result[key] = container[key]
            deep_update(template, result)
            container = template
        else:
            normalize(container, {
                'config': {},
                'devices': []
            })

        normalize(container, {
            'enabled': True,
            'immutable': False
        })

        normalize(container['config'], {
            'memsize': 512*1024*1024,
            'ncpus': 1
        })

        container['config']['memsize'] = int(container['config']['memsize'] / (1024 * 1024))

        self.join_subtasks(self.run_subtask('container.cache.update', container['template']['name']))

        self.init_dataset(container)
        self.init_files(container)
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
@description('Imports existing container')
class ContainerImportTask(ContainerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Importing container'

    def describe(self, name, volume):
        return TaskDescription('Importing container {name}', name=name)

    def verify(self, name, volume):
        if not self.dispatcher.call_sync('volume.query', [('id', '=', volume)], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(volume))

        if self.datastore.exists('containers', ('name', '=', name)):
            raise VerifyException(errno.EEXIST, 'Container {0} already exists'.format(name))

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


@accepts(str, bool)
@description('Sets container immutable')
class ContainerSetImmutableTask(ContainerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Updating container\'s immutable property'

    def describe(self, id, immutable):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription(
            'Setting {name} container\'s immutable property to {value}',
            name=container.get('name', '') if container else '',
            value='on' if immutable else 'off'
        )

    def verify(self, id, immutable):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} does not exist'.format(id))

        return ['system']

    def run(self, id, immutable):
        container = self.datastore.get_by_id('containers', id)
        container['enabled'] = not immutable
        container['immutable'] = immutable
        self.datastore.update('containers', id, container)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, h.ref('container'))
@description('Updates container')
class ContainerUpdateTask(ContainerBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Updating container'

    def describe(self, id, updated_params):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription('Updating container {name}', name=container.get('name', '') if container else '')

    def verify(self, id, updated_params):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        container = self.datastore.get_by_id('containers', id)
        if container['immutable']:
            raise TaskException(errno.EACCES, 'Cannot modify immutable container {0}.'.format(id))
        try:
            delete_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    container['target'],
                    os.path.join('vm', container['name'])
                ),
                'vm-{0}'.format(container['name'])
            )
        except (RpcException, OSError):
            pass

        if 'devices' in updated_params:
            self.join_subtasks(self.run_subtask('container.cache.update', container['template']['name']))
            for res in updated_params['devices']:
                existing = first_or_default(lambda i: i['name'] == res['name'], container['devices'])
                if existing:
                    self.update_device(container, existing, res)
                else:
                    self.create_device(container, res)

        if not updated_params.get('enabled', True):
            self.join_subtasks(self.run_subtask('container.stop', id))

        config = updated_params.get('config')
        if config:
            if 'memsize' in config:
                updated_params['config']['memsize'] = int(config.get('memsize') / (1024 * 1024))

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
@description('Deletes container')
class ContainerDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting container'

    def describe(self, id):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription('Deleting container {name}', name=container.get('name', '') if container else '')

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
        except (RpcException, OSError):
            pass

        pool = container['target']
        root_ds = os.path.join(pool, 'vm')
        container_ds = os.path.join(root_ds, container['name'])

        try:
            self.join_subtasks(self.run_subtask('volume.dataset.delete', container_ds))
        except RpcException as err:
            if err.code != errno.ENOENT:
                raise err

        self.datastore.delete('containers', id)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description('Exports container from database')
class ContainerExportTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Exporting container'

    def describe(self, id):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription('Exporting container {name}', name=container.get('name', '') if container else '')

    def verify(self, id):
        if not self.datastore.exists('containers', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Container {0} not found'.format(id))

        return ['system']

    def run(self, id):
        self.datastore.delete('containers', id)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description('Starts container')
class ContainerStartTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Starting container'

    def describe(self, id):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription('Starting container {name}', name=container.get('name', '') if container else '')

    def verify(self, id):
        container = self.dispatcher.call_sync('container.query', [('id', '=', id)], {'single': True})
        if not container['enabled']:
            raise VerifyException(errno.EACCES, "Cannot start disabled container {0}".format(id))
        return ['system']

    def run(self, id):
        self.dispatcher.call_sync('containerd.management.start_container', id)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, bool)
@description('Stops container')
class ContainerStopTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Stopping container'

    def describe(self, id, force=False):
        container = self.datastore.get_by_id('containers', id)
        return TaskDescription('Stopping container {name}', name=container.get('name', '') if container else '')

    def verify(self, id, force=False):
        return ['system']

    def run(self, id, force=False):
        self.dispatcher.call_sync('containerd.management.stop_container', id, force)
        self.dispatcher.dispatch_event('container.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str)
@description('Caches container images')
class CacheImagesTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Caching container images'

    def describe(self, name):
        return TaskDescription('Caching container images {name}', name=name or '')

    def verify(self, name):
        return ['system']

    def run(self, name):
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_image_cache')
        template = self.dispatcher.call_sync(
            'container.template.query',
            [('template.name', '=', name)],
            {'single': True}
        )
        if not template:
            raise TaskException(errno.ENOENT, 'Template of container {0} does not exist'.format(name))

        self.set_progress(0, 'Caching images')
        res_cnt = len(template['devices'])

        for idx, res in enumerate(template['devices']):
            self.set_progress(int(idx / res_cnt), 'Caching images')
            if res['type'] == 'DISK':
                res_name = res['name']
                if res['properties'].get('source'):
                    source = res['properties']['source']
                    url = source['url']
                    sha256 = source['sha256']

                    destination = os.path.join(cache_dir, name, res_name)
                    sha256_path = os.path.join(destination, 'sha256')
                    if os.path.isdir(destination):
                        if os.path.exists(sha256_path):
                            with open(sha256_path) as sha256_file:
                                if sha256_file.read() == sha256:
                                    continue
                    else:
                        os.makedirs(destination)

                    self.join_subtasks(self.run_subtask(
                        'container.image.download',
                        url,
                        sha256,
                        destination
                    ))

        self.set_progress(100, 'Cached images')


@accepts(str)
@description('Deletes cached container images')
class DeleteImagesTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting cached container images'

    def describe(self, name):
        return TaskDescription('Deleting cached container {name} images', name=name)

    def verify(self, name):
        return ['system']

    def run(self, name):
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_image_cache')
        images_dir = os.path.join(cache_dir, name)
        shutil.rmtree(images_dir)


@accepts(str, str, str)
@description('Downloads container image')
class DownloadImageTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Downloading container image'

    def describe(self, url, sha256, destination):
        return TaskDescription('Downloading container image {name}', name=url or '')

    def verify(self, url, sha256, destination):
        return ['system']

    def run(self, url, sha256, destination):
        def progress_hook(nblocks, blocksize, totalsize):
            self.set_progress((nblocks * blocksize) / float(totalsize) * 100)

        image_path = os.path.join(destination, 'img.gz')
        sha256_path = os.path.join(destination, 'sha256')

        self.set_progress(0, 'Downloading image')
        urllib.request.urlretrieve(url, image_path, progress_hook)
        hasher = hashlib.sha256()

        self.set_progress(100, 'Verifying checksum')
        with open(image_path, 'rb') as f:
            for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
                hasher.update(chunk)

        if hasher.hexdigest() != sha256:
            raise TaskException(errno.EINVAL, 'Invalid SHA256 checksum')

        with open(sha256_path, 'w') as sha256_file:
            sha256_file.write(sha256)


@accepts(str, str, str)
@description('Installs container image')
class InstallImageTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Installing container image'

    def describe(self, name, res, destination):
        return TaskDescription(
            'Installing container image {name} in {destination}',
            name=os.path.join(name, res) or '',
            destination=destination or ''
        )

    def verify(self, name, res, destination):
        return ['system']

    def run(self, name, res, destination):
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_image_cache')
        image_path = os.path.join(cache_dir, name, res, 'img.gz')

        with open(destination, 'wb') as dst:
            with gzip.open(image_path, 'rb') as src:
                for chunk in iter(lambda: src.read(BLOCKSIZE), b""):
                    dst.write(chunk)


@description('Downloads container templates')
class ContainerTemplateFetchTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Downloading container templates'

    def describe(self):
        return TaskDescription('Downloading container templates')

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

        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'container_templates')

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


def _depends():
    return ['VolumePlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('container', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'enabled': {'type': 'boolean'},
            'immutable': {'type': 'boolean'},
            'target': {'type': 'string'},
            'template': {
                'type': ['object', 'null'],
                'properties': {
                    'name': {'type': 'string'},
                    'path': {'type': 'string'},
                    'cached': {'type': 'boolean'}
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
                'enum': ['DISK', 'CDROM', 'NIC', 'VOLUME']
            },
            'properties': {'type': 'object'}
        },
        'required': ['name', 'type', 'properties']
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

    plugin.register_schema_definition('container-device-volume', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {
                'type': 'string',
                'enum': ['VT9P', 'NFS']
            },
            'auto': {'type': ['boolean', 'null']},
            'destination': {'type': ['string', 'null']}
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
    plugin.register_provider('container.template', ContainerTemplateProvider)
    plugin.register_task_handler('container.create', ContainerCreateTask)
    plugin.register_task_handler('container.import', ContainerImportTask)
    plugin.register_task_handler('container.update', ContainerUpdateTask)
    plugin.register_task_handler('container.delete', ContainerDeleteTask)
    plugin.register_task_handler('container.export', ContainerExportTask)
    plugin.register_task_handler('container.start', ContainerStartTask)
    plugin.register_task_handler('container.stop', ContainerStopTask)
    plugin.register_task_handler('container.immutable.set', ContainerSetImmutableTask)
    plugin.register_task_handler('container.image.install', InstallImageTask)
    plugin.register_task_handler('container.image.download', DownloadImageTask)
    plugin.register_task_handler('container.cache.update', CacheImagesTask)
    plugin.register_task_handler('container.cache.delete', DeleteImagesTask)
    plugin.register_task_handler('container.template.fetch', ContainerTemplateFetchTask)

    plugin.attach_hook('volume.pre_destroy', volume_pre_destroy)
    plugin.attach_hook('volume.pre_detach', volume_pre_detach)

    plugin.register_event_type('container.changed')
