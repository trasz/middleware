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
import logging
from task import Provider, Task, ProgressTask, VerifyException, TaskException, query, TaskWarning, TaskDescription
from freenas.dispatcher.rpc import RpcException
from freenas.dispatcher.rpc import SchemaHelper as h, description, accepts
from freenas.utils import first_or_default, normalize, deep_update, process_template, in_directory
from utils import save_config, load_config, delete_config
from freenas.utils.decorators import throttle
from freenas.utils.query import wrap


VM_OUI = '00:a0:98'  # NetApp
BLOCKSIZE = 65536


logger = logging.getLogger(__name__)


@description('Provides information about VMs')
class VMProvider(Provider):
    @query('vm')
    def query(self, filter=None, params=None):
        def extend(obj):
            obj['status'] = self.dispatcher.call_sync('containerd.management.get_status', obj['id'])
            root = self.dispatcher.call_sync('vm.get_vm_root', obj['id'])
            if os.path.isdir(root):
                readme = get_readme(root)
                if readme:
                    with open(readme, 'r') as readme_file:
                        obj['template']['readme'] = readme_file.read()
            return obj

        return self.datastore.query('vms', *(filter or []), callback=extend, **(params or {}))

    def get_vm_root(self, vm_id):
        vm = self.datastore.get_by_id('vms', vm_id)
        if not vm:
            return None

        return os.path.join('/mnt', vm['target'], 'vm', vm['name'])

    def get_disk_path(self, vm_id, disk_name):
        vm = self.datastore.get_by_id('vms', vm_id)
        if not vm:
            return None

        disk = first_or_default(lambda d: d['name'] == disk_name, vm['devices'])
        if not disk:
            return None

        if disk['type'] == 'DISK':
            return os.path.join('/dev/zvol', vm['target'], 'vm', vm['name'], disk_name)

        if disk['type'] == 'CDROM':
            return disk['properties']['path']

    def get_volume_path(self, vm_id, volume_name):
        vm = self.datastore.get_by_id('vms', vm_id)
        if not vm:
            return None

        vol = first_or_default(lambda v: v['name'] == volume_name, vm['devices'])
        if not vol:
            return None

        if vol['properties'].get('auto'):
            return os.path.join(self.get_vm_root(vm_id), vol['name'])

        return vol['properties']['destination']

    @description("Get VMs dependent on provided filesystem path")
    def get_dependencies(self, path, enabled_only=True, recursive=True):
        result = []

        if enabled_only:
            vms = self.datastore.query('vms', ('enabled', '=', True))
        else:
            vms = self.datastore.query('vms')

        for i in vms:
            target_path = self.get_vm_root(i['id'])
            if recursive:
                if in_directory(target_path, path):
                    result.append(i)
            else:
                if target_path == path:
                    result.append(i)

        return result

    def generate_mac(self):
        return VM_OUI + ':' + ':'.join('{0:02x}'.format(random.randint(0, 255)) for _ in range(0, 3))


@description('Provides information about VM snapshots')
class VMSnapshotProvider(Provider):
    @query('vm')
    def query(self, filter=None, params=None):
        return self.datastore.query('vm.snapshots', *(filter or []), **(params or {}))


@description('Provides information about VM templates')
class VMTemplateProvider(Provider):
    @query('vm')
    def query(self, filter=None, params=None):
        fetch_lock = self.dispatcher.get_lock('vm_templates')
        try:
            fetch_lock.acquire(1)
            fetch_templates(self.dispatcher)
        except RpcException:
            pass
        finally:
            fetch_lock.release()

        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_templates')
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_image_cache')
        templates = []
        for root, dirs, files in os.walk(templates_dir):
            if 'template.json' in files:
                with open(os.path.join(root, 'template.json'), encoding='utf-8') as template_file:
                    try:
                        template = json.loads(template_file.read())
                        readme = get_readme(root)
                        if readme:
                            with open(readme, 'r') as readme_file:
                                template['template']['readme'] = readme_file.read()
                        template['template']['path'] = root
                        template['template']['cached'] = False
                        if os.path.isdir(os.path.join(cache_dir, template['template']['name'])):
                            template['template']['cached'] = True

                        templates.append(template)
                    except ValueError:
                        pass

        return wrap(templates).query(*(filter or []), **(params or {}))


class VMBaseTask(ProgressTask):
    def init_dataset(self, vm):
        pool = vm['target']
        root_ds = os.path.join(pool, 'vm')
        self.vm_ds = os.path.join(root_ds, vm['name'])

        if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', root_ds)], {'single': True}):
            # Create VM root
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': pool,
                'id': root_ds
            }))
        try:
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': pool,
                'id': self.vm_ds
            }))
        except RpcException:
            raise TaskException(
                errno.EACCES,
                'Dataset of the same name as {0} already exists. Maybe you meant to import an VM?'.format(self.vm_ds)
            )

    def init_files(self, vm, progress_cb=None):
        if progress_cb:
            progress_cb(0, 'Initializing VM files')
        template = vm.get('template')
        if not template:
            return

        if template.get('files'):
            source_root = os.path.join(template['path'], 'files')
            dest_root = self.dispatcher.call_sync('volume.get_dataset_path', self.vm_ds)
            files_root = os.path.join(dest_root, 'files')

            try:
                shutil.copyfile(os.path.join(template['path'], 'README.md'), dest_root)
            except OSError:
                pass

            os.mkdir(files_root)

            for root, dirs, files in os.walk(source_root):
                r = os.path.relpath(root, source_root)

                for f in files:
                    name, ext = os.path.splitext(f)
                    if ext == '.in':
                        process_template(os.path.join(root, f), os.path.join(files_root, r, name), **{
                            'VM_ROOT': dest_root
                        })
                    else:
                        shutil.copy(os.path.join(root, f), os.path.join(files_root, r, f))

                for d in dirs:
                    os.mkdir(os.path.join(files_root, r, d))

            if template.get('fetch'):
                for f in template['fetch']:
                    if f.get('dest'):
                        self.join_subtasks(self.run_subtask(
                            'vm.file.install',
                            vm['template']['name'],
                            f['name'],
                            files_root if f['dest'] == '.' else os.path.join(files_root, f['dest']),
                            progress_callback=progress_cb
                        ))

        if progress_cb:
            progress_cb(100, 'Initializing VM files')

    def create_device(self, vm, res, progress_cb=None):
        if progress_cb:
            progress_cb(0, 'Creating {0}'.format(res['type'].lower()))

        if res['type'] == 'DISK':
            vm_ds = os.path.join(vm['target'], 'vm', vm['name'])
            ds_name = os.path.join(vm_ds, res['name'])
            self.join_subtasks(self.run_subtask('volume.dataset.create', {
                'volume': vm['target'],
                'id': ds_name,
                'type': 'VOLUME',
                'volsize': res['properties']['size']
            }))

            normalize(res['properties'], {
                'mode': 'AHCI'
            })

            if res['properties'].get('source'):
                self.join_subtasks(self.run_subtask(
                    'vm.file.install',
                    vm['template']['name'],
                    res['properties']['source'],
                    os.path.join('/dev/zvol', ds_name),
                    progress_callback=progress_cb
                ))

        if res['type'] == 'NIC':
            normalize(res['properties'], {
                'device': 'VIRTIO',
                'mode': 'NAT',
                'link_address': self.dispatcher.call_sync('vm.generate_mac')
            })

            if res['properties'].get('type') == 'BRIDGE' and res['properties'].get('bridge') == 'default':
                res['properties']['bridge'] = self.dispatcher.call_sync('networkd.configuration.get_default_interface')

        if res['type'] == 'VOLUME':
            properties = res['properties']
            mgmt_net = ipaddress.ip_interface(self.configstore.get('container.network.management'))
            vm_ds = os.path.join(vm['target'], 'vm', vm['name'])
            opts = {}

            normalize(res['properties'], {
                'type': 'VT9P',
                'auto': False
            })

            if properties['type'] == 'NFS':
                opts['sharenfs'] = {'value': '-network={0}'.format(str(mgmt_net.network))}
                if not self.configstore.get('service.nfs.enable'):
                    self.join_subtasks(self.run_subtask('service.update', 'nfs', {'enable': True}))

            if properties['type'] == 'VT9P':
                if properties.get('auto'):
                    ds_name = os.path.join(vm_ds, res['name'])
                    self.join_subtasks(self.run_subtask('volume.dataset.create', {
                        'volume': vm['target'],
                        'id': ds_name
                    }))

                    if properties.get('source'):
                        self.join_subtasks(self.run_subtask(
                            'vm.file.install',
                            vm['template']['name'],
                            properties['source'],
                            self.dispatcher.call_sync('volume.get_dataset_path', ds_name),
                            progress_callback=progress_cb
                        ))

        if progress_cb:
            progress_cb(100, 'Creating {0}'.format(res['type'].lower()))

    def update_device(self, vm, old_res, new_res):
        if new_res['type'] == 'DISK':
            pass

    def delete_device(self, vm, res):
        if res['type'] in ('DISK', 'VOLUME'):
            vm_ds = os.path.join(vm['target'], 'vm', vm['name'])
            ds_name = os.path.join(vm_ds, res['name'])
            self.join_subtasks(self.run_subtask(
                'volume.dataset.delete',
                ds_name
            ))


@accepts(h.ref('vm'))
@description('Creates a VM')
class VMCreateTask(VMBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Creating VM'

    def describe(self, vm):
        return TaskDescription('Creating VM {name}', name=vm.get('name', ''))

    def verify(self, vm):
        if not self.dispatcher.call_sync('volume.query', [('id', '=', vm['target'])], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(vm['target']))

        return ['zpool:{0}'.format(vm['target'])]

    def run(self, vm):
        def collect_download_progress(percentage, message=None, extra=None):
            split_message = message.split(' ')
            self.set_progress(
                percentage / 4,
                'Downloading VM files: {0} {1}'.format(split_message[-2], split_message[-1]),
                extra
            )

        def collect_progress(static_message, start_percentage, percentage, message=None, extra=None):
            self.set_progress(
                start_percentage + ((30 * (idx / devices_len)) + (0.3 * (percentage / devices_len))),
                '{0} {1}'.format(static_message, message),
                extra
            )

        self.set_progress(0, 'Creating VM')
        if vm.get('template'):
            template = self.dispatcher.call_sync(
                'vm.template.query',
                [('template.name', '=', vm['template'].get('name'))],
                {'single': True}
            )

            if template is None:
                raise TaskException(errno.ENOENT, 'Template {0} not found'.format(vm['template'].get('name')))

            template['template'].pop('readme')

            result = {}
            for key in vm:
                if vm[key]:
                    result[key] = vm[key]
            deep_update(template, result)
            vm = template

            self.join_subtasks(self.run_subtask(
                'vm.cache.update',
                vm['template']['name'],
                progress_callback=collect_download_progress
            ))
        else:
            normalize(vm, {
                'config': {},
                'devices': []
            })

        normalize(vm, {
            'enabled': True,
            'immutable': False,
            'parent': None
        })

        normalize(vm['config'], {
            'memsize': 512,
            'ncpus': 1,
            'autostart': False
        })

        if vm['config']['ncpus'] > 16:
            raise TaskException(errno.EINVAL, 'Upper limit of VM cores exceeded. Maximum permissible value is 16.')

        self.set_progress(25, 'Initializing VM dataset')
        self.init_dataset(vm)
        devices_len = len(vm['devices'])
        for idx, res in enumerate(vm['devices']):
            self.create_device(vm, res, lambda p, m, e=None: collect_progress('Initializing VM devices:', 30, p, m, e))

        self.init_files(vm, lambda p, m, e=None: collect_progress('Initializing VM files:', 60, p, m, e))

        id = self.datastore.insert('vms', vm)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'create',
            'ids': [id]
        })

        vm = self.datastore.get_by_id('vms', id)
        self.set_progress(90, 'Saving VM configuration')
        save_config(
            self.dispatcher.call_sync(
                'volume.resolve_path',
                vm['target'],
                os.path.join('vm', vm['name'])
            ),
            'vm-{0}'.format(vm['name']),
            vm
        )
        self.set_progress(100, 'Finished')

        return id


@accepts(str, str)
@description('Imports existing VM')
class VMImportTask(VMBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Importing VM'

    def describe(self, name, volume):
        return TaskDescription('Importing VM {name}', name=name)

    def verify(self, name, volume):
        if not self.dispatcher.call_sync('volume.query', [('id', '=', volume)], {'single': True}):
            raise VerifyException(errno.ENXIO, 'Volume {0} doesn\'t exist'.format(volume))

        if self.datastore.exists('vms', ('name', '=', name)):
            raise VerifyException(errno.EEXIST, 'VM {0} already exists'.format(name))

        return ['zpool:{0}'.format(volume)]

    def run(self, name, volume):
        try:
            vm = load_config(
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

        id = self.datastore.insert('vms', vm)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'create',
            'ids': [id]
        })

        return id


@accepts(str, bool)
@description('Sets VM immutable')
class VMSetImmutableTask(VMBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Updating VM\'s immutable property'

    def describe(self, id, immutable):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription(
            'Setting {name} VM\'s immutable property to {value}',
            name=vm.get('name', '') if vm else '',
            value='on' if immutable else 'off'
        )

    def verify(self, id, immutable):
        if not self.datastore.exists('vms', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'VM {0} does not exist'.format(id))

        return ['system']

    def run(self, id, immutable):
        vm = self.datastore.get_by_id('vms', id)
        vm['enabled'] = not immutable
        vm['immutable'] = immutable
        self.datastore.update('vms', id, vm)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, h.ref('vm'))
@description('Updates VM')
class VMUpdateTask(VMBaseTask):
    @classmethod
    def early_describe(cls):
        return 'Updating VM'

    def describe(self, id, updated_params):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Updating VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id, updated_params):
        if not self.datastore.exists('vms', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'VM {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        if 'config' in updated_params and updated_params['config']['ncpus'] > 16:
            raise TaskException(errno.EINVAL, 'Upper limit of VM cores exceeded. Maximum permissible value is 16.')

        state = self.dispatcher.call_sync('vm.query', [('id', '=', id)], {'select': 'status.state', 'single': True})
        if 'name' in updated_params and state != 'STOPPED':
            raise TaskException(errno.EACCES, 'Name of a running VM cannot be modified')

        vm = self.datastore.get_by_id('vms', id)
        if vm['immutable']:
            raise TaskException(errno.EACCES, 'Cannot modify immutable VM {0}.'.format(id))
        try:
            delete_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    vm['target'],
                    os.path.join('vm', vm['name'])
                ),
                'vm-{0}'.format(vm['name'])
            )
        except (RpcException, OSError):
            pass

        if 'name' in updated_params:
            root_ds = os.path.join(vm['target'], 'vm')
            vm_ds = os.path.join(root_ds, vm['name'])
            new_vm_ds = os.path.join(root_ds, updated_params['name'])

            self.join_subtasks(self.run_subtask('zfs.rename', vm_ds, new_vm_ds))

        if 'template' in updated_params:
            readme = updated_params['template'].pop('readme')
            if readme:
                root = self.dispatcher.call_sync('vm.get_vm_root', vm['id'])
                with open(os.path.join(root, 'README.md'), 'w') as readme_file:
                    readme_file.write(readme)

        if 'devices' in updated_params:
            if vm.get('template'):
                self.join_subtasks(self.run_subtask('vm.cache.update', vm['template']['name']))

            for res in updated_params['devices']:
                existing = first_or_default(lambda i: i['name'] == res['name'], vm['devices'])
                if existing:
                    self.update_device(vm, existing, res)
                else:
                    self.create_device(vm, res)

            for res in vm['devices']:
                if not first_or_default(lambda i: i['name'] == res['name'], updated_params['devices']):
                    self.delete_device(vm, res)

        if not updated_params.get('enabled', True):
            self.join_subtasks(self.run_subtask('vm.stop', id))

        vm.update(updated_params)
        self.datastore.update('vms', id, vm)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'update',
            'ids': [id]
        })

        vm = self.datastore.get_by_id('vms', id)
        save_config(
            self.dispatcher.call_sync(
                'volume.resolve_path',
                vm['target'],
                os.path.join('vm', vm['name'])
            ),
            'vm-{0}'.format(vm['name']),
            vm
        )


@accepts(str)
@description('Deletes VM')
class VMDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting VM'

    def describe(self, id):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Deleting VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id):
        if not self.datastore.exists('vms', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'VM {0} not found'.format(id))

        return ['system']

    def run(self, id):
        vm = self.datastore.get_by_id('vms', id)
        try:
            delete_config(
                self.dispatcher.call_sync(
                    'volume.resolve_path',
                    vm['target'],
                    os.path.join('vm', vm['name'])
                ),
                'vm-{0}'.format(vm['name'])
            )
        except (RpcException, OSError):
            pass

        pool = vm['target']
        root_ds = os.path.join(pool, 'vm')
        vm_ds = os.path.join(root_ds, vm['name'])

        try:
            self.join_subtasks(self.run_subtask('vm.stop', id, True))
        except RpcException:
            pass

        try:
            self.join_subtasks(self.run_subtask('volume.dataset.delete', vm_ds))
        except RpcException as err:
            if err.code != errno.ENOENT:
                raise err

        self.datastore.delete('vms', id)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description('Exports VM from database')
class VMExportTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Exporting VM'

    def describe(self, id):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Exporting VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id):
        if not self.datastore.exists('vms', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'VM {0} not found'.format(id))

        return ['system']

    def run(self, id):
        self.datastore.delete('vms', id)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description('Starts VM')
class VMStartTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Starting VM'

    def describe(self, id):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Starting VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id):
        vm = self.dispatcher.call_sync('vm.query', [('id', '=', id)], {'single': True})
        if not vm['enabled']:
            raise VerifyException(errno.EACCES, "Cannot start disabled VM {0}".format(id))
        return ['system']

    def run(self, id):
        self.dispatcher.call_sync('containerd.management.start_vm', id)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, bool)
@description('Stops VM')
class VMStopTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Stopping VM'

    def describe(self, id, force=False):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Stopping VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id, force=False):
        return ['system']

    def run(self, id, force=False):
        self.dispatcher.call_sync('containerd.management.stop_vm', id, force, timeout=120)
        self.dispatcher.dispatch_event('vm.changed', {
            'operation': 'update',
            'ids': [id]
        })


@accepts(str, bool)
@description('Reboots VM')
class VMRebootTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Rebooting VM'

    def describe(self, id, force=False):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Rebooting VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id, force=False):
        return ['system']

    def run(self, id, force=False):
        self.join_subtasks(self.run_subtask('vm.stop', id, force))
        self.join_subtasks(self.run_subtask('vm.start', id))


@accepts(str, str)
@description('Creates a snapshot of VM')
class VMSnapshotCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Creating snapshot of VM'

    def describe(self, id, name):
        vm = self.datastore.get_by_id('vms', id)
        return TaskDescription('Creating snapshot of VM {name}', name=vm.get('name', '') if vm else '')

    def verify(self, id, name):
        return ['system']

    def run(self, id, name):
        vm = self.datastore.get_by_id('vms', id)
        if not vm:
            raise TaskException(errno.ENOENT, 'VM {0} does not exist'.format(id))
        if vm['parent']:
            raise TaskException(errno.EACCES, 'Cannot create a snapshot of another snapshot')

        state = self.dispatcher.call_sync('vm.query', [('id', '=', id)], {'select': 'status.state', 'single': True})
        if state != 'STOPPED':
            raise TaskException(errno.EACCES, 'Cannot create a snapshot of a running VM')

        if self.dispatcher.call_sync('vm.snapshot.query', [('name', '=', name), ('parent', '=', vm['id'])]):
            raise TaskException(errno.EEXIST, 'Snapshot {0} of VM {1} already exists'.format(name, vm['name']))

        vm.pop('id')
        vm['parent'] = id
        vm['name'] = name
        snapshot_id = self.datastore.insert('vm.snapshots', vm)
        self.dispatcher.dispatch_event('vm.snapshot.changed', {
            'operation': 'create',
            'ids': [snapshot_id]
        })

        self.join_subtasks(self.run_subtask('volume.snapshot_dataset', vm['target'], True, None, snapshot_id))


@accepts(str)
@description('Deletes a snapshot of VM')
class VMSnapshotDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting snapshot of VM'

    def describe(self, id):
        snapshot = self.datastore.get_by_id('vm.snapshots', id)
        return TaskDescription('Deleting VM snapshot {name}', name=snapshot.get('name') if snapshot else '')

    def verify(self, id):
        return ['system']

    def run(self, id):
        snapshot = self.datastore.get_by_id('vm.snapshots', id)
        if not snapshot:
            raise TaskException(errno.ENOENT, 'Snapshot {0} does not exist'.format(id))

        snapshot_id = self.dispatcher.call_sync(
            'zfs.snapshot.query',
            [('name', '~', id)],
            {'single': True, 'select': 'id'}
        )
        self.join_subtasks(self.run_subtask('volume.snapshot.delete', snapshot_id))

        self.datastore.delete('vm.snapshots', id)
        self.dispatcher.dispatch_event('vm.snapshot.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@accepts(str)
@description('Caches VM files')
class CacheFilesTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Caching VM files'

    def describe(self, name):
        return TaskDescription('Caching VM files {name}', name=name or '')

    def verify(self, name):
        return ['system-dataset']

    def run(self, name):
        def collect_progress(percentage, message=None, extra=None):
            self.set_progress(
                (100 * (weight * idx)) + (percentage * weight),
                'Caching files: {0}'.format(message), extra
            )

        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_image_cache')
        template = self.dispatcher.call_sync(
            'vm.template.query',
            [('template.name', '=', name)],
            {'single': True}
        )
        if not template:
            raise TaskException(errno.ENOENT, 'Template of VM {0} does not exist'.format(name))

        self.set_progress(0, 'Caching images')
        res_cnt = len(template['template']['fetch'])
        weight = 1 / res_cnt

        for idx, res in enumerate(template['template']['fetch']):
            url = res['url']
            res_name = res['name']
            destination = os.path.join(cache_dir, name, res_name)
            sha256 = res['sha256']

            sha256_path = os.path.join(destination, 'sha256')
            if os.path.isdir(destination):
                if os.path.exists(sha256_path):
                    with open(sha256_path) as sha256_file:
                        if sha256_file.read() == sha256:
                            continue
            else:
                os.makedirs(destination)

            self.join_subtasks(self.run_subtask(
                'vm.file.download',
                url,
                sha256,
                destination,
                progress_callback=collect_progress
            ))

        self.set_progress(100, 'Cached images')


@accepts(str)
@description('Deletes cached VM files')
class DeleteFilesTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting cached VM files'

    def describe(self, name):
        return TaskDescription('Deleting cached VM {name} files', name=name)

    def verify(self, name):
        return ['system-dataset']

    def run(self, name):
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_image_cache')
        images_dir = os.path.join(cache_dir, name)
        shutil.rmtree(images_dir)


@accepts(str, str, str)
@description('Downloads VM file')
class DownloadFileTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Downloading VM file'

    def describe(self, url, sha256, destination):
        return TaskDescription('Downloading VM file {name}', name=url or '')

    def verify(self, url, sha256, destination):
        return ['system-dataset']

    def run(self, url, sha256, destination):
        done = 0

        @throttle(seconds=1)
        def progress_hook(nblocks, blocksize, totalsize):
            nonlocal done
            current_done = nblocks * blocksize
            speed = (current_done - done) / 1000
            self.set_progress((current_done / float(totalsize)) * 100, 'Downloading file {0} KB/s'.format(speed))
            done = current_done

        file_path = os.path.join(destination, url.split('/')[-1])
        sha256_path = os.path.join(destination, 'sha256')

        self.set_progress(0, 'Downloading file')
        urllib.request.urlretrieve(url, file_path, progress_hook)
        hasher = hashlib.sha256()

        self.set_progress(100, 'Verifying checksum')
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(BLOCKSIZE), b""):
                hasher.update(chunk)

        if hasher.hexdigest() != sha256:
            raise TaskException(errno.EINVAL, 'Invalid SHA256 checksum')

        with open(sha256_path, 'w') as sha256_file:
            sha256_file.write(sha256)


@accepts(str, str, str)
@description('Installs VM file')
class InstallFileTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Installing VM file'

    def describe(self, name, res, destination):
        return TaskDescription(
            'Installing VM file {name} in {destination}',
            name=os.path.join(name, res) or '',
            destination=destination or ''
        )

    def verify(self, name, res, destination):
        return ['system-dataset']

    def run(self, name, res, destination):
        self.set_progress(0, 'Installing file')
        cache_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_image_cache')
        files_path = os.path.join(cache_dir, name, res)
        file_path = self.get_archive_path(files_path)

        if not file_path:
            raise TaskException(errno.ENOENT, 'File {0} not found'.format(files_path))

        if file_path.endswith('tar.gz'):
            shutil.unpack_archive(file_path, destination)
        else:
            if os.path.isdir(destination):
                destination = os.path.join(destination, res)
            self.unpack_gzip(file_path, destination)

        self.set_progress(100, 'Finished')

    def get_archive_path(self, files_path):
        archive_path = None
        for file in os.listdir(files_path):
            if file.endswith('.gz'):
                archive_path = os.path.join(files_path, file)
        return archive_path

    def unpack_gzip(self, path, destination):
        @throttle(seconds=1)
        def report_progress():
            self.set_progress((zip_file.tell() / size) * 100, 'Installing file')

        size = os.path.getsize(path)
        with open(destination, 'wb') as dst:
            with open(path, 'rb') as zip_file:
                with gzip.open(zip_file) as src:
                    for chunk in iter(lambda: src.read(BLOCKSIZE), b""):
                        done = 0
                        total = len(chunk)

                        while done < total:
                            ret = os.write(dst.fileno(), chunk[done:])
                            if ret == 0:
                                raise TaskException(errno.ENOSPC, 'Image is too large to fit in destination')

                            done += ret
                        report_progress()


@description('Downloads VM templates')
class VMTemplateFetchTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return 'Downloading VM templates'

    def describe(self):
        return TaskDescription('Downloading VM templates')

    def verify(self):
        return ['system-dataset']

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

        templates_dir = self.dispatcher.call_sync('system_dataset.request_directory', 'vm_templates')

        progress = 0
        self.set_progress(progress, 'Downloading templates')
        template_sources = self.datastore.query('vm.template_sources')

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


def get_readme(path):
    file_path = None
    for file in os.listdir(path):
        if file == 'README.md':
            file_path = os.path.join(path, file)

    return file_path


@throttle(minutes=10)
def fetch_templates(dispatcher):
    dispatcher.call_task_sync('vm.template.fetch')


def _depends():
    return ['VolumePlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('vm', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'parent': {'type': ['string', 'null']},
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
            'config': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'memsize': {'type': 'integer'},
                    'ncpus': {'type': 'integer'},
                    'bootloader': {'$ref': 'vm-config-bootloader'},
                    'boot_device': {'type': ['string', 'null']},
                    'boot_partition': {'type': ['string', 'null']},
                    'boot_directory': {'type': ['string', 'null']},
                    'cloud_init': {'type': ['string', 'null']},
                    'vnc_enabled': {'type': 'boolean'},
                    'vnc_port': {'type': ['integer', 'null']},
                    'vnc_password': {'type': ['string', 'null']},
                    'autostart': {'type': 'boolean'},
                    'docker_host': {'type': 'boolean'},
                }
            },
            'devices': {
                'type': 'array',
                'items': {'$ref': 'vm-device'}
            }
        }
    })

    plugin.register_schema_definition('vm-config-bootloader', {
        'type': 'string',
        'enum': ['BHYVELOAD', 'GRUB', 'UEFI', 'UEFI_CSM']
    })

    plugin.register_schema_definition('vm-device', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'name': {'type': 'string'},
            'type': {'$ref': 'vm-device-type'},
            'properties': {'type': 'object'}
        },
        'required': ['name', 'type', 'properties']
    })

    plugin.register_schema_definition('vm-device-type', {
        'type': 'string',
        'enum': ['DISK', 'CDROM', 'NIC', 'VOLUME', 'GRAPHICS', 'USB']
    })

    plugin.register_schema_definition('vm-device-nic', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'device': {'$ref': 'vm-device-nic-device'},
            'mode': {'$ref': 'vm-device-nic-mode'},
            'link_address': {'type': 'string'},
            'bridge': {'type': ['string', 'null']}
        }
    })

    plugin.register_schema_definition('vm-device-nic-device', {
        'type': 'string',
        'enum': ['VIRTIO', 'E1000', 'NE2K']
    })

    plugin.register_schema_definition('vm-device-nic-mode', {
        'type': 'string',
        'enum': ['BRIDGED', 'NAT', 'HOSTONLY', 'MANAGEMENT']
    })

    plugin.register_schema_definition('vm-device-disk', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'mode': {'$ref': 'vm-device-disk-mode'},
            'size': {'type': 'integer'}
        }
    })

    plugin.register_schema_definition('vm-device-disk-mode', {
        'type': 'string',
        'enum': ['AHCI', 'VIRTIO']
    })

    plugin.register_schema_definition('vm-device-cdrom', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'path': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('vm-device-volume', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'$ref': 'vm-device-volume-type'},
            'auto': {'type': ['boolean', 'null']},
            'destination': {'type': ['string', 'null']}
        }
    })

    plugin.register_schema_definition('vm-device-volume-type', {
        'type': 'string',
        'enum': ['VT9P', 'NFS']
    })

    plugin.register_schema_definition('vm-device-graphics', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'resolution': {'$ref': 'vm-device-graphics-resolution'}
        }
    })

    plugin.register_schema_definition('vm-device-graphics-resolution', {
        'type': 'string',
        'enum': [
            '1920x1200',
            '1920x1080',
            '1600x1200',
            '1600x900',
            '1280x1024',
            '1280x720',
            '1024x768',
            '800x600',
            '640x480'
        ]
    })

    plugin.register_schema_definition('vm-device-usb', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'device': {'type': 'string'},
            'config': {
                'type': 'object'  # XXX: not sure what goes there
            }
        }
    })

    plugin.register_schema_definition('vm-device-usb-device', {
        'type': 'string',
        'enum': ['tablet']
    })

    def volume_pre_detach(args):
        for vm in dispatcher.call_sync('vm.query'):
            if vm['target'] == args['name']:
                dispatcher.call_task_sync('vm.stop', vm['id'])
        return True

    def volume_pre_destroy(args):
        for vm in dispatcher.call_sync('vm.query'):
            if vm['target'] == args['name']:
                dispatcher.call_task_sync('vm.delete', vm['id'])
        return True

    plugin.register_provider('vm', VMProvider)
    plugin.register_provider('vm.snapshot', VMSnapshotProvider)
    plugin.register_provider('vm.template', VMTemplateProvider)
    plugin.register_task_handler('vm.create', VMCreateTask)
    plugin.register_task_handler('vm.import', VMImportTask)
    plugin.register_task_handler('vm.update', VMUpdateTask)
    plugin.register_task_handler('vm.delete', VMDeleteTask)
    plugin.register_task_handler('vm.export', VMExportTask)
    plugin.register_task_handler('vm.start', VMStartTask)
    plugin.register_task_handler('vm.stop', VMStopTask)
    plugin.register_task_handler('vm.reboot', VMRebootTask)
    plugin.register_task_handler('vm.snapshot.create', VMSnapshotCreateTask)
    plugin.register_task_handler('vm.snapshot.delete', VMSnapshotDeleteTask)
    plugin.register_task_handler('vm.immutable.set', VMSetImmutableTask)
    plugin.register_task_handler('vm.file.install', InstallFileTask)
    plugin.register_task_handler('vm.file.download', DownloadFileTask)
    plugin.register_task_handler('vm.cache.update', CacheFilesTask)
    plugin.register_task_handler('vm.cache.delete', DeleteFilesTask)
    plugin.register_task_handler('vm.template.fetch', VMTemplateFetchTask)

    plugin.attach_hook('volume.pre_destroy', volume_pre_destroy)
    plugin.attach_hook('volume.pre_detach', volume_pre_detach)

    plugin.register_event_type('vm.changed')
    plugin.register_event_type('vm.snapshot.changed')
