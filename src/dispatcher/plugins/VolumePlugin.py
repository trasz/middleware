#+
# Copyright 2014 iXsystems, Inc.
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
import re
import gevent
import logging
import tempfile
import shutil
import itertools
import base64
import copy
import bsd
import bsd.kld
import hashlib
import json
import time
import uuid
from cache import EventCacheStore
from lib.system import system, SubprocessException
from lib.freebsd import fstyp
from task import (
    Provider, Task, ProgressTask, TaskException, TaskWarning, VerifyException, query,
    TaskDescription
)
from freenas.dispatcher.rpc import (
    RpcException, description, accepts, returns, private, SchemaHelper as h, generator
)
from utils import first_or_default, load_config
from datastore import DuplicateKeyException
from freenas.utils import include, exclude, normalize, chunks, yesno_to_bool, remove_unchanged, query as q
from freenas.utils.copytree import count_files, copytree
from cryptography.fernet import Fernet, InvalidToken


VOLUME_LAYOUTS = {
    'stripe': 'disk',
    'mirror': 'mirror',
    'raidz': 'raidz1',
    'raidz1': 'raidz1',
    'raidz2': 'raidz2',
    'raidz3': 'raidz3',
    'virtualization': 'mirror',
    'speed': 'mirror',
    'backup': 'raidz2',
    'safety': 'raidz2',
    'storage': 'raidz1',
    'auto': 'mirror'
}

DISKS_PER_VDEV = {
    'disk': 1,
    'mirror': 2,
    'raidz1': 3,
    'raidz2': 4,
    'raidz3': 5
}

VOLUMES_ROOT = '/mnt'
DEFAULT_ACLS = [
    {'text': 'owner@:rwxpDdaARWcCos:fd:allow'},
    {'text': 'group@:rwxpDdaARWcCos:fd:allow'},
    {'text': 'everyone@:rxaRc:fd:allow'}
]
logger = logging.getLogger('VolumePlugin')
snapshots = None
datasets = None


@description("Provides access to volumes information")
class VolumeProvider(Provider):
    @query('volume')
    @generator
    def query(self, filter=None, params=None):
        def is_upgraded(pool):
            if q.get(pool, 'properties.version.value') != '-':
                return False

            for feat in pool['features']:
                if feat['state'] == 'DISABLED':
                    return False

            return True

        def extend(vol):
            config = self.dispatcher.call_sync('zfs.pool.query', [('id', '=', vol['id'])], {'single': True})
            if not config:
                vol['status'] = 'UNKNOWN'
            else:
                topology = config['groups']
                for vdev, _ in iterate_vdevs(topology):
                    try:
                        vdev['path'] = self.dispatcher.call_sync(
                            'disk.partition_to_disk',
                            vdev['path']
                        )
                    except RpcException as err:
                        if err.code == errno.ENOENT:
                            pass

                vol.update({
                    'rname': 'zpool:{0}'.format(vol['id']),
                    'description': None,
                    'mountpoint': None,
                    'upgraded': None,
                    'topology': topology,
                    'root_vdev': config['root_vdev'],
                    'status': config['status'],
                    'scan': config['scan'],
                    'properties': include(
                        config['properties'],
                        'size', 'capacity', 'health', 'version', 'delegation', 'failmode',
                        'autoreplace', 'dedupratio', 'free', 'allocated', 'readonly',
                        'comment', 'expandsize', 'fragmentation', 'leaked'
                    )
                })

                if config['status'] != 'UNAVAIL':
                    vol.update({
                        'description': q.get(config, 'root_dataset.properties.org\\.freenas:description.value'),
                        'mountpoint': q.get(config, 'root_dataset.properties.mountpoint.value'),
                        'upgraded': is_upgraded(config),
                    })

            encrypted = vol.get('key_encrypted', False) or vol.get('password_encrypted', False)
            if encrypted is True:
                online = 0
                offline = 0
                for vdev, _ in get_disks(vol['topology']):
                    try:
                        vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                        if vdev_conf.get('encrypted', False) is True:
                            online += 1
                        else:
                            offline += 1
                    except RpcException:
                        offline += 1
                        pass

                if offline == 0:
                    presence = 'ALL'
                elif online == 0:
                    presence = 'NONE'
                else:
                    presence = 'PART'
            else:
                presence = None

            vol.update({
                'providers_presence': presence
            })

            return vol

        return q.query(
            self.datastore.query_stream('volumes', callback=extend),
            *(filter or []),
            stream=True,
            **(params or {})
        )

    @description("Finds volumes available for import")
    @accepts()
    @returns(h.array(
        h.object(properties={
            'id': str,
            'name': str,
            'topology': h.ref('zfs-topology'),
            'status': str
        })
    ))
    def find(self):
        result = []
        for pool in self.dispatcher.call_sync('zfs.pool.find'):
            topology = pool['groups']
            for vdev, _ in iterate_vdevs(topology):
                try:
                    vdev['path'] = self.dispatcher.call_sync(
                        'disk.partition_to_disk',
                        vdev['path']
                    )
                except RpcException:
                    pass

            if self.datastore.exists('volumes', ('id', '=', pool['guid'])):
                continue

            result.append({
                'id': str(pool['guid']),
                'name': pool['name'],
                'topology': topology,
                'status': pool['status']
            })

        return result

    @returns(h.array(h.ref('importable-disk')))
    def find_media(self):
        result = []

        for disk in self.dispatcher.call_sync('disk.query', [('path', 'in', self.get_available_disks())]):
            # Try whole disk first
            typ, label = fstyp(disk['path'])
            if typ:
                result.append({
                    'path': disk['path'],
                    'size': disk['mediasize'],
                    'fstype': typ,
                    'label': label or disk['description']
                })
                continue

            for part in q.get(disk, 'status.partitions'):
                path = part['paths'][0]
                typ, label = fstyp(path)
                if typ:
                    result.append({
                        'path': path,
                        'size': part['mediasize'],
                        'fstype': typ,
                        'label': label or disk['description']
                    })

        return result

    @accepts(str, str)
    @returns(str)
    def resolve_path(self, volname, path):
        volume = self.dispatcher.call_sync('volume.query', [('id', '=', volname)], {'single': True})
        if not volume:
            raise RpcException(errno.ENOENT, 'Volume {0} not found'.format(volname))

        return os.path.join(volume['mountpoint'], path)

    @accepts(str)
    @returns(str)
    def get_dataset_path(self, dsname):
        return os.path.join(VOLUMES_ROOT, dsname)

    @description("Extracts volume name, dataset name and relative path from full path")
    @accepts(str)
    @returns(h.tuple(str, str, h.one_of(str, None)))
    def decode_path(self, path):
        path = os.path.normpath(path)[1:]
        tokens = path.split(os.sep)

        if tokens[0:2] == ['dev', 'zvol']:
            return tokens[2], '/'.join(tokens[2:]), None

        if tokens[0] != VOLUMES_ROOT[1:]:
            raise RpcException(errno.EINVAL, 'Invalid path')

        volname = tokens[1]
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', volname)], {'single': True})
        if not vol:
            raise RpcException(errno.ENOENT, "Volume '{0}' does not exist".format(volname))

        datasets = list(self.dispatcher.call_sync('volume.dataset.query', [('volume', '=', volname)], {'select': 'id'}))
        n = len(tokens)

        while n > 0:
            fragment = '/'.join(tokens[1:n])
            if fragment in datasets:
                return volname, fragment, '/'.join(tokens[n:])

            n -= 1

        raise RpcException(errno.ENOENT, 'Cannot look up path')

    @description("Returns Disks associated with Volume specified in the call")
    @accepts(str)
    @returns(h.array(str))
    def get_volume_disks(self, name):
        result = []
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', name)], {'single': True})

        encrypted = vol.get('key_encrypted', False) or vol.get('password_encrypted', False)
        if not encrypted or vol.get('providers_presence', 'NONE') != 'NONE':
            for dev in self.dispatcher.call_sync('zfs.pool.get_disks', name):
                try:
                    result.append(self.dispatcher.call_sync('disk.partition_to_disk', dev))
                except RpcException:
                    pass

        return result

    @description("Returns dataset tree for given pool")
    @accepts(str)
    @returns(h.ref('zfs-dataset'))
    def get_dataset_tree(self, name):
        pool = self.dispatcher.call_sync(
            'zfs.pool.query',
            [('name', '=', name)],
            {"single": True})

        if not pool:
            return None

        return pool['root_dataset']

    @description("Returns the list of disks currently not used by any Volume")
    @accepts()
    @returns(h.array(str))
    def get_available_disks(self):
        disks = set([d['path'] for d in self.dispatcher.call_sync('disk.query')])
        for pool in self.dispatcher.call_sync('zfs.pool.query'):
            for dev in self.dispatcher.call_sync('zfs.pool.get_disks', pool['id']):
                try:
                    disk = self.dispatcher.call_sync('disk.partition_to_disk', dev)
                except RpcException:
                    continue

                disks.remove(disk)

        return list(disks)

    @description("Returns allocation of given disk")
    @accepts(h.array(str))
    @returns(h.ref('disks-allocation'))
    def get_disks_allocation(self, disks):
        ret = {}
        boot_pool_name = self.configstore.get('system.boot_pool_name')
        boot_devs = self.dispatcher.call_sync('zfs.pool.get_disks', boot_pool_name)

        for dev in boot_devs:
            boot_disk = self.dispatcher.call_sync('disk.partition_to_disk', dev)
            if os.path.join('/dev', boot_disk) in disks:
                ret[boot_disk] = {'type': 'BOOT'}

        for vol in self.dispatcher.call_sync('volume.query'):
            if vol['status'] in ('UNAVAIL', 'UNKNOWN'):
                continue

            for dev in self.dispatcher.call_sync('volume.get_volume_disks', vol['id']):
                if dev in disks:
                    ret[dev] = {
                        'type': 'VOLUME',
                        'name': vol['id']
                    }

        for disk in set(disks) - set(ret.keys()):
            try:
                label = self.get_disk_label(disk)
                ret[disk] = {
                    'type': 'EXPORTED_VOLUME',
                    'name': label['volume_id']
                }
            except:
                continue

        return ret

    @accepts(str, str)
    @returns(h.ref('zfs-vdev'))
    def vdev_by_guid(self, volume, guid):
        vdev = self.dispatcher.call_sync('zfs.pool.vdev_by_guid', volume, guid)
        vdev['path'] = self.dispatcher.call_sync(
            'disk.partition_to_disk',
            vdev['path']
        )

        return vdev

    @accepts(str)
    @returns(h.ref('volume-disk-label'))
    def get_disk_label(self, disk):
        dev = get_disk_gptid(self.dispatcher, disk)
        if not dev:
            raise RpcException(errno.ENOENT, 'Disk {0} not found'.format(disk))

        label = self.dispatcher.call_sync('zfs.pool.get_disk_label', dev)
        return {
            'volume_id': label['name'],
            'volume_guid': str(label['pool_guid']),
            'vdev_guid': str(label['guid']),
            'hostname': label['hostname'],
            'hostid': label.get('hostid')
        }

    @description("Returns volume capabilities")
    @accepts(str)
    @returns(h.object())
    def get_capabilities(self, type):
        if type == 'zfs':
            return self.dispatcher.call_sync('zfs.pool.get_capabilities')

        raise RpcException(errno.EINVAL, 'Invalid volume type')

    @accepts()
    @returns(str)
    @private
    def get_volumes_root(self):
        return VOLUMES_ROOT

    @accepts()
    @returns(h.ref('volume-vdev-recommendations'))
    def vdev_recommendations(self):
        return {
            "storage": {
                "storage": {"drives": 9, "type": "raidz1"},
                "redundancy": {"drives": 10, "type": "raidz2"},
                "speed": {"drives": 7, "type": "raidz1"}
            },
            "redundancy": {
                "storage": {"drives": 8, "type": "raidz2"},
                "redundancy": {"drives": 4, "type": "raidz2"},
                "speed": {"drives": 6, "type": "raidz1"}
            },
            "speed": {
                "storage": {"drives": 3, "type": "raidz1"},
                "redundancy": {"drives": 2, "type": "mirror"},
                "speed": {"drives": 2, "type": "mirror"}
            }
        }


@description('Provides information about datasets')
class DatasetProvider(Provider):
    @query('volume-dataset')
    @generator
    def query(self, filter=None, params=None):
        return datasets.query(*(filter or []), stream=True, **(params or {}))


@description('Provides information about snapshots')
class SnapshotProvider(Provider):
    @query('volume-snapshot')
    @generator
    def query(self, filter=None, params=None):
        return snapshots.query(*(filter or []), stream=True, **(params or {}))


@description("Creating a volume")
@accepts(
    h.all_of(
        h.ref('volume'),
        h.required('id', 'topology')
    ),
    h.one_of(str, None)
)
class VolumeCreateTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Creating a volume"

    def describe(self, volume, password=None):
        return TaskDescription("Creating volume {name}", name=volume['id'])

    def verify(self, volume, password=None):
        return ['disk:{0}'.format(disk_spec_to_path(self.dispatcher, i)) for i, _ in get_disks(volume['topology'])]

    def run(self, volume, password=None):
        if self.datastore.exists('volumes', ('id', '=', volume['id'])):
            raise TaskException(errno.EEXIST, 'Volume with same name already exists')

        name = volume['id']
        type = volume.get('type', 'zfs')
        params = volume.get('params') or {}
        mount = params.get('mount', True)
        mountpoint = params.pop(
            'mountpoint',
            os.path.join(VOLUMES_ROOT, volume['id'])
        )
        key_encryption = volume.pop('key_encrypted', False)
        password_encryption = volume.pop('password_encrypted', False)
        auto_unlock = volume.pop('auto_unlock', None)

        if auto_unlock and (not key_encryption or password_encryption):
            raise TaskException(
                errno.EINVAL,
                'Automatic volume unlock can be selected for volumes using only key based encryption.'
            )

        self.dispatcher.run_hook('volume.pre_create', {'name': name})
        if key_encryption:
            key = base64.b64encode(os.urandom(256)).decode('utf-8')
        else:
            key = None

        if password_encryption:
            if not password:
                raise TaskException(errno.EINVAL, 'Please provide a password when choosing a password based encryption')
            salt, digest = get_digest(password)
        else:
            salt = None
            digest = None

        if type != 'zfs':
            raise TaskException(errno.EINVAL, 'Invalid volume type')

        self.set_progress(10)

        subtasks = []
        for dname, dgroup in get_disks(volume['topology']):
            disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
            subtasks.append(self.run_subtask('disk.format.gpt', disk_id, 'freebsd-zfs', {
                'blocksize': params.get('blocksize', 4096),
                'swapsize': params.get('swapsize', 2048) if dgroup == 'data' else 0
            }))

        self.join_subtasks(*subtasks)

        disk_ids = []
        for dname, dgroup in get_disks(volume['topology']):
            disk_ids.append(self.dispatcher.call_sync('disk.path_to_id', dname))

        self.set_progress(20)

        if key_encryption or password_encryption:
            subtasks = []
            for disk_id in disk_ids:
                subtasks.append(self.run_subtask('disk.geli.init', disk_id, {
                    'key': key,
                    'password': password
                }))
            self.join_subtasks(*subtasks)
            self.set_progress(30)

            subtasks = []
            for disk_id in disk_ids:
                subtasks.append(self.run_subtask('disk.geli.attach', disk_id, {
                    'key': key,
                    'password': password
                }))
            self.join_subtasks(*subtasks)

        self.set_progress(40)

        with self.dispatcher.get_lock('volumes'):
            self.join_subtasks(self.run_subtask(
                'zfs.pool.create',
                name,
                convert_topology_to_gptids(
                    self.dispatcher,
                    volume['topology']
                ),
                {'mountpoint': mountpoint}
            ))

            self.join_subtasks(self.run_subtask(
                'zfs.update',
                name,
                {'org.freenas:permissions_type': {'value': 'PERM'}}
            ))

            self.set_progress(60)

            if mount:
                self.join_subtasks(self.run_subtask('zfs.mount', name))

            self.set_progress(80)

            pool = self.dispatcher.call_sync('zfs.pool.query', [('name', '=', name)], {'single': True})
            id = self.datastore.insert('volumes', {
                'id': name,
                'guid': str(pool['guid']),
                'type': type,
                'mountpoint': mountpoint,
                'topology': volume['topology'],
                'encryption': {
                    'key': key,
                    'hashed_password': digest,
                    'salt': salt,
                    'slot': 0 if key_encryption or password_encryption else None},
                'key_encrypted': key_encryption,
                'password_encrypted': password_encryption,
                'auto_unlock': auto_unlock,
                'attributes': volume.get('attributes', {})
            })

        self.set_progress(90)
        self.dispatcher.dispatch_event('volume.changed', {
            'operation': 'create',
            'ids': [id]
        })


@description("Creates new volume and automatically guesses disks layout")
@accepts(str, str, str, h.one_of(h.array(str), str), h.array(str), h.array(str), h.one_of(bool, None), h.one_of(str, None), h.one_of(bool, None))
class VolumeAutoCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating a volume"

    def describe(self, name, type, layout, disks, cache_disks=None, log_disks=None, key_encryption=False, password=None, auto_unlock=None):
        return TaskDescription("Creating the volume {name}", name=name)

    def verify(self, name, type, layout, disks, cache_disks=None, log_disks=None, key_encryption=False, password=None, auto_unlock=None):
        if isinstance(disks, str) and disks == 'auto':
            return ['disk:{0}'.format(disk) for disk in self.dispatcher.call_sync('disk.query', {'select': 'path'})]
        else:
            return ['disk:{0}'.format(disk_spec_to_path(self.dispatcher, i)) for i in disks]

    def run(self, name, type, layout, disks, cache_disks=None, log_disks=None, key_encryption=False, password=None, auto_unlock=None):
        if self.datastore.exists('volumes', ('id', '=', name)):
            raise TaskException(
                errno.EEXIST,
                'Volume with same name already exists'
            )

        vdevs = []
        ltype = VOLUME_LAYOUTS[layout]
        ndisks = DISKS_PER_VDEV[ltype]

        if isinstance(disks, str) and disks == 'auto':
            available_disks = self.dispatcher.call_sync(
                'disk.query',
                [('path', 'in', self.dispatcher.call_sync('volume.get_available_disks'))]
            )
            available_disks = sorted(
                available_disks,
                key=lambda item: (not item['status']['is_ssd'], item['mediasize'], item['status']['max_rotation']),
                reverse=True
            )
            disks = q.query(
                available_disks,
                ('status.is_ssd', '=', available_disks[0]['status']['is_ssd']),
                ('status.max_rotation', '=', available_disks[0]['status']['max_rotation']),
                ('mediasize', '=', available_disks[0]['mediasize']),
                select='path'
            )

        if len(disks) == 1:
            ltype = 'disk'
            ndisks = 1

        for chunk in chunks(disks, ndisks):
            if len(chunk) != ndisks:
                break

            if ltype == 'disk':
                vdevs.append({
                    'type': 'disk',
                    'path': disk_spec_to_path(self.dispatcher, chunk[0])
                })
            else:
                vdevs.append({
                    'type': ltype,
                    'children': [
                        {'type': 'disk', 'path': disk_spec_to_path(self.dispatcher, i)} for i in chunk
                    ]
                })

        cache_vdevs = [
            {'type': 'disk', 'path': disk_spec_to_path(self.dispatcher, i)} for i in cache_disks or []
        ]

        log_vdevs = [
            {'type': 'disk', 'path': disk_spec_to_path(self.dispatcher, i)} for i in log_disks or []
        ]

        self.join_subtasks(self.run_subtask(
            'volume.create',
            {
                'id': name,
                'type': type,
                'topology': {
                    'data': vdevs,
                    'cache': cache_vdevs,
                    'log': log_vdevs
                },
                'key_encrypted': key_encryption,
                'password_encrypted': True if password else False,
                'auto_unlock': auto_unlock
            },
            password
        ))


@description("Destroys active volume")
@accepts(str)
class VolumeDestroyTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting volume"

    def describe(self, id):
        return TaskDescription("Deleting the volume {name}", name=id)

    def verify(self, id):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        try:
            disks = self.dispatcher.call_sync('volume.get_volume_disks', id)
            return ['disk:{0}'.format(d) for d in disks]
        except RpcException:
            return []

    def run(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        config = self.dispatcher.call_sync('zfs.pool.query', [('id', '=', id)], {'single': True})

        self.dispatcher.run_hook('volume.pre_destroy', {'name': id})

        with self.dispatcher.get_lock('volumes'):
            if config:
                try:
                    self.join_subtasks(self.run_subtask('zfs.umount', id))
                    self.join_subtasks(self.run_subtask('zfs.pool.destroy', id))
                except RpcException as err:
                    if err.code == errno.EBUSY:
                        # Find out what's holding unmount or destroy
                        files = self.dispatcher.call_sync('filesystem.get_open_files', vol['mountpoint'])
                        if len(files) == 1:
                            raise TaskException(
                                errno.EBUSY,
                                'Volume is in use by {process_name} (pid {pid}, path {path})'.format(**files[0]),
                                extra=files
                            )
                        else:
                            raise TaskException(
                                errno.EBUSY,
                                'Volume is in use by {0} processes'.format(len(files)),
                                extra=files
                            )
                    else:
                        raise

            try:
                if vol.get('mountpoint'):
                    os.rmdir(vol['mountpoint'])
            except FileNotFoundError:
                pass

            if vol['key_encrypted'] or vol['password_encrypted']:
                subtasks = []
                if 'topology' in vol:
                    for dname, _ in get_disks(vol['topology']):
                        disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                        subtasks.append(self.run_subtask('disk.geli.kill', disk_id))
                    self.join_subtasks(*subtasks)

            self.datastore.delete('volumes', vol['id'])
            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'delete',
                'ids': [vol['id']]
            })


@description("Updates configuration of existing volume")
@accepts(str, h.ref('volume'), h.one_of(str, None))
class VolumeUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating a volume"

    def describe(self, id, updated_params, password=None):
        return TaskDescription("Updating the volume {name}", name=id)

    def verify(self, id, updated_params, password=None):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        topology = updated_params.get('topology')
        if not topology:
            disks = self.dispatcher.call_sync('volume.get_volume_disks', id)
            return ['disk:{0}'.format(d) for d in disks]

        return ['disk:{0}'.format(i) for i, _ in get_disks(topology)]

    def run(self, id, updated_params, password=None):
        if password is None:
            password = ''

        volume = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        remove_unchanged(updated_params, volume)

        encryption = volume.get('encryption')
        if not volume:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        if 'id' in updated_params:
            # Renaming pool. Need to export and import again using different name
            new_name = updated_params['id']
            self.dispatcher.run_hook('volume.pre_rename', {'name': id, 'new_name': new_name})

            # Rename mountpoint
            self.join_subtasks(self.run_subtask('zfs.update', id, {
                'mountpoint': {'value': '{0}/{1}'.format(VOLUMES_ROOT, new_name)}
            }))

            self.join_subtasks(self.run_subtask('zfs.pool.export', id))
            self.join_subtasks(self.run_subtask('zfs.pool.import', volume['guid'], new_name))

            # Configure newly imported volume
            self.join_subtasks(self.run_subtask('zfs.update', new_name, {}))
            self.join_subtasks(self.run_subtask('zfs.mount', new_name))

            volume['id'] = new_name
            self.datastore.update('volumes', volume['id'], volume)
            self.dispatcher.run_hook('volume.post_rename', {
                'name': id,
                'mountpoint': os.path.join(VOLUMES_ROOT, id),
                'new_name': new_name,
                'new_mountpoint': os.path.join(VOLUMES_ROOT, new_name)
            })

        if 'topology' in updated_params:
            new_vdevs = {}
            removed_vdevs = []
            updated_vdevs = []
            params = {}
            subtasks = []
            new_topology = updated_params['topology']
            old_topology = self.dispatcher.call_sync(
                'volume.query',
                [('id', '=', id)],
                {'single': True, 'select': 'topology'}
            )

            for group, vdevs in old_topology.items():
                for vdev in vdevs:
                    new_vdev = first_or_default(lambda v: v.get('guid') == vdev['guid'], new_topology.get(group, []))
                    if not new_vdev:
                        # Vdev is missing - it's either remove vdev request or bogus topology
                        if group == 'data':
                            raise TaskException(errno.EBUSY, 'Cannot remove data vdev {0}'.format(vdev['guid']))

                        removed_vdevs.append(vdev['guid'])
                        continue

                    if compare_vdevs(new_vdev, vdev):
                        continue

                    if new_vdev['type'] not in ('disk', 'mirror'):
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot detach vdev {0}, {1} is not mirror or disk'.format(
                                vdev['guid'],
                                vdev['type']
                            )
                        )

                    if vdev['type'] not in ('disk', 'mirror'):
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot change vdev {0} type ({1}) to {2}'.format(
                                vdev['guid'],
                                vdev['type'],
                                new_vdev['type']
                            )
                        )

                    if vdev['type'] == 'mirror' and new_vdev['type'] == 'mirror' and len(new_vdev['children']) < 2:
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot mirror vdev {0} must have at least two members'.format(vdev['guid'])
                        )

                    if new_vdev['type'] == 'mirror':
                        for i in vdev['children']:
                            if not first_or_default(lambda v: v.get('guid') == i['guid'], new_vdev['children']):
                                removed_vdevs.append(i['guid'])

            for group, vdevs in list(updated_params['topology'].items()):
                for vdev in vdevs:
                    if 'guid' not in vdev:
                        if encryption['hashed_password'] is not None:
                            if not is_password(
                                password,
                                encryption.get('salt', ''),
                                encryption.get('hashed_password', '')
                            ):
                                raise TaskException(
                                    errno.EINVAL,
                                    'Password provided for volume {0} configuration update is not valid'.format(id)
                                )
                        new_vdevs.setdefault(group, []).append(vdev)
                        continue

                    # look for vdev in existing configuration using guid
                    old_vdev = first_or_default(lambda v: v['guid'] == vdev['guid'], old_topology[group])
                    if not old_vdev:
                        raise TaskException(errno.EINVAL, 'Cannot extend vdev {0}: not found'.format(vdev['guid']))

                    if compare_vdevs(old_vdev, vdev):
                        continue

                    if old_vdev['type'] not in ('disk', 'mirror'):
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot extend vdev {0}, {1} is not mirror or disk'.format(
                                old_vdev['guid'],
                                old_vdev['type']
                            )
                        )

                    if vdev['type'] != 'mirror':
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot change vdev {0} type ({1}) to {2}'.format(
                                old_vdev['guid'],
                                old_vdev['type'],
                                vdev['type']
                            )
                        )

                    if old_vdev['type'] == 'mirror' and vdev['type'] == 'mirror' and \
                       len(old_vdev['children']) + 1 != len(vdev['children']):
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot extend mirror vdev {0} by more than one disk at once'.format(vdev['guid'])
                        )

                    if old_vdev['type'] == 'disk' and vdev['type'] == 'mirror' and len(vdev['children']) != 2:
                        raise TaskException(
                            errno.EINVAL,
                            'Cannot extend disk vdev {0} by more than one disk at once'.format(vdev['guid'])
                        )

                    updated_vdevs.append({
                        'target_guid': vdev['guid'],
                        'vdev': vdev['children'][-1]
                    })

            for vdev in removed_vdevs:
                self.join_subtasks(self.run_subtask('zfs.pool.detach', id, vdev))

            for vdev, group in iterate_vdevs(new_vdevs):
                if vdev['type'] == 'disk':
                    subtasks.append(self.run_subtask(
                        'disk.format.gpt',
                        self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                        'freebsd-zfs',
                        {
                            'blocksize': params.get('blocksize', 4096),
                            'swapsize': params.get('swapsize', 2048) if group == 'data' else 0
                        }
                    ))

            for vdev in updated_vdevs:
                subtasks.append(self.run_subtask(
                    'disk.format.gpt',
                    self.dispatcher.call_sync('disk.path_to_id', vdev['vdev']['path']),
                    'freebsd-zfs',
                    {
                        'blocksize': params.get('blocksize', 4096),
                        'swapsize': params.get('swapsize', 2048)
                    }
                ))

            self.join_subtasks(*subtasks)

            if encryption['key'] or encryption['hashed_password']:
                subtasks = []
                for vdev, group in iterate_vdevs(new_vdevs):
                    subtasks.append(self.run_subtask(
                        'disk.geli.init',
                        self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                        {
                            'key': encryption['key'],
                            'password': password
                        }
                    ))
                self.join_subtasks(*subtasks)

                if encryption['slot'] != 0:
                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask(
                            'disk.geli.ukey.set',
                            self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                            {
                                'key': encryption['key'],
                                'password': password,
                                'slot': 1
                            }
                        ))
                    self.join_subtasks(*subtasks)

                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask(
                            'disk.geli.ukey.del',
                            self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                            0
                        ))
                    self.join_subtasks(*subtasks)

                vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
                if vol.get('providers_presence', 'NONE') != 'NONE':
                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask(
                            'disk.geli.attach',
                            self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                            {
                                'key': encryption['key'],
                                'password': password
                            }
                        ))
                    self.join_subtasks(*subtasks)

            new_vdevs_gptids = convert_topology_to_gptids(self.dispatcher, new_vdevs)

            for vdev in updated_vdevs:
                vdev['vdev']['path'] = get_disk_gptid(self.dispatcher, vdev['vdev']['path'])

            self.join_subtasks(self.run_subtask(
                'zfs.pool.extend',
                id,
                new_vdevs_gptids,
                updated_vdevs)
            )

            volume['topology'] = new_topology
            if 'auto_unlock' in updated_params:
                auto_unlock = updated_params.get('auto_unlock', False)
                if auto_unlock and (not volume.get('key_encryption') or volume.get('password_encryption')):
                    raise TaskException(
                        errno.EINVAL,
                        'Automatic volume unlock can be selected for volumes using only key based encryption.'
                    )

                volume['auto_unlock'] = auto_unlock

            self.datastore.update('volumes', volume['id'], volume)
            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [volume['id']]
            })


@description("Imports previously exported volume")
@accepts(str, str, h.object(), h.ref('volume-import-params'), h.one_of(str, None))
class VolumeImportTask(Task):
    @classmethod
    def early_describe(cls):
        return "Importing a volume"

    def describe(self, id, new_name, params=None, enc_params=None, password=None):
        return TaskDescription("Importing the volume {name}", name=new_name)

    def verify(self, id, new_name, params=None, enc_params=None, password=None):
        if enc_params is None:
            enc_params = {}

        if enc_params.get('key') or password:
            disks = enc_params.get('disks', None)
            if disks is None:
                raise VerifyException(
                    errno.EINVAL, 'List of disks must be provided for import')
            else:
                if isinstance(disks, str):
                    disks = [disks]
                return ['disk:{0}'.format(i) for i in disks]
        else:
            return self.verify_subtask('zfs.pool.import', id)

    def run(self, id, new_name, params=None, enc_params=None, password=None):
        if self.datastore.exists('volumes', ('id', '=', id)):
            raise TaskException(
                errno.ENOENT,
                'Volume with id {0} already exists'.format(id)
            )

        if self.datastore.exists('volumes', ('id', '=', new_name)):
            raise TaskException(
                errno.ENOENT,
                'Volume with name {0} already exists'.format(new_name)
            )

        if enc_params is None:
            enc_params = {}

        with self.dispatcher.get_lock('volumes'):
            key = enc_params.get('key')

            if password:
                salt, digest = get_digest(password)
            else:
                salt = None
                digest = None

            if key or password:
                disks = enc_params.get('disks', [])
                if isinstance(disks, str):
                    disks = [disks]

                attach_params = {'key': key, 'password': password}
                for dname in disks:
                    disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                    self.join_subtasks(self.run_subtask('disk.geli.attach', disk_id, attach_params))

            mountpoint = os.path.join(VOLUMES_ROOT, new_name)
            self.join_subtasks(self.run_subtask('zfs.pool.import', id, new_name, params))
            self.join_subtasks(self.run_subtask(
                'zfs.update',
                new_name,
                {'mountpoint': {'value': mountpoint}}
            ))

            self.join_subtasks(self.run_subtask('zfs.mount', new_name, True))

            new_id = self.datastore.insert('volumes', {
                'id': new_name,
                'guid': id,
                'type': 'zfs',
                'encryption': {
                    'key': key ,
                    'hashed_password': digest,
                    'salt': salt,
                    'slot': 0 if key or password else None},
                'key_encrypted': True if key else False,
                'password_encrypted': True if password else False,
                'mountpoint': mountpoint
            })

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'create',
                'ids': [new_id]
            })
        self.dispatcher.run_hook('volume.post_attach', {'name': new_name})


@description("Imports non-ZFS disk contents into existing volume")
@accepts(str, str, str)
class VolumeDiskImportTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Importing disk into volume"

    def describe(self, src, dest_path, fstype=None):
        return TaskDescription("Importing disk {src} into {dst} path", src=src, dst=dest_path)

    def verify(self, src, dest_path, fstype=None):
        disk = self.dispatcher.call_sync('disk.partition_to_disk', src)
        if not disk:
            raise VerifyException(errno.ENOENT, "Partition {0} not found".format(src))

        return ['disk:{0}'.format(disk)]

    def run(self, src, dest_path, fstype=None):
        if not fstype:
            try:
                fstype, _ = system('/usr/sbin/fstyp', src)
            except SubprocessException:
                raise TaskException(errno.EINVAL, 'Cannot figure out filesystem type')

        if fstype == 'ntfs':
            try:
                bsd.kld.kldload('/boot/kernel/fuse.ko')
            except OSError as err:
                raise TaskException(err.errno, str(err))

        src_mount = tempfile.mkdtemp()

        try:
            bsd.nmount(source=src, fspath=src_mount, fstype=fstype)
        except OSError as err:
            raise TaskException(err.errno, "Cannot mount disk: {0}".format(str(err)))

        def callback(srcfile, dstfile):
            self.set_progress(self.copied / self.nfiles * 100, "Copying {0}".format(os.path.basename(srcfile)))

        self.set_progress(0, "Counting files...")
        self.nfiles = count_files(src_mount)
        self.copied = 0
        failures = []

        try:
            copytree(src_mount, dest_path, progress_callback=callback)
        except shutil.Error as err:
            failures = list(err)

        try:
            bsd.unmount(src_mount, bsd.MountFlags.FORCE)
        except OSError:
            pass

        bsd.kld.kldunload('fuse')
        os.rmdir(src_mount)
        return failures


@description("Exports active volume")
@accepts(str)
class VolumeDetachTask(Task):
    @classmethod
    def early_describe(cls):
        return "Detaching a volume"

    def describe(self, id):
        return TaskDescription("Detaching the volume {name}", name=id)

    def verify(self, id):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', vol['id'])]

    def run(self, id):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.dispatcher.run_hook('volume.pre_detach', {'name': id})
        vol = self.datastore.get_by_id('volumes', id)
        disks = self.dispatcher.call_sync('volume.get_volume_disks', id)
        self.join_subtasks(self.run_subtask('zfs.umount', id))
        self.join_subtasks(self.run_subtask('zfs.pool.export', id))

        encryption = vol.get('encryption')

        if encryption['key'] or encryption['hashed_password']:
            subtasks = []
            for dname in disks:
                disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                subtasks.append(self.run_subtask('disk.geli.detach', disk_id))
            self.join_subtasks(*subtasks)

        self.datastore.delete('volumes', vol['id'])

        if encryption['key']:
            return encryption['key']
        else:
            return None


@description("Upgrades volume to newest ZFS version")
@accepts(str)
class VolumeUpgradeTask(Task):
    @classmethod
    def early_describe(cls):
        return "Upgrading a volume"

    def describe(self, id):
        return TaskDescription("Upgrading the volume {name}", name=id)

    def verify(self, id):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.join_subtasks(self.run_subtask('zfs.pool.upgrade', id))
        self.dispatcher.dispatch_event('volume.changed', {
            'operation': 'update',
            'ids': [vol['id']]
        })


@description('Replaces failed disk in active volume')
@accepts(str, str, h.one_of(str, None))
class VolumeAutoReplaceTask(Task):
    @classmethod
    def early_describe(cls):
        return "Replacing failed disk in a volume"

    def describe(self, id, failed_vdev, password=None):
        return TaskDescription("Replacing the failed disk {vdev} in the volume {name}", name=id, vdev=failed_vdev)

    def verify(self, id, failed_vdev, password=None):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['zpool:{0}'.format(id)]

    def run(self, id, failed_vdev, password=None):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        def do_replace(disk, global_spare=False):
            if vol.get('key_encrypted') or vol.get('password_encrypted'):
                encryption = vol['encryption']
                if vol.get('password_encrypted'):
                    if not is_password(
                            password,
                            encryption.get('salt', ''),
                            encryption.get('hashed_password', '')
                    ):
                        raise TaskException(
                            errno.EINVAL,
                            'Password provided for volume {0} is not valid'.format(id)
                        )

                self.join_subtasks(self.run_subtask('disk.geli.init', disk['id'], {
                    'key': encryption['key'],
                    'password': password
                }))

                if encryption['slot'] is not 0:
                    self.join_subtasks(self.run_subtask('disk.geli.ukey.set', disk['id'], {
                        'key': encryption['key'],
                        'password': password,
                        'slot': 1
                    }))

                    self.join_subtasks(self.run_subtask('disk.geli.ukey.del', disk['id'], 0))

                if vol.get('providers_presence', 'NONE') != 'NONE':
                    self.join_subtasks(self.run_subtask('disk.geli.attach', disk['id'], {
                        'key': encryption['key'],
                        'password': password
                    }))

            self.join_subtasks(self.run_subtask('zfs.pool.replace', id, failed_vdev, {
                'type': 'disk',
                'path': disk['status']['data_partition_path']
            }))

            if not global_spare:
                self.join_subtasks(self.run_subtask('zfs.pool.detach', id, failed_vdev))

        empty_disks = self.dispatcher.call_sync('disk.query', [('status.empty', '=', True)])
        vdev = self.dispatcher.call_sync('zfs.pool.vdev_by_guid', id, failed_vdev)
        minsize = vdev['stats']['size']

        # First, try to find spare assigned to the pool
        spares = vol['topology'].get('spare', [])
        if spares:
            spare = spares[0]
            disk = self.dispatcher.call_sync('disk.query', [('path', '=', spare['path'])], {'single': True})
            do_replace(disk)
            return

        if self.configstore.get('storage.hotsparing.strong_match'):
            pass
        else:
            matching_disks = sorted(empty_disks, key=lambda d: d['mediasize'])
            disk = first_or_default(lambda d: d['mediasize'] >= minsize, matching_disks)
            if disk:
                self.join_subtasks(self.run_subtask('disk.format.gpt', disk['id'], 'freebsd-zfs', {'swapsize': 2048}))
                disk = self.dispatcher.call_sync('disk.query', [('id', '=', disk['id'])], {'single': True})
                do_replace(disk, True)
                return

        raise TaskException(errno.EBUSY, 'No matching disk to be used as spare found')


@description("Locks encrypted ZFS volume")
@accepts(str)
class VolumeLockTask(Task):
    @classmethod
    def early_describe(cls):
        return "Locking encrypted volume"

    def describe(self, id):
        return TaskDescription("Locking the encrypted volume {name}", name=id)

    def verify(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        if not (vol.get('key_encrypted') or vol.get('password_encrypted')):
            raise TaskException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') == 'NONE':
            raise TaskException(errno.EINVAL, 'Volume {0} does not have any unlocked providers'.format(id))

        self.dispatcher.run_hook('volume.pre_detach', {'name': id})

        with self.dispatcher.get_lock('volumes'):
            self.join_subtasks(self.run_subtask('zfs.umount', id))
            self.join_subtasks(self.run_subtask('zfs.pool.export', id))

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if vdev_conf.get('encrypted'):
                        subtasks.append(self.run_subtask(
                            'disk.geli.detach',
                            self.dispatcher.call_sync('disk.path_to_id', vdev['path'])
                        ))
                else:
                    subtasks.append(self.run_subtask(
                        'disk.geli.detach',
                        self.dispatcher.call_sync('disk.path_to_id', vdev['path'])
                    ))
            self.join_subtasks(*subtasks)

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })


@description("Imports VMs, shares and system dataset from a volume")
@accepts(
    str,
    h.enum(str, ['all', 'vms', 'shares', 'system'])
)
class VolumeAutoImportTask(Task):
    @classmethod
    def early_describe(cls):
        return "Importing services from a volume"

    def describe(self, volume, scope):
        return TaskDescription("Importing {scope} services from the volume {name}", name=volume, scope=scope)

    def verify(self, volume, scope):
        if not self.datastore.exists('volumes', ('id', '=', volume)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(volume))

        return ['zpool:{0}'.format(volume)]

    def run(self, volume, scope):
        if not self.datastore.exists('volumes', ('id', '=', volume)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(volume))

        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('id', '=', volume)], {'single': True})
            share_types = self.dispatcher.call_sync('share.supported_types')
            vm_types = ['VM']
            imported = {
                'shares': [],
                'vms': [],
                'system': []
            }

            for root, dirs, files in os.walk(vol['mountpoint']):
                for file in files:
                    config_name = re.match('(\.config-)(.*)(\.json)', file)
                    config_path = os.path.join(root, file)
                    if config_name:
                        try:
                            config = load_config(root, config_name.group(2))
                        except ValueError:
                            self.add_warning(
                                TaskWarning(
                                    errno.EINVAL,
                                    'Cannot read {0}. This file is not a valid JSON file'.format(config_path)
                                )
                            )
                            continue

                        item_type = config.get('type', '')
                        if item_type != '':

                            if scope in ['all', 'shares'] and item_type in share_types:
                                try:
                                    self.join_subtasks(self.run_subtask(
                                        'share.import',
                                        root,
                                        config.get('name', ''),
                                        item_type
                                    ))

                                    imported['shares'].append(
                                        {
                                            'path': config_path,
                                            'type': item_type,
                                            'name': config.get('name', '')
                                        }
                                    )
                                except RpcException as err:
                                    self.add_warning(
                                        TaskWarning(
                                            err.code,
                                            'Share import from {0} failed. Message: {1}'.format(
                                                config_path,
                                                err.message
                                            )
                                        )
                                    )
                                    continue
                            elif scope in ['all', 'vms'] and item_type in vm_types:
                                try:
                                    self.join_subtasks(self.run_subtask(
                                        'vm.import',
                                        config.get('name', ''),
                                        volume
                                    ))

                                    imported['vms'].append(
                                        {
                                            'type': item_type,
                                            'name': config.get('name', '')
                                        }
                                    )
                                except RpcException as err:
                                    self.add_warning(
                                        TaskWarning(
                                            err.code,
                                            'VM import from {0} failed. Message: {1}'.format(
                                                config_path,
                                                err.message
                                            )
                                        )
                                    )
                                    continue
                            elif item_type not in itertools.chain(share_types, vm_types):
                                self.add_warning(
                                    TaskWarning(
                                        errno.EINVAL,
                                        'Import from {0} failed because {1} is unsupported share/VM type'.format(
                                            config_path,
                                            item_type
                                        )
                                    )
                                )
                                continue
                        else:
                            self.add_warning(
                                TaskWarning(
                                    errno.EINVAL,
                                    'Cannot read importable type from {0}.'.format(config_path)
                                    )
                            )
                            continue

            if scope in ['all', 'system']:
                if self.dispatcher.call_sync('zfs.dataset.query', [('volume', '=', volume), ('name', '~', '.system')], {'single': True}):
                    try:
                        self.join_subtasks(self.run_subtask(
                            'system_dataset.import',
                            volume
                        ))
                        status = self.dispatcher.call_sync('system_dataset.status')
                        imported['system'].append(
                            {
                                'id': status['id'],
                                'volume': volume
                            }
                        )
                    except RpcException as err:
                        self.add_warning(
                            TaskWarning(
                                err.code,
                                'System dataset import from {0} failed. Message: {1}'.format(
                                    volume,
                                    err.message
                                )
                            )
                        )

        return imported


@description("Unlocks encrypted ZFS volume")
@accepts(str, h.one_of(str, None), h.object())
class VolumeUnlockTask(Task):
    @classmethod
    def early_describe(cls):
        return "Unlocking encrypted volume"

    def describe(self, id, password=None, params=None):
        return TaskDescription("Unlocking the encrypted volume {name}", name=id)

    def verify(self, id, password=None, params=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, password=None, params=None):
        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

            encryption = vol.get('encryption')

            if not (vol.get('key_encrypted') or vol.get('password_encrypted')):
                raise TaskException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

            if vol.get('providers_presence', 'ALL') == 'ALL':
                raise TaskException(errno.EINVAL, 'Volume {0} does not have any locked providers'.format(id))

            if encryption['hashed_password']:
                if password is None:
                    raise TaskException(
                        errno.EINVAL,
                        'Volume {0} is protected with password. Provide a valid password.'.format(id)
                    )
                if not is_password(password,
                                   encryption.get('salt', ''),
                                   encryption.get('hashed_password', '')):
                    raise TaskException(errno.EINVAL, 'Password provided for volume {0} unlock is not valid'.format(id))

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if not vdev_conf.get('encrypted'):
                        subtasks.append(self.run_subtask(
                            'disk.geli.attach',
                            self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                            {
                                'key': vol['encryption']['key'],
                                'password': password
                            }
                        ))
                else:
                    subtasks.append(self.run_subtask(
                        'disk.geli.attach',
                        self.dispatcher.call_sync('disk.path_to_id', vdev['path']),
                        {
                            'key': vol['encryption']['key'],
                            'password': password
                        }
                    ))
            self.join_subtasks(*subtasks)

            self.join_subtasks(self.run_subtask('zfs.pool.import', vol['guid'], id, params))
            self.join_subtasks(self.run_subtask(
                'zfs.update',
                id,
                {'mountpoint': {'value': vol['mountpoint']}}
            ))

            self.join_subtasks(self.run_subtask('zfs.mount', id))

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })

        self.dispatcher.run_hook('volume.post_attach', {'name': id})


@description("Generates and sets new key for encrypted ZFS volume")
@accepts(str, bool, h.one_of(str, None))
class VolumeRekeyTask(Task):
    @classmethod
    def early_describe(cls):
        return "Regenerating the keys of encrypted volume"

    def describe(self, id, key_encrypted, password=None):
        return TaskDescription("Regenerating the keys of the encrypted volume {name}", name=id)

    def verify(self, id, key_encrypted, password=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id, key_encrypted, password=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        if not (vol.get('key_encrypted') or vol.get('password_encrypted')):
            raise TaskException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise TaskException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(id))

        with self.dispatcher.get_lock('volumes'):
            vol = self.datastore.get_by_id('volumes', id)
            encryption = vol.get('encryption')
            disks = self.dispatcher.call_sync('volume.get_volume_disks', id)

            if key_encrypted:
                key = base64.b64encode(os.urandom(256)).decode('utf-8')
            else:
                key = None

            slot = 0 if encryption['slot'] is 1 else 1

            if password:
                salt, digest = get_digest(password)
            else:
                salt = None
                digest = None

            subtasks = []
            disk_ids = []
            for dname in disks:
                disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                disk_ids.append(disk_id)
                subtasks.append(self.run_subtask('disk.geli.ukey.set', disk_id, {
                    'key': key,
                    'password': password,
                    'slot': slot
                }))
            self.join_subtasks(*subtasks)

            encryption = {
                'key': key,
                'hashed_password': digest,
                'salt': salt,
                'slot': slot}

            vol['encryption'] = encryption
            vol['key_encrypted'] = key_encrypted
            vol['password_encrypted'] = True if password else False
            if password:
                vol['auto_unlock'] = False

            self.datastore.update('volumes', vol['id'], vol)

            slot = 0 if encryption['slot'] is 1 else 1

            subtasks = []
            for disk_id in disk_ids:
                subtasks.append(self.run_subtask('disk.geli.ukey.del', disk_id, slot))
            self.join_subtasks(*subtasks)

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })


@description("Creates a backup file of Master Keys of encrypted volume")
@accepts(str, str)
class VolumeBackupKeysTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating a backup of the keys of encrypted volume"

    def describe(self, id, out_path=None):
        return TaskDescription("Creating a backup of the keys of the encrypted volume {name}", name=id)

    def verify(self, id, out_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id, out_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        if not (vol.get('key_encrypted') or vol.get('password_encrypted')):
            raise TaskException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise TaskException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(id))

        if out_path is None:
            raise TaskException(errno.EINVAL, 'Output file is not specified')

        with self.dispatcher.get_lock('volumes'):
            disks = self.dispatcher.call_sync('volume.get_volume_disks', id)
            out_data = {}

            subtasks = []
            for dname in disks:
                disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                subtasks.append(self.run_subtask('disk.geli.mkey.backup', disk_id))
            output = self.join_subtasks(*subtasks)

        for result in output:
            out_data[result['disk']] = result

        password = str(uuid.uuid4())
        enc_data = fernet_encrypt(password, json.dumps(out_data).encode('utf-8'))

        with open(out_path, 'wb') as out_file:
            out_file.write(enc_data)

        return password


@description("Loads a backup file of Master Keys of encrypted volume")
@accepts(str, h.one_of(str, None), str)
class VolumeRestoreKeysTask(Task):
    @classmethod
    def early_describe(cls):
        return "Uploading the keys from backup to encrypted volume"

    def describe(self, id, password=None, in_path=None):
        return TaskDescription("Uploading the keys from backup to the encrypted volume {name}", name=id)

    def verify(self, id, password=None, in_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, password=None, in_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        if not (vol.get('key_encrypted') or vol.get('password_encrypted')):
            raise TaskException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'ALL') != 'NONE':
            raise TaskException(errno.EINVAL, 'Volume {0} cannot have any online providers'.format(id))

        if in_path is None:
            raise TaskException(errno.EINVAL, 'Input file is not specified')

        if password is None:
            raise TaskException(errno.EINVAL, 'Password is not specified')

        vol = self.datastore.get_by_id('volumes', id)
        with open(in_path, 'rb') as in_file:
            enc_data = in_file.read()

        try:
            json_data = fernet_decrypt(password, enc_data)
        except InvalidToken:
            raise TaskException(errno.EINVAL, 'Provided password do not match backup file')

        data = json.loads(json_data.decode('utf-8'), 'utf-8')

        with self.dispatcher.get_lock('volumes'):
            subtasks = []
            for dname, dgroup in get_disks(vol['topology']):
                disk = data.get(dname, None)
                if disk is None:
                    raise TaskException(errno.EINVAL, 'Disk {0} is not a part of volume {1}'.format(disk['disk'], id))

                disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)

                subtasks.append(self.run_subtask('disk.geli.mkey.restore', disk_id, disk))

            self.join_subtasks(*subtasks)


@description("Scrubs the volume")
@accepts(str)
class VolumeScrubTask(ProgressTask):
    @classmethod
    def early_describe(cls):
        return "Performing a scrub of a volume"

    def describe(self, id):
        return TaskDescription("Performing a scrub of the volume {name}", name=id)

    def verify(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def abort(self):
        self.abort_subtasks()

    def run(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.join_subtasks(self.run_subtask(
            'zfs.pool.scrub', id,
            progress_callback=self.set_progress
        ))


@description("Makes vdev in a volume offline")
@accepts(str, str)
class VolumeOfflineVdevTask(Task):
    @classmethod
    def early_describe(cls):
        return "Turning offline a disk of a volume"

    def describe(self, id, vdev_guid):
        try:
            config = self.dispatcher.call_sync('disk.get_disk_config_by_id', vdev_guid)
        except RpcException:
            config = None

        return TaskDescription(
            "Turning offline the {disk} disk of the volume {name}",
            name=id,
            disk=config['path'] if config else vdev_guid
        )

    def verify(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.join_subtasks(self.run_subtask(
            'zfs.pool.offline_disk',
            id,
            vdev_guid
        ))


@description("Makes vdev in a volume online")
@accepts(str, str)
class VolumeOnlineVdevTask(Task):
    @classmethod
    def early_describe(cls):
        return "Turning online a disk of a volume"

    def describe(self, id, vdev_guid):
        try:
            config = self.dispatcher.call_sync('disk.get_disk_config_by_id', vdev_guid)
        except RpcException:
            config = None

        return TaskDescription(
            "Turning online the {disk} disk of the volume {name}",
            name=id,
            disk=config['path'] if config else vdev_guid
        )

    def verify(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.join_subtasks(self.run_subtask(
            'zfs.pool.online_disk',
            id,
            vdev_guid
        ))


@description("Creates a dataset in an existing volume")
@accepts(h.all_of(
    h.ref('volume-dataset'),
    h.required('id', 'volume')
))
class DatasetCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating a dataset"

    def describe(self, dataset):
        return TaskDescription("Creating the dataset {name}", name=dataset['id'])

    def verify(self, dataset):
        if not self.datastore.exists('volumes', ('id', '=', dataset['volume'])):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(dataset['volume']))

        return ['zpool:{0}'.format(dataset['volume'])]

    def run(self, dataset):
        if not self.datastore.exists('volumes', ('id', '=', dataset['volume'])):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(dataset['volume']))

        props = {}
        normalize(dataset, {
            'type': 'FILESYSTEM',
            'permissions_type': 'PERM',
            'mounted': True,
            'properties': {}
        })

        if dataset['type'] == 'FILESYSTEM':
            props = {
                'org.freenas:permissions_type': {'value': dataset['permissions_type']},
                'aclmode': {'value': 'restricted' if dataset['permissions_type'] == 'ACL' else 'passthrough'}
            }

        if dataset['type'] == 'VOLUME':
            props['volsize'] = {'value': str(dataset['volsize'])}

        props.update(exclude(
            dataset['properties'],
            'used', 'usedbydataset', 'usedbysnapshots', 'usedbychildren', 'logicalused', 'logicalreferenced',
            'written', 'usedbyrefreservation', 'referenced', 'available', 'compressratio', 'refcompressratio'
        ))

        props['org.freenas:uuid'] = {'value': str(uuid.uuid4())}

        self.join_subtasks(self.run_subtask(
            'zfs.create_dataset',
            dataset['id'],
            dataset['type'],
            props
        ))

        if dataset['mounted']:
            self.join_subtasks(self.run_subtask('zfs.mount', dataset['id']))

        if dataset['permissions_type'] == 'ACL':
            fs_path = self.dispatcher.call_sync('volume.get_dataset_path', dataset['id'])
            self.join_subtasks(self.run_subtask('file.set_permissions', fs_path, {'acl': DEFAULT_ACLS}, True))

        if dataset.get('permissions'):
            path = os.path.join(VOLUMES_ROOT, dataset['id'])
            self.join_subtasks(self.run_subtask('file.set_permissions', path, dataset['permissions']))


@description("Deletes an existing Dataset from a Volume")
@accepts(str)
class DatasetDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting a dataset"

    def describe(self, id):
        return TaskDescription("Deleting the dataset {name}", name=id)

    def verify(self, id):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def run(self, id):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        deps = self.dispatcher.call_sync('zfs.dataset.get_dependencies', id)
        ds = self.dispatcher.call_sync('volume.dataset.query', [('id', '=', id)], {'single': True})
        if not ds:
            raise TaskException(errno.ENOENT, 'Dataset {0} not found', id)

        for i in deps:
            if i['type'] == 'FILESYSTEM':
                self.join_subtasks(self.run_subtask('zfs.umount', i['id']))

            self.join_subtasks(self.run_subtask('zfs.destroy', i['id']))

        try:
            self.join_subtasks(self.run_subtask('zfs.umount', id))
            self.join_subtasks(self.run_subtask('zfs.destroy', id))
        except RpcException as err:
            if err.code == errno.EBUSY:
                # Find out what's holding unmount or destroy
                files = self.dispatcher.call_sync('filesystem.get_open_files', ds['mountpoint'])
                if len(files) == 1:
                    raise TaskException(
                        errno.EBUSY,
                        'Dataset is in use by {process_name} (pid {pid}, path {path})'.format(**files[0]),
                        extra=files
                    )
                else:
                    raise TaskException(
                        errno.EBUSY,
                        'Dataset is in use by {0} processes'.format(len(files)),
                        extra=files
                    )
            else:
                raise


@description("Configures/Updates an existing Dataset's properties")
@accepts(str, h.ref('volume-dataset'))
class DatasetConfigureTask(Task):
    @classmethod
    def early_describe(cls):
        return "Configuring a dataset"

    def describe(self, id, updated_params):
        return TaskDescription("Configuring the dataset {name}", name=id)

    def verify(self, id, updated_params):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def switch_to_acl(self, path):
        fs_path = self.dispatcher.call_sync('volume.get_dataset_path', path)
        self.join_subtasks(
            self.run_subtask('zfs.update', path, {
                'aclmode': {'value': 'restricted'},
                'org.freenas:permissions_type': {'value': 'ACL'}
            }),
            self.run_subtask('file.set_permissions', fs_path, {
                'acl': DEFAULT_ACLS
            }, True)
        )

    def switch_to_chmod(self, path):
        self.join_subtasks(self.run_subtask('zfs.update', path, {
            'aclmode': {'value': 'passthrough'},
            'org.freenas:permissions_type': {'value': 'PERM'}
        }))

    def run(self, id, updated_params):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        pool_name, _, _ = id.partition('/')
        ds = self.dispatcher.call_sync('zfs.dataset.query', [('id', '=', id)], {'single': True})

        if not ds:
            raise TaskException(errno.ENOENT, 'Dataset {0} not found'.format(id))

        if 'id' in updated_params:
            self.join_subtasks(self.run_subtask('zfs.rename', ds['id'], updated_params['id']))
            ds['id'] = updated_params['id']

        if 'properties' in updated_params:
            props = exclude(
                updated_params['properties'],
                'used', 'usedbydataset', 'usedbysnapshots', 'usedbychildren', 'logicalused',
                'logicalreferenced', 'written', 'usedbyrefreservation', 'referenced', 'available',
                'casesensitivity', 'compressratio', 'refcompressratio'
            )
            self.join_subtasks(self.run_subtask('zfs.update', ds['id'], props))

        if 'permissions_type' in updated_params:
            oldtyp = q.get(ds, 'properties.org\\.freenas:permissions_type.value')
            typ = updated_params['permissions_type']

            if oldtyp != 'ACL' and typ == 'ACL':
                self.switch_to_acl(ds['id'])

            if oldtyp != 'PERM' and typ == 'PERM':
                self.switch_to_chmod(ds['id'])

        if 'permissions' in updated_params:
            fs_path = os.path.join(VOLUMES_ROOT, ds['id'])
            self.join_subtasks(self.run_subtask('file.set_permissions', fs_path, updated_params['permissions']))

        if 'mounted' in updated_params:
            if updated_params['mounted']:
                self.join_subtasks(self.run_subtask('zfs.mount', ds['id']))
            else:
                self.join_subtasks(self.run_subtask('zfs.umount', ds['id']))


@description("Mounts target readonly dataset")
@accepts(str, str)
class DatasetTemporaryMountTask(Task):
    @classmethod
    def early_describe(cls):
        return "Mounting a dataset"

    def describe(self, id, dest):
        return TaskDescription("Mounting the dataset {name}", name=id)

    def verify(self, id, dest):
        try:
            return ['zpool:{0}'.format(self.dispatcher.call_sync('volume.decode_path', id)[0])]
        except RpcException:
            return ['system']

    def run(self, id, dest):
        ds_ro = self.dispatcher.call_sync(
            'zfs.dataset.query',
            [('id', '=', id)],
            {'select': 'properties.readonly.rawvalue'}
        )
        if not ds_ro:
            raise TaskException(errno.ENOENT, 'Dataset {0} does not exist'.format(id))
        if not ds_ro[0]:
            raise TaskException(errno.EINVAL, 'Only readonly datasets can have temporary mountpoints')

        try:
            bsd.nmount(source=id, fspath=dest, fstype='zfs')
        except FileNotFoundError:
            raise TaskException(errno.EACCES, 'Location {0} does not exist'.format(dest))
        except OSError:
            raise TaskException(errno.EACCES, 'Cannot mount {0} under {1}'.format(id, dest))


@description("Unmounts target readonly dataset")
@accepts(str)
class DatasetTemporaryUmountTask(Task):
    @classmethod
    def early_describe(cls):
        return "Unmounting a dataset"

    def describe(self, id):
        return TaskDescription("Unmounting the dataset {name}", name=id)

    def verify(self, id):
        try:
            return ['zpool:{0}'.format(self.dispatcher.call_sync('volume.decode_path', id)[0])]
        except RpcException:
            return ['system']

    def run(self, id):
        ds_ro = list(self.dispatcher.call_sync(
            'zfs.dataset.query',
            [('id', '=', id)],
            {'select': 'properties.readonly.rawvalue'}
        ))
        if not ds_ro:
            raise TaskException(errno.ENOENT, 'Dataset {0} does not exist'.format(id))
        if not ds_ro[0]:
            raise TaskException(errno.EINVAL, 'Only readonly datasets can be unmounted')

        self.join_subtasks(self.run_subtask('zfs.umount', id))


@description("Creates a snapshot")
@accepts(
    h.all_of(
        h.ref('volume-snapshot'),
        h.required('dataset', 'name'),
        h.forbidden('id')
    ),
    bool
)
class SnapshotCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating a snapshot"

    def describe(self, snapshot, recursive=False):
        return TaskDescription("Creating the snapshot {name}", name=snapshot['name'])

    def verify(self, snapshot, recursive=False):
        return ['zfs:{0}'.format(snapshot['dataset'])]

    def run(self, snapshot, recursive=False):
        normalize(snapshot, {
            'replicable': True,
            'lifetime': None,
            'hidden': False
        })

        self.join_subtasks(self.run_subtask(
            'zfs.create_snapshot',
            snapshot['dataset'],
            snapshot['name'],
            recursive,
            {
                'org.freenas:replicable': {'value': 'yes' if snapshot['replicable'] else 'no'},
                'org.freenas:hidden': {'value': 'yes' if snapshot['hidden'] else 'no'},
                'org.freenas:lifetime': {'value': str(snapshot['lifetime'] or 'no')},
                'org.freenas:uuid': {'value': str(uuid.uuid4())}
            }
        ))


@description("Deletes the specified snapshot")
@accepts(str)
class SnapshotDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting a snapshot"

    def describe(self, id):
        return TaskDescription("Deleting the snapshot {name}", name=id)

    def verify(self, id):
        pool, ds, snap = split_snapshot_name(id)
        return ['zfs:{0}'.format(ds)]

    def run(self, id):
        pool, ds, snap = split_snapshot_name(id)
        self.join_subtasks(self.run_subtask(
            'zfs.delete_snapshot',
            ds,
            snap
        ))


@description("Updates configuration of specified snapshot")
@accepts(str, h.all_of(
    h.ref('volume-snapshot')
))
class SnapshotConfigureTask(Task):
    @classmethod
    def early_describe(cls):
        return "Configuring a snapshot"

    def describe(self, id, updated_params):
        return TaskDescription("Configuring the snapshot {name}", name=id)

    def verify(self, id, updated_params):
        pool, ds, snap = split_snapshot_name(id)
        return ['zfs:{0}'.format(ds)]

    def run(self, id, updated_params):
        pool, ds, snap = split_snapshot_name(id)
        params = {}

        if 'id' in updated_params:
            self.join_subtasks(self.run_subtask('zfs.rename', id, updated_params['id']))
            id = updated_params['id']

        if 'name' in updated_params:
            new_id = '{0}@{1}'.format(ds, updated_params['name'])
            self.join_subtasks(self.run_subtask('zfs.rename', id, new_id))
            id = new_id

        if 'lifetime' in updated_params:
            params['org.freenas:lifetime'] = {'value': str(updated_params['lifetime'] or 'no')}

        if 'replicable' in updated_params:
            params['org.freenas:replicable'] = {'value': 'yes' if updated_params['replicable'] else 'no'}

        if 'hidden' in updated_params:
            params['org.freenas:hidden'] = {'value': 'yes' if updated_params['hidden'] else 'no'}

        self.join_subtasks(self.run_subtask('zfs.update', id, params))


@description("Cloning the specified snapshot into new name")
@accepts(str, str)
class SnapshotCloneTask(Task):
    @classmethod
    def early_describe(cls):
        return "Cloning a snapshot"

    def describe(self, name, new_name):
        return TaskDescription("Cloning the snapshot {name}", name=name)

    def verify(self, name, new_name):
        pool, ds, snap = split_snapshot_name(name)
        return ['zfs:{0}'.format(ds)]

    def run(self, name, new_name):
        self.join_subtasks(self.run_subtask(
            'zfs.clone',
            name,
            new_name
        ))


@description("Returns filesystem to the specified snapshot state")
@accepts(str, bool)
class SnapshotRollbackTask(Task):
    @classmethod
    def early_describe(cls):
        return "Returning filesystem to a snapshot state"

    def describe(self, name, force=False):
        return TaskDescription("Returning filesystem to the snapshot {name}", name=name)

    def verify(self, name, force=False):
        pool, ds, snap = split_snapshot_name(name)
        return ['zfs:{0}'.format(ds)]

    def run(self, name, force=False):
        self.join_subtasks(self.run_subtask(
            'zfs.rollback',
            name,
            force
        ))


def compare_vdevs(vd1, vd2):
    if vd1 is None or vd2 is None:
        return False

    if vd1['guid'] != vd2['guid']:
        return False

    if vd1['type'] != vd2['type']:
        return False

    if vd1.get('path') != vd2.get('path'):
        return False

    for c1, c2 in itertools.zip_longest(vd1.get('children', []), vd2.get('children', [])):
        if not compare_vdevs(c1, c2):
            return False

    return True


def iterate_vdevs(topology):
    for name, grp in list(topology.items()):
        for vdev in grp:
            if vdev['type'] == 'disk':
                yield vdev, name
                continue

            if 'children' in vdev:
                for child in vdev['children']:
                    yield child, name


def disk_spec_to_path(dispatcher, ident):
    return dispatcher.call_sync(
        'disk.query',
        [
            ('or', [('path', '=', ident), ('name', '=', ident), ('id', '=', ident)]),
            ('online', '=', True)
        ],
        {'single': True, 'select': 'path'}
    )


def get_disks(topology):
    for vdev, gname in iterate_vdevs(topology):
        yield vdev['path'], gname


def get_disk_gptid(dispatcher, disk):
    id = dispatcher.call_sync('disk.path_to_id', disk)
    config = dispatcher.call_sync('disk.get_disk_config_by_id', id)
    return config.get('data_partition_path', disk)


def convert_topology_to_gptids(dispatcher, topology):
    topology = copy.deepcopy(topology)
    for vdev, _ in iterate_vdevs(topology):
        vdev['path'] = get_disk_gptid(dispatcher, vdev['path'])

    return topology


def split_snapshot_name(name):
    ds, _, snap = name.partition('@')
    pool = ds.split('/', 1)[0]
    return pool, ds, snap


def get_digest(password, salt=None):
    if salt is None:
        salt = base64.b64encode(os.urandom(256)).decode('utf-8')

    hmac = hashlib.pbkdf2_hmac('sha256', bytes(str(password), 'utf-8'), salt.encode('utf-8'), 200000)
    digest = base64.b64encode(hmac).decode('utf-8')
    return salt, digest


def is_password(password, salt, digest):
    return get_digest(password, salt)[1] == digest


def fernet_encrypt(password, in_data):
    digest = get_digest(password, '')[1]
    f = Fernet(digest)
    return f.encrypt(in_data)


def fernet_decrypt(password, in_data):
    digest = get_digest(password, '')[1]
    f = Fernet(digest)
    return f.decrypt(in_data)


def register_property_schemas(plugin):
    VOLUME_PROPERTIES = {
        'size': {
            'type': 'integer',
            'readOnly': True
        },
        'capacity': {
            'type': 'integer',
            'readOnly': True
        },
        'health': {
            'type': 'string',
            'enum': ['ONLINE', 'DEGRADED', 'FAULTED', 'OFFLINE', 'REMOVED', 'UNAVAIL']
        },
        'version': {
            'type': ['integer', 'null'],
            'readOnly': True
        },
        'delegation': {
            'type': 'boolean'
        },
        'failmode': {
            'type': 'string',
            'enum': ['wait', 'continue', 'panic'],
        },
        'autoreplace': {
            'type': 'boolean'
        },
        'dedupratio': {
            'type': 'string',
            'readOnly': True
        },
        'free': {
            'type': 'integer',
            'readOnly': True
        },
        'allocated': {
            'type': 'integer',
            'readOnly': True
        },
        'readonly': {
            'type': 'boolean'
        },
        'comment': {
            'type': 'string'
        },
        'expandsize': {
            'type': 'integer',
            'readOnly': True
        },
        'fragmentation': {
            'type': 'string',
            'readOnly': True
        },
        'leaked': {
            'type': 'integer',
            'readOnly': True
        }
    }

    DATASET_PROPERTIES = {
        'used': {
            'type': 'integer',
            'readOnly': True
        },
        'available': {
            'type': 'integer',
            'readOnly': True
        },
        'compression': {
            'type': ['string', 'null'],
            'enum': [
                'on', 'off', 'lzjb', 'zle', 'lz4', 'gzip', 'gzip-1', 'gzip-2', 'gzip-3',
                'gzip-4', 'gzip-5', 'gzip-6', 'gzip-7', 'gzip-8', 'gzip-9', None
            ]
        },
        'atime': {
            'type': ['boolean', 'null']
        },
        'dedup': {
            'type': ['string', 'null'],
            'enum': [
                'on', 'off', 'verify', None
            ]
        },
        'quota': {
            'type': ['integer', 'null']
        },
        'refquota': {
            'type': ['integer', 'null'],
        },
        'reservation': {
            'type': ['integer', 'null'],
        },
        'refreservation': {
            'type': ['integer', 'null'],
        },
        'casesensitivity': {
            'type': ['string', 'null'],
            'enum': ['sensitive', 'insensitive', 'mixed', None]
        },
        'volsize': {
            'type': 'integer',
            'readOnly': True
        },
        'volblocksize': {
            'type': 'integer',
            'enum': [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072]
        },
        'refcompressratio': {
            'type': 'string',
            'readOnly': True
        },
        'numclones': {
            'type': 'string',
            'readOnly': True
        },
        'compressratio': {
            'type': 'string',
            'readOnly': True
        },
        'written': {
            'type': 'integer',
            'readOnly': True
        },
        'referenced': {
            'type': 'integer',
            'readOnly': True
        },
        'readonly': {
            'type': 'boolean',
            'readOnly': True
        },
        'usedbyrefreservation': {
            'type': 'integer',
            'readOnly': True
        },
        'usedbysnapshots': {
            'type': 'integer',
            'readOnly': True
        },
        'usedbydataset': {
            'type': 'integer',
            'readOnly': True
        },
        'usedbychildren': {
            'type': 'integer',
            'readOnly': True
        },
        'logicalused': {
            'type': 'integer',
            'readOnly': True
        },
        'logicalreferenced': {
            'type': 'integer',
            'readOnly': True
        }
    }

    SNAPSHOT_PROPERTIES = {
        'used': {
            'type': 'integer',
            'readOnly': True
        },
        'referenced': {
            'type': 'integer',
            'readOnly': True
        },
        'compressratio': {
            'type': 'string',
            'readOnly': True
        },
        'clones': {
            'type': 'integer',
            'readOnly': True
        },
        'creation': {
            'type': 'datetime',
            'readOnly': True
        }
    }

    for i, props in {
        'volume': VOLUME_PROPERTIES,
        'volume-dataset': DATASET_PROPERTIES,
        'volume-snapshot': SNAPSHOT_PROPERTIES
    }.items():
        for name, prop in props.items():
            plugin.register_schema_definition('{0}-property-{1}-value'.format(i, name), prop)
            plugin.register_schema_definition('{0}-property-{1}'.format(i, name), {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'source': {'$ref': 'volume-property-source'},
                    'rawvalue': {'type': 'string', 'readOnly': True},
                    'value': {'type': ['string', 'null']},
                    'parsed': {'$ref': '{0}-property-{1}-value'.format(i, name)}
                }
            })

        plugin.register_schema_definition('{0}-properties'.format(i), {
            'type': 'object',
            'additionalProperties': False,
            'properties': {name: {'$ref': '{0}-property-{1}'.format(i, name)} for name in props},
        })


def _depends():
    return ['DevdPlugin', 'ZfsPlugin', 'AlertPlugin', 'DiskPlugin']


def _init(dispatcher, plugin):
    boot_pool = dispatcher.call_sync('zfs.pool.get_boot_pool')

    def convert_snapshot(snapshot):
        dataset, _, name = snapshot['name'].partition('@')
        pool = dataset.partition('/')[0]
        lifetime = None

        if pool == boot_pool['id']:
            return None

        try:
            lifetime = int(q.get(snapshot, 'properties.org\\.freenas:lifetime.value'))
        except (ValueError, TypeError):
            pass

        return {
            'id': snapshot['name'],
            'volume': pool,
            'dataset': dataset,
            'name': name,
            'lifetime': lifetime,
            'replicable': yesno_to_bool(q.get(snapshot, 'properties.org\\.freenas:replicable.value')),
            'hidden': yesno_to_bool(q.get(snapshot, 'properties.org\\.freenas:hidden.value')),
            'properties': include(
                snapshot['properties'],
                'used', 'referenced', 'compressratio', 'clones', 'creation'
            ),
            'holds': snapshot['holds']
        }

    def convert_dataset(ds):
        perms = None

        if ds['pool'] == boot_pool['id']:
            return None

        if ds['mountpoint']:
            try:
                perms = dispatcher.call_sync('filesystem.stat', ds['mountpoint'])
            except RpcException:
                pass

        temp_mountpoint = None
        if q.get(ds, 'properties.readonly.parsed') and q.get(ds, 'properties.mounted.parsed'):
            for mnt in bsd.getmntinfo():
                if mnt.source == ds['name'] and mnt.dest != q.get(ds, 'properties.mountpoint.parsed'):
                    temp_mountpoint = mnt.dest
                    break

        return {
            'id': ds['name'],
            'name': ds['name'],
            'rname': 'zfs:{0}'.format(ds['id']),
            'volume': ds['pool'],
            'type': ds['type'],
            'mountpoint': q.get(ds, 'properties.mountpoint.value'),
            'temp_mountpoint': temp_mountpoint,
            'mounted': q.get(ds, 'properties.mounted.parsed'),
            'volsize': q.get(ds, 'properties.volsize.parsed'),
            'properties': include(
                ds['properties'],
                'used', 'available', 'compression', 'atime', 'dedup',
                'quota', 'refquota', 'reservation', 'refreservation',
                'casesensitivity', 'volsize', 'volblocksize', 'refcompressratio',
                'numclones', 'compressratio', 'written', 'referenced',
                'usedbyrefreservation', 'usedbysnapshots', 'usedbydataset',
                'usedbychildren', 'logicalused', 'logicalreferenced', 'readonly'
            ),
            'permissions_type': q.get(ds, 'properties.org\\.freenas:permissions_type.value'),
            'permissions': perms['permissions'] if perms else None
        }

    def on_pool_change(args):
        if args['operation'] == 'delete':
            for i in args['ids']:
                with dispatcher.get_lock('volumes'):
                    volume = dispatcher.call_sync('volume.query', [('id', '=', i)], {'single': True})
                    encrypted = volume.get('key_encrypted') or volume.get('password_encrypted')
                    if volume and not encrypted:
                        logger.info('Volume {0} is going away'.format(volume['id']))
                        dispatcher.datastore.delete('volumes', volume['id'])
                        dispatcher.dispatch_event('volume.changed', {
                            'operation': 'delete',
                            'ids': [volume['id']]
                        })

        if args['operation'] in ('create', 'update'):
            entities = filter(lambda i: i['guid'] != boot_pool['guid'], args['entities'])
            for i in entities:
                if args['operation'] == 'update':
                    volume = dispatcher.datastore.get_one('volumes', ('id', '=', i['name']))
                    if volume:
                        dispatcher.dispatch_event('volume.changed', {
                            'operation': 'update',
                            'ids': [volume['id']]
                        })
                    continue

                with dispatcher.get_lock('volumes'):
                    try:
                        dispatcher.datastore.insert('volumes', {
                            'id': i['name'],
                            'guid': str(i['guid']),
                            'type': 'zfs',
                            'attributes': {},
                            'key_encrypted': False,
                            'password_encrypted': False,
                            'encryption': {
                                'key': None,
                                'hashed_password': None,
                                'salt': None,
                                'slot': None
                            }
                        })
                    except DuplicateKeyException:
                        # already inserted by task
                        continue

                    logger.info('New volume {0} <{1}>'.format(i['name'], i['guid']))

                    # Set correct mountpoint
                    dispatcher.call_task_sync('zfs.update', i['name'], {
                        'mountpoint': {'value': os.path.join(VOLUMES_ROOT, i['name'])}
                    })

                    if q.get(i, 'properties.altroot.source') != 'DEFAULT':
                        # Ouch. That pool is created or imported with altroot.
                        # We need to export and reimport it to remove altroot property
                        dispatcher.call_task_sync('zfs.pool.export', i['name'])
                        dispatcher.call_task_sync('zfs.pool.import', i['guid'], i['name'])

                    dispatcher.dispatch_event('volume.changed', {
                        'operation': 'create',
                        'ids': [i['name']]
                    })

    def on_snapshot_change(args):
        snapshots.propagate(args, callback=convert_snapshot)

    def on_dataset_change(args):
        datasets.propagate(args, callback=convert_dataset)

    def on_vdev_remove(args):
        dispatcher.call_sync('alert.emit', {
            'name': 'volume.disk_removed',
            'description': 'Some disk was removed from the system',
            'severity': 'WARNING'
        })

    def on_vdev_state_change(args):
        guid = args['guid']
        volume = dispatcher.call_sync('volume.query', [('guid', '=', guid)], {'single': True})
        if not volume:
            return

        if args['vdev_guid'] == guid:
            # Ignore root vdev state changes
            return

        if args['vdev_state'] in ('FAULTED', 'REMOVED'):
            logger.warning('Vdev {0} of pool {1} is now in {2} state - attempting to replace'.format(
                args['vdev_guid'],
                volume['id'],
                args['vdev_state']
            ))

            dispatcher.call_task_sync('volume.autoreplace', volume['id'], args['vdev_guid'])

    def scrub_snapshots():
        interval = dispatcher.configstore.get('middleware.snapshot_scrub_interval')
        while True:
            gevent.sleep(interval)
            ts = int(time.time())
            for snap in dispatcher.call_sync('volume.snapshot.query'):
                creation = int(q.get(snap, 'properties.creation.rawvalue'))
                lifetime = snap['lifetime']

                if lifetime is None:
                    continue

                if creation + lifetime <= ts:
                    dispatcher.call_task_sync('volume.snapshot.delete', snap['id'])

    plugin.register_schema_definition('volume', {
        'type': 'object',
        'title': 'volume',
        'additionalProperties': False,
        'readOnly': True,
        'properties': {
            'id': {'type': 'string'},
            'guid': {
                'type': 'string',
                'readOnly': True
            },
            'type': {
                'type': 'string',
                'enum': ['zfs']
            },
            'rname': {'type': 'string'},
            'topology': {'$ref': 'zfs-topology'},
            'scan': {'$ref': 'zfs-scan'},
            'key_encrypted': {'type': 'boolean'},
            'password_encrypted': {'type': 'boolean'},
            'encryption': {'$ref': 'volume-encryption'},
            'auto_unlock': {
                'oneOf': [
                    {'type': 'boolean'},
                    {'type': 'null'}
                ]
            },
            'providers_presence': {
                'oneOf': [
                    {'$ref': 'volume-providerspresence'},
                    {'type': 'null'}
                ],
                'readOnly': True
            },
            'properties': {'$ref': 'volume-properties', 'readOnly': True},
            'attributes': {'type': 'object'},
            'params': {
                'type': ['object', 'null'],
                'properties': {
                    'mount': {'type': 'boolean'},
                    'mountpoint': {'type': 'string'},
                    'blocksize': {'type': 'number'},
                    'swapsize': {'type': 'number'}
                }
            }
        }
    })

    plugin.register_schema_definition('volume-providerspresence', {
        'type': 'string',
        'enum': ['ALL', 'PART', 'NONE']
    })

    plugin.register_schema_definition('volume-property-source', {
        'type': 'string',
        'enum': ['NONE', 'DEFAULT', 'LOCAL', 'INHERITED']
    })

    plugin.register_schema_definition('volume-encryption', {
        'type': 'object',
        'readOnly': True,
        'properties': {
            'key': {'type': ['string', 'null']},
            'hashed_password': {'type': ['string', 'null']},
            'salt': {'type': ['string', 'null']},
            'slot': {'type': ['integer', 'null']}
        }
    })

    plugin.register_schema_definition('volume-dataset', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'rname': {'type': 'string'},
            'volume': {'type': 'string'},
            'mountpoint': {
                'type': ['string', 'null'],
                'readOnly': True
            },
            'temp_mountpoint': {
                'type': ['string', 'null'],
                'readOnly': True
            },
            'mounted': {'type': 'boolean'},
            'type': {'allOf': [
                {'$ref': 'volume-dataset-type'},
                {'readOnly': True}
            ]},
            'volsize': {'type': ['integer', 'null']},
            'properties': {'$ref': 'volume-dataset-properties'},
            'permissions': {'$ref': 'permissions'},
            'permissions_type': {'$ref': 'volume-dataset-permissionstype'}
        }
    })

    plugin.register_schema_definition('volume-dataset-type', {
        'type': 'string',
        'enum': ['FILESYSTEM', 'VOLUME'],
    })

    plugin.register_schema_definition('volume-dataset-permissionstype', {
        'type': 'string',
        'enum': ['PERM', 'ACL']
    })

    plugin.register_schema_definition('volume-snapshot', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'volume': {'type': 'string'},
            'dataset': {'type': 'string'},
            'name': {'type': 'string'},
            'replicable': {'type': 'boolean'},
            'hidden': {'type': 'boolean'},
            'lifetime': {'type': ['integer', 'null']},
            'properties': {'$ref': 'volume-snapshot-properties'},
            'holds': {'type': 'object'}
        }
    })

    plugin.register_schema_definition('volume-disk-label', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'volume_id': {'type': 'string'},
            'volume_guid': {'type': 'string'},
            'vdev_guid': {'type': 'string'},
            'hostname': {'type': 'string'},
            'hostid': {'type': ['integer', 'null']}
        }
    })

    plugin.register_schema_definition('disks-allocation', {
        'type': 'object',
        'additionalProperties': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'type': {'$ref': 'disks-allocation-type'},
                'name': {'type': 'string'}
            }
        }
    })

    plugin.register_schema_definition('disks-allocation-type', {
        'type': 'string',
        'enum': ['BOOT', 'VOLUME', 'EXPORTED_VOLUME', 'ISCSI'],
    })

    plugin.register_schema_definition('importable-disk', {
        'type': 'object',
        'properties': {
            'path': {'type': 'string'},
            'fstype': {'type': 'string'},
            'size': {'type': 'integer'},
            'label': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('volume-import-params', {
        'type': 'object',
        'properties': {
            'key': {'type': ['string', 'null']},
            'disks': {
                'type': 'array',
                'items': {'type': 'string'}
            }
        }
    })

    plugin.register_schema_definition('volume-vdev-recommendation', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'drives': {'type': 'integer'},
            'type': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('volume-vdev-recommendations', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'storage': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'storage': {'$ref': 'volume-vdev-recommendation'},
                    'redundancy': {'$ref': 'volume-vdev-recommendation'},
                    'speed': {'$ref': 'volume-vdev-recommendation'},
                }
            },
            'redundancy': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'storage': {'$ref': 'volume-vdev-recommendation'},
                    'redundancy': {'$ref': 'volume-vdev-recommendation'},
                    'speed': {'$ref': 'volume-vdev-recommendation'},
                }
            },
            'speed': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'storage': {'$ref': 'volume-vdev-recommendation'},
                    'redundancy': {'$ref': 'volume-vdev-recommendation'},
                    'speed': {'$ref': 'volume-vdev-recommendation'},
                }
            },
        }
    })

    register_property_schemas(plugin)

    plugin.register_provider('volume', VolumeProvider)
    plugin.register_provider('volume.dataset', DatasetProvider)
    plugin.register_provider('volume.snapshot', SnapshotProvider)
    plugin.register_task_handler('volume.create', VolumeCreateTask)
    plugin.register_task_handler('volume.create_auto', VolumeAutoCreateTask)
    plugin.register_task_handler('volume.delete', VolumeDestroyTask)
    plugin.register_task_handler('volume.import', VolumeImportTask)
    plugin.register_task_handler('volume.import_disk', VolumeDiskImportTask)
    plugin.register_task_handler('volume.autoimport', VolumeAutoImportTask)
    plugin.register_task_handler('volume.export', VolumeDetachTask)
    plugin.register_task_handler('volume.update', VolumeUpdateTask)
    plugin.register_task_handler('volume.upgrade', VolumeUpgradeTask)
    plugin.register_task_handler('volume.autoreplace', VolumeAutoReplaceTask)
    plugin.register_task_handler('volume.lock', VolumeLockTask)
    plugin.register_task_handler('volume.unlock', VolumeUnlockTask)
    plugin.register_task_handler('volume.rekey', VolumeRekeyTask)
    plugin.register_task_handler('volume.keys.backup', VolumeBackupKeysTask)
    plugin.register_task_handler('volume.keys.restore', VolumeRestoreKeysTask)
    plugin.register_task_handler('volume.scrub', VolumeScrubTask)
    plugin.register_task_handler('volume.vdev.online', VolumeOnlineVdevTask)
    plugin.register_task_handler('volume.vdev.offline', VolumeOfflineVdevTask)
    plugin.register_task_handler('volume.dataset.create', DatasetCreateTask)
    plugin.register_task_handler('volume.dataset.delete', DatasetDeleteTask)
    plugin.register_task_handler('volume.dataset.update', DatasetConfigureTask)
    plugin.register_task_handler('volume.dataset.temporary.mount', DatasetTemporaryMountTask)
    plugin.register_task_handler('volume.dataset.temporary.umount', DatasetTemporaryUmountTask)
    plugin.register_task_handler('volume.snapshot.create', SnapshotCreateTask)
    plugin.register_task_handler('volume.snapshot.delete', SnapshotDeleteTask)
    plugin.register_task_handler('volume.snapshot.update', SnapshotConfigureTask)
    plugin.register_task_handler('volume.snapshot.clone', SnapshotCloneTask)
    plugin.register_task_handler('volume.snapshot.rollback', SnapshotRollbackTask)

    plugin.register_hook('volume.pre_destroy')
    plugin.register_hook('volume.pre_detach')
    plugin.register_hook('volume.pre_create')
    plugin.register_hook('volume.post_attach')

    plugin.register_hook('volume.pre_rename')
    plugin.register_hook('volume.post_rename')

    plugin.register_event_handler('entity-subscriber.zfs.pool.changed', on_pool_change)
    plugin.register_event_handler('fs.zfs.vdev.removed', on_vdev_remove)
    plugin.register_event_handler('fs.zfs.vdev.state_changed', on_vdev_state_change)

    plugin.register_event_type('volume.changed')
    plugin.register_event_type('volume.dataset.changed')
    plugin.register_event_type('volume.snapshot.changed')

    for vol in dispatcher.call_sync('volume.query'):
        if vol.get('providers_presence', 'ALL') == 'NONE':
            if vol.get('auto_unlock') and vol.get('key_encrypted') and not vol.get('password_encrypted'):
                dispatcher.call_task_sync('volume.unlock', vol['id'])
            else:
                continue

        try:
            dispatcher.call_task_sync('zfs.mount', vol['id'], True)

            # XXX: check mountpoint property and correct if needed
        except TaskException as err:
            if err.code != errno.EBUSY:
                logger.warning('Cannot mount volume {0}: {1}'.format(vol['id'], str(err)))

    for pool in dispatcher.call_sync('zfs.pool.query'):
        if dispatcher.datastore.exists('volumes', ('id', '=', pool['id'])):
            continue

        if pool['guid'] == boot_pool['guid']:
            continue

        logger.info('New volume {0} <{1}>'.format(pool['name'], pool['guid']))
        dispatcher.datastore.insert('volumes', {
            'id': pool['name'],
            'guid': str(pool['guid']),
            'type': 'zfs',
            'attributes': {}
        })

    global snapshots
    snapshots = EventCacheStore(dispatcher, 'volume.snapshot')
    snapshots.populate(dispatcher.call_sync('zfs.snapshot.query'), callback=convert_snapshot)
    snapshots.ready = True
    plugin.register_event_handler(
        'entity-subscriber.zfs.snapshot.changed',
        on_snapshot_change
    )

    global datasets
    datasets = EventCacheStore(dispatcher, 'volume.dataset')
    datasets.populate(dispatcher.call_sync('zfs.dataset.query'), callback=convert_dataset)
    datasets.ready = True
    plugin.register_event_handler(
        'entity-subscriber.zfs.dataset.changed',
        on_dataset_change
    )

    gevent.spawn(scrub_snapshots)
