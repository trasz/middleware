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
from task import Provider, Task, ProgressTask, MasterProgressTask, TaskException, TaskWarning, VerifyException, query
from freenas.dispatcher.rpc import (
    RpcException, description, accepts, returns, private, SchemaHelper as h, generator
)
from utils import first_or_default, load_config
from datastore import DuplicateKeyException
from freenas.utils import include, exclude, normalize, chunks, yesno_to_bool
from freenas.utils.query import wrap
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

SNAPSHOT_SCRUB_INTERVAL = 300
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
            if pool['properties.version.value'] != '-':
                return False

            for feat in pool['features']:
                if feat['state'] == 'DISABLED':
                    return False

            return True

        def extend(vol):
            config = wrap(self.dispatcher.call_sync('zfs.pool.query', [('id', '=', vol['id'])], {'single': True}))
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
                        'description': config.get('root_dataset.properties.org\\.freenas:description.value'),
                        'mountpoint': config['root_dataset.properties.mountpoint.value'],
                        'upgraded': is_upgraded(config),
                    })

            encrypted = vol.get('encrypted', False)
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

        return self.datastore.query_stream('volumes', *(filter or []), callback=extend, **(params or {}))

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

        for disk in wrap(self.dispatcher.call_sync('disk.query', [('path', 'in', self.get_available_disks())])):
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

            for part in disk['status.partitions']:
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
    @returns(h.tuple(str, str, str))
    def decode_path(self, path):
        path = os.path.normpath(path)[1:]
        tokens = path.split(os.sep)

        if tokens[0] != VOLUMES_ROOT[1:]:
            raise RpcException(errno.EINVAL, 'Invalid path')

        volname = tokens[1]
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', volname)], {'single': True})
        if vol:
            datasets = self.dispatcher.call_sync('volume.dataset.query', [('volume', '=', volname)], {'select': 'id'})
        else:
            raise RpcException(errno.ENOENT, "Volume '{0}' does not exist".format(volname))
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

        if vol.get('encrypted', False) is False or vol.get('providers_presence', 'NONE') != 'NONE':
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
            if boot_disk in disks:
                ret[boot_disk] = {'type': 'BOOT'}

        for vol in self.dispatcher.call_sync('volume.query'):
            if vol['status'] == 'UNAVAIL':
                continue

            for dev in self.dispatcher.call_sync('volume.get_volume_disks', vol['id']):
                if dev in disks:
                    ret[dev] = {
                        'type': 'VOLUME',
                        'name': vol['id']
                    }

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

    @description("Describes the various capacibilities of a Volumes given" +
                 "What type of Volume it is (example call it with 'zfs'")
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


class DatasetProvider(Provider):
    @query('volume-dataset')
    @generator
    def query(self, filter=None, params=None):
        return datasets.query(*(filter or []), **(params or {}))


class SnapshotProvider(Provider):
    @query('volume-snapshot')
    @generator
    def query(self, filter=None, params=None):
        return snapshots.query(*(filter or []), **(params or {}))


@description("Creates new volume")
@accepts(h.ref('volume'), h.one_of(str, None))
class VolumeCreateTask(ProgressTask):
    def verify(self, volume, password=None):
        if self.datastore.exists('volumes', ('id', '=', volume['id'])):
            raise VerifyException(errno.EEXIST, 'Volume with same name already exists')

        return ['disk:{0}'.format(i) for i, _ in get_disks(volume['topology'])]

    def run(self, volume, password=None):
        name = volume['id']
        type = volume.get('type', 'zfs')
        params = volume.get('params') or {}
        mount = params.get('mount', True)
        mountpoint = params.pop(
            'mountpoint',
            os.path.join(VOLUMES_ROOT, volume['id'])
        )
        encryption = params.pop('encryption', False)

        self.dispatcher.run_hook('volume.pre_create', {'name': name})
        if encryption:
            key = base64.b64encode(os.urandom(64)).decode('utf-8')
            if password is not None:
                salt, digest = get_digest(password)
            else:
                salt = None
                digest = None
        else:
            key = None
            password = None
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

        if encryption:
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
                name, name,
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
                    'key': key if key else None,
                    'hashed_password': digest,
                    'salt': salt,
                    'slot': 0 if key else None},
                'encrypted': True if key else False,
                'attributes': volume.get('attributes', {})
            })

        self.set_progress(90)
        self.dispatcher.dispatch_event('volume.changed', {
            'operation': 'create',
            'ids': [id]
        })


@description("Creates new volume and automatically guesses disks layout")
@accepts(str, str, str, h.array(str), h.array(str), h.array(str), h.one_of(bool, None), h.one_of(str, None))
class VolumeAutoCreateTask(Task):
    def verify(self, name, type, layout, disks, cache_disks=None, log_disks=None, encryption=False, password=None):
        if self.datastore.exists('volumes', ('id', '=', name)):
            raise VerifyException(
                errno.EEXIST,
                'Volume with same name already exists'
            )

        return ['disk:{0}'.format(os.path.join('/dev', i)) for i in disks]

    def run(self, name, type, layout, disks, cache_disks=None, log_disks=None, encryption=False, password=None):
        vdevs = []
        ltype = VOLUME_LAYOUTS[layout]
        ndisks = DISKS_PER_VDEV[ltype]

        if len(disks) == 1:
            ltype = 'disk'
            ndisks = 1

        for chunk in chunks(disks, ndisks):
            if len(chunk) != ndisks:
                break

            if ltype == 'disk':
                vdevs.append({
                    'type': 'disk',
                    'path': os.path.join('/dev', chunk[0])
                })
            else:
                vdevs.append({
                    'type': ltype,
                    'children': [
                        {'type': 'disk', 'path': os.path.join('/dev', i)} for i in chunk
                    ]
                })

        cache_vdevs = [
            {'type': 'disk', 'path': os.path.join('/dev', i)} for i in cache_disks or []
        ]

        log_vdevs = [
            {'type': 'disk', 'path': os.path.join('/dev', i)} for i in log_disks or []
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
                'params': {
                    'encryption': encryption
                }
            },
            password
        ))


@description("Destroys active volume")
@accepts(str)
class VolumeDestroyTask(Task):
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
        vol = self.datastore.get_by_id('volumes', id)
        encryption = vol.get('encryption', {})
        config = self.dispatcher.call_sync('zfs.pool.query', [('id', '=', id)], {'single': True})

        self.dispatcher.run_hook('volume.pre_destroy', {'name': id})

        with self.dispatcher.get_lock('volumes'):
            if config:
                self.join_subtasks(self.run_subtask('zfs.umount', id))
                self.join_subtasks(self.run_subtask('zfs.pool.destroy', id))

            if encryption.get('key', None) is not None:
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

        volume = self.datastore.get_by_id('volumes', id)
        encryption = volume.get('encryption')
        if not volume:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        if 'name' in updated_params:
            # Renaming pool. Need to export and import again using different name
            new_name = updated_params['name']
            self.dispatcher.run_hook('volume.pre_rename', {'name': id, 'new_name': new_name})

            # Rename mountpoint
            self.join_subtasks(self.run_subtask('zfs.update', id, id, {
                'mountpoint': {'value': '{0}/{1}'.format(VOLUMES_ROOT, new_name)}
            }))

            self.join_subtasks(self.run_subtask('zfs.pool.export', id))
            self.join_subtasks(self.run_subtask('zfs.pool.import', volume['guid'], new_name))

            # Configure newly imported volume
            self.join_subtasks(self.run_subtask('zfs.update', new_name, new_name, {}))
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
            updated_vdevs = []
            params = {}
            subtasks = []
            old_topology = self.dispatcher.call_sync(
                'volume.query',
                [('id', '=', id)],
                {'single': True, 'select': 'topology'}
            )

            new_topology = []
            for vdev in old_topology['data']:
                if vdev['type'] == 'disk':
                    new_topology.append({'type': vdev['type'], 'path': vdev['path'], 'guid': vdev['guid']})
                else:
                    new_topology.append({'type': vdev['type'], 'children': vdev['children'], 'guid': vdev['guid']})

            for group, vdevs in list(updated_params['topology'].items()):
                for vdev in vdevs:
                    if 'guid' not in vdev:
                        if encryption['hashed_password'] is not None:
                            if not is_password(password,
                                               encryption.get('salt', ''),
                                               encryption.get('hashed_password', '')):
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

                    for idx, entry in enumerate(new_topology):
                        if entry['guid'] is old_vdev['guid']:
                            del new_topology[idx]
                            if vdev['type'] == 'disk':
                                new_topology.append({'type': vdev['type'], 'path': vdev['path'], 'guid': vdev['guid']})
                            else:
                                new_topology.append({
                                    'type': vdev['type'],
                                    'children': vdev['children'],
                                    'guid': vdev['guid']
                                })

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

            if encryption['key'] is not None:
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

                if encryption['slot'] is not 0:
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

            for vdev in new_topology:
                vdev.pop('guid', None)

            for vdev, group in iterate_vdevs(new_vdevs):
                if vdev['type'] == 'disk':
                    new_topology.append({'type': vdev['type'], 'path': vdev['path']})
                else:
                    new_topology.append({'type': vdev['type'], 'children': vdev['children']})

            volume['topology'] = {'data': new_topology}
            self.datastore.update('volumes', volume['id'], volume)

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [volume['id']]
            })


@description("Imports previously exported volume")
@accepts(str, str, h.object(), h.ref('volume-import-params'), h.one_of(str, None))
class VolumeImportTask(Task):
    def verify(self, id, new_name, params=None, enc_params=None, password=None):
        if enc_params is None:
            enc_params = {}

        if self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(
                errno.ENOENT,
                'Volume with id {0} already exists'.format(id)
            )

        if self.datastore.exists('volumes', ('id', '=', new_name)):
            raise VerifyException(
                errno.ENOENT,
                'Volume with name {0} already exists'.format(new_name)
            )

        if enc_params.get('key', None) is None:
            return self.verify_subtask('zfs.pool.import', id)
        else:
            disks = enc_params.get('disks', None)
            if disks is None:
                raise VerifyException(
                    errno.EINVAL, 'List of disks must be provided for import')
            else:
                if isinstance(disks, str):
                    disks = [disks]
                return ['disk:{0}'.format(i) for i in disks]

    def run(self, id, new_name, params=None, enc_params=None, password=None):
        if enc_params is None:
            enc_params = {}

        with self.dispatcher.get_lock('volumes'):
            key = enc_params.get('key', None)
            if key is not None:
                disks = enc_params.get('disks', [])
                if isinstance(disks, str):
                    disks = [disks]

                if password is not None:
                    salt, digest = get_digest(password)
                else:
                    salt = None
                    digest = None

                attach_params = {'key': key, 'password': password}
                for dname in disks:
                    disk_id = self.dispatcher.call_sync('disk.path_to_id', dname)
                    self.join_subtasks(self.run_subtask('disk.geli.attach', disk_id, attach_params))
            else:
                salt = None
                digest = None

            mountpoint = os.path.join(VOLUMES_ROOT, new_name)
            self.join_subtasks(self.run_subtask('zfs.pool.import', id, new_name, params))
            self.join_subtasks(self.run_subtask(
                'zfs.update',
                new_name,
                new_name,
                {'mountpoint': {'value': mountpoint}}
            ))

            self.join_subtasks(self.run_subtask('zfs.mount', new_name, True))

            new_id = self.datastore.insert('volumes', {
                'id': new_name,
                'guid': id,
                'type': 'zfs',
                'encryption': {
                    'key': key if key else None,
                    'hashed_password': digest,
                    'salt': salt,
                    'slot': 0 if key else None},
                'encrypted': True if key else False,
                'mountpoint': mountpoint
            })

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'create',
                'ids': [new_id]
            })
        self.dispatcher.run_hook('volume.post_attach', {'name': new_name})


@description("Imports non-ZFS disk contents info existing volume")
@accepts(str, str, str)
class VolumeDiskImportTask(ProgressTask):
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

        if encryption['key'] is not None:
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


@accepts(str, h.ref('zfs-vdev'), h.one_of(str, None))
class VolumeAutoReplaceTask(Task):
    def verify(self, id, failed_vdev, password=None):
        vol = self.datastore.get_by_id('volumes', id)
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['zpool:{0}'.format(id)]

    def run(self, id, failed_vdev, password=None):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        empty_disks = self.dispatcher.call_sync('disk.query', [('status.empty', '=', True)])
        vdev = self.dispatcher.call_sync('zfs.pool.vdev_by_guid', id, failed_vdev)
        minsize = vdev['stats']['size']

        if self.configstore.get('storage.hotsparing.strong_match'):
            pass
        else:
            matching_disks = sorted(empty_disks, key=lambda d: d['mediasize'])
            disk = first_or_default(lambda d: d['mediasize'] > minsize, matching_disks)

            if disk:
                self.join_subtasks(self.run_subtask('disk.format.gpt', disk['id'], 'freebsd-zfs', {'swapsize': 2048}))
                disk = self.dispatcher.call_sync('disk.path_to_id', disk['path'])
                if vol.get('encrypted', False):
                    encryption = vol['encryption']
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
            else:
                raise TaskException(errno.EBUSY, 'No matching disk to be used as spare found')


@description("Locks encrypted ZFS volume")
@accepts(str)
class VolumeLockTask(Task):
    def verify(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') == 'NONE':
            raise VerifyException(errno.EINVAL, 'Volume {0} does not have any unlocked providers'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(id))

        self.dispatcher.run_hook('volume.pre_detach', {'name': id})

        with self.dispatcher.get_lock('volumes'):
            self.join_subtasks(self.run_subtask('zfs.umount', id))
            self.join_subtasks(self.run_subtask('zfs.pool.export', id))

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if vdev_conf.get('encrypted', False) is True:
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


@description("Imports containers, shares and system dataset from a volume")
@accepts(
    str,
    h.enum(str, ['all', 'containers', 'shares', 'system'])
)
class VolumeAutoImportTask(Task):
    def verify(self, volume, scope):
        if not self.datastore.exists('volumes', ('id', '=', volume)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(volume))

        return ['zpool:{0}'.format(volume)]

    def run(self, volume, scope):
        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('id', '=', volume)], {'single': True})
            share_types = self.dispatcher.call_sync('share.supported_types')
            container_types = self.dispatcher.call_sync('container.supported_types')
            imported = {
                'shares': [],
                'containers': [],
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
                            elif scope in ['all', 'containers'] and item_type in container_types:
                                try:
                                    self.join_subtasks(self.run_subtask(
                                        'container.import',
                                        config.get('name', ''),
                                        volume
                                    ))

                                    imported['containers'].append(
                                        {
                                            'type': item_type,
                                            'name': config.get('name', '')
                                        }
                                    )
                                except RpcException as err:
                                    self.add_warning(
                                        TaskWarning(
                                            err.code,
                                            'Container import from {0} failed. Message: {1}'.format(
                                                config_path,
                                                err.message
                                            )
                                        )
                                    )
                                    continue
                            elif item_type not in itertools.chain(share_types, container_types):
                                self.add_warning(
                                    TaskWarning(
                                        errno.EINVAL,
                                        'Import from {0} failed because {1} is unsupported share/container type'.format(
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
    def verify(self, id, password=None, params=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'ALL') == 'ALL':
            raise VerifyException(errno.EINVAL, 'Volume {0} does not have any locked providers'.format(id))

        if encryption['hashed_password'] is not None:
            if password is None:
                raise VerifyException(errno.EINVAL, 'Volume {0} is protected with password. Provide a valid password.'
                                      .format(id))
            if not is_password(password,
                               encryption.get('salt', ''),
                               encryption.get('hashed_password', '')):
                raise VerifyException(errno.EINVAL, 'Password provided for volume {0} unlock is not valid'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, password=None, params=None):
        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if vdev_conf.get('encrypted', False) is False:
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
@accepts(str, h.one_of(str, None))
class VolumeRekeyTask(Task):
    def verify(self, id, password=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise VerifyException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(id))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id, password=None):
        with self.dispatcher.get_lock('volumes'):
            vol = self.datastore.get_by_id('volumes', id)
            encryption = vol.get('encryption')
            disks = self.dispatcher.call_sync('volume.get_volume_disks', id)

            key = base64.b64encode(os.urandom(64)).decode('utf-8')
            slot = 0 if encryption['slot'] is 1 else 1
            if password is not None:
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
    def verify(self, id, out_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise VerifyException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(id))

        if out_path is None:
            raise VerifyException(errno.EINVAL, 'Output file is not specified')

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', id)]

    def run(self, id, out_path=None):
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
    def verify(self, id, password=None, in_path=None):
        if not self.datastore.exists('volumes', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(id))

        if vol.get('providers_presence', 'ALL') != 'NONE':
            raise VerifyException(errno.EINVAL, 'Volume {0} cannot have any online providers'.format(id))

        if in_path is None:
            raise VerifyException(errno.EINVAL, 'Input file is not specified')

        if password is None:
            raise VerifyException(errno.EINVAL, 'Password is not specified')

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, password=None, in_path=None):
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
class VolumeScrubTask(MasterProgressTask):
    def verify(self, id):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def abort(self):
        # We only have one task in here so just wait till it joins and/or ends
        # to begin the abort
        subtask = next(iter(self.progress_subtasks.values()))
        while True:
            if subtask.joining.wait(1):
                break
            if subtask.ended.wait(0.1):
                break
        self.abort_subtask(subtask.tid)

    def run(self, id):
        self.join_subtasks(self.run_subtask('zfs.pool.scrub', id))


@description("Makes vdev in a volume offline")
@accepts(str, str)
class VolumeOfflineVdevTask(Task):
    def verify(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, vdev_guid):
        self.join_subtasks(self.run_subtask(
            'zfs.pool.offline_disk',
            id,
            vdev_guid
        ))


@description("Makes vdev in a volume online")
@accepts(str, str)
class VolumeOnlineVdevTask(Task):
    def verify(self, id, vdev_guid):
        vol = self.dispatcher.call_sync('volume.query', [('id', '=', id)], {'single': True})
        if not vol:
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, id, vdev_guid):
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
    def verify(self, dataset):
        if not self.datastore.exists('volumes', ('id', '=', dataset['volume'])):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(dataset['volume']))

        return ['zpool:{0}'.format(dataset['volume'])]

    def run(self, dataset):
        props = {}
        normalize(dataset, {
            'type': 'FILESYSTEM',
            'permissions_type': 'CHMOD',
            'mounted': True,
            'properties': {}
        })

        if dataset['type'] == 'FILESYSTEM':
            props = {
                'org.freenas:permissions_type': dataset['permissions_type'],
                'aclmode': 'restricted' if dataset['permissions_type'] == 'ACL' else 'passthrough'
            }

        if dataset['type'] == 'VOLUME':
            props['volsize'] = str(dataset['volsize'])

        props['org.freenas:uuid'] = uuid.uuid4()
        self.join_subtasks(self.run_subtask(
            'zfs.create_dataset',
            dataset['volume'],
            dataset['id'],
            dataset['type'],
            props
        ))

        if dataset['mounted']:
            self.join_subtasks(self.run_subtask('zfs.mount', dataset['id']))

        if dataset.get('permissions'):
            path = os.path.join(VOLUMES_ROOT, dataset['id'])
            self.join_subtasks(self.run_subtask('file.set_permissions', path, dataset['permissions']))


@description("Deletes an existing Dataset from a Volume")
@accepts(str)
class DatasetDeleteTask(Task):
    def verify(self, id):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def run(self, id):
        deps = self.dispatcher.call_sync('zfs.dataset.get_dependencies', id)

        for i in deps:
            if i['type'] == 'FILESYSTEM':
                self.join_subtasks(self.run_subtask('zfs.umount', i['id']))

            self.join_subtasks(self.run_subtask('zfs.destroy', i['id']))

        self.join_subtasks(self.run_subtask('zfs.umount', id))
        self.join_subtasks(self.run_subtask('zfs.destroy', id))


@description("Configures/Updates an existing Dataset's properties")
@accepts(str, h.ref('volume-dataset'))
class DatasetConfigureTask(Task):
    def verify(self, id, updated_params):
        pool_name, _, ds = id.partition('/')
        if not self.datastore.exists('volumes', ('id', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def switch_to_acl(self, pool_name, path):
        fs_path = self.dispatcher.call_sync('volume.get_dataset_path', path)
        self.join_subtasks(
            self.run_subtask('zfs.update', pool_name, path, {
                'aclmode': {'value': 'restricted'},
                'org.freenas:permissions_type': {'value': 'ACL'}
            }),
            self.run_subtask('file.set_permissions', fs_path, {
                'acl': DEFAULT_ACLS
            }, True)
        )

    def switch_to_chmod(self, pool_name, path):
        self.join_subtasks(self.run_subtask('zfs.update', pool_name, path, {
            'aclmode': {'value': 'passthrough'},
            'org.freenas:permissions_type': {'value': 'PERM'}
        }))

    def run(self, id, updated_params):
        pool_name, _, _ = id.partition('/')
        ds = wrap(self.dispatcher.call_sync('zfs.dataset.query', [('id', '=', id)], {'single': True}))

        if 'id' in updated_params:
            self.join_subtasks(self.run_subtask('zfs.rename', ds['id'], updated_params['id']))
            ds['id'] = updated_params['id']

        if 'properties' in updated_params:
            props = exclude(
                updated_params['properties'],
                'used', 'usedbydataset', 'usedbysnapshots', 'usedbychildren', 'logicalused', 'logicalreferenced',
                'written', 'usedbyrefreservation', 'referenced', 'available', 'dedup', 'casesensitivity',
                'compressratio', 'refcompressratio'
            )
            self.join_subtasks(self.run_subtask('zfs.update', pool_name, ds['id'], props))

        if 'permissions_type' in updated_params:
            oldtyp = ds['properties.org\\.freenas:permissions_type.value']
            typ = updated_params['permissions_type']

            if oldtyp != 'ACL' and typ == 'ACL':
                self.switch_to_acl(pool_name, ds['id'])

            if oldtyp != 'PERM' and typ == 'PERM':
                self.switch_to_chmod(pool_name, ds['id'])

        if 'permissions' in updated_params:
            fs_path = os.path.join(VOLUMES_ROOT, id)
            self.join_subtasks(self.run_subtask('file.set_permissions', fs_path, updated_params['permissions']))

        if 'mounted' in updated_params:
            if updated_params['mounted']:
                self.join_subtasks(self.run_subtask('zfs.mount', ds['id']))
            else:
                self.join_subtasks(self.run_subtask('zfs.umount', ds['id']))


@description("Creates a snapshot")
@accepts(h.all_of(
    h.ref('volume-snapshot'),
    h.required('volume', 'dataset', 'name'),
    h.forbidden('id')
))
class SnapshotCreateTask(Task):
    def verify(self, snapshot):
        return ['zfs:{0}'.format(snapshot['dataset'])]

    def run(self, snapshot, recursive=False):
        normalize(snapshot, {
            'replicable': True,
            'lifetime': None
        })

        self.join_subtasks(self.run_subtask(
            'zfs.create_snapshot',
            snapshot['volume'],
            snapshot['dataset'],
            snapshot['name'],
            recursive,
            {
                'org.freenas:replicable': {'value': 'yes' if snapshot['replicable'] else 'no'},
                'org.freenas:lifetime': {'value': str(snapshot['lifetime'] or 'no')},
                'org.freenas:uuid': {'value': str(uuid.uuid4())}
            }
        ))


@description("Deletes the specified snapshot")
@accepts(str)
class SnapshotDeleteTask(Task):
    def verify(self, id):
        pool, ds, snap = split_snapshot_name(id)
        return ['zfs:{0}'.format(ds)]

    def run(self, id):
        pool, ds, snap = split_snapshot_name(id)
        self.join_subtasks(self.run_subtask(
            'zfs.delete_snapshot',
            pool,
            ds,
            snap
        ))


@description("Updates configuration of specified snapshot")
@accepts(str, h.all_of(
    h.ref('volume-snapshot')
))
class SnapshotConfigureTask(Task):
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

        self.join_subtasks(self.run_subtask('zfs.update', pool, id, params))


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
        salt = base64.b64encode(os.urandom(64)).decode('utf-8')
    digest = base64.b64encode(hashlib.pbkdf2_hmac('sha256', bytes(str(password), 'utf-8'),
                                                  salt.encode('utf-8'), 200000)).decode('utf-8')
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


def _depends():
    return ['DevdPlugin', 'ZfsPlugin', 'AlertPlugin', 'DiskPlugin']


def _init(dispatcher, plugin):
    boot_pool = dispatcher.call_sync('zfs.pool.get_boot_pool')

    def convert_snapshot(snapshot):
        snapshot = wrap(snapshot)
        dataset, _, name = snapshot['name'].partition('@')
        pool = dataset.partition('/')[0]
        lifetime = None

        if pool == boot_pool['id']:
            return None

        try:
            lifetime = int(snapshot.get('properties.org\\.freenas:lifetime.value'))
        except (ValueError, TypeError):
            pass

        uuid_source = snapshot.get('properties.org\\.freenas:uuid.source')
        if not uuid_source or uuid_source == 'INHERITED':
            dispatcher.submit_task('zfs.update', pool, snapshot['name'], {
                'org.freenas:uuid': {'value': str(uuid.uuid4())}
            })

        return {
            'id': snapshot['name'],
            'volume': pool,
            'dataset': dataset,
            'name': name,
            'lifetime': lifetime,
            'replicable': yesno_to_bool(snapshot.get('properties.org\\.freenas:replicable.value')),
            'properties': include(
                snapshot['properties'],
                'used', 'referenced', 'compressratio', 'clones', 'creation'
            ),
            'holds': snapshot['holds']
        }

    def convert_dataset(ds):
        ds = wrap(ds)
        perms = None

        if pool == boot_pool['id']:
            return None

        if ds['mountpoint']:
            try:
                perms = dispatcher.call_sync('filesystem.stat', ds['mountpoint'])
            except RpcException:
                pass

        uuid_source = ds.get('properties.org\\.freenas:uuid.source')
        if not uuid_source or uuid_source == 'INHERITED':
            dispatcher.submit_task('zfs.update', ds['pool'], ds['name'], {
                'org.freenas:uuid': {'value': str(uuid.uuid4())}
            })

        return {
            'id': ds['name'],
            'name': ds['name'],
            'volume': ds['pool'],
            'type': ds['type'],
            'mountpoint': ds.get('properties.mountpoint.value'),
            'mounted': ds.get('properties.mounted.value'),
            'volsize': ds.get('properties.volsize.rawvalue'),
            'properties': include(
                ds['properties'],
                'used', 'available', 'compression', 'atime', 'dedup',
                'quota', 'refquota', 'reservation', 'refreservation',
                'casesensitivity', 'volsize', 'volblocksize', 'refcompressratio',
                'numclones', 'compressratio', 'written', 'referenced',
                'usedbyrefreservation', 'usedbysnapshots', 'usedbydataset',
                'usedbychildren', 'logicalused', 'logicalreferenced'
            ),
            'permissions_type': ds.get('properties.org\\.freenas:permissions_type.value'),
            'permissions': perms['permissions'] if perms else None
        }

    def on_pool_change(args):
        if args['operation'] == 'delete':
            for i in args['ids']:
                with dispatcher.get_lock('volumes'):
                    volume = dispatcher.call_sync('volume.query', [('id', '=', i)], {'single': True})
                    if volume and volume.get('encrypted', False) == False:
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
                            'attributes': {}
                        })
                    except DuplicateKeyException:
                        # already inserted by task
                        continue

                    logger.info('New volume {0} <{1}>'.format(i['name'], i['guid']))

                    # Set correct mountpoint
                    dispatcher.call_task_sync('zfs.update', i['name'], i['name'], {
                        'mountpoint': {'value': os.path.join(VOLUMES_ROOT, i['name'])}
                    })

                    if i['properties.altroot.source'] != 'DEFAULT':
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

    def scrub_snapshots():
        while True:
            gevent.sleep(SNAPSHOT_SCRUB_INTERVAL)
            ts = int(time.time())
            for snap in dispatcher.call_sync('volume.snapshot.query'):
                snap = wrap(snap)
                creation = int(snap['properties.creation.rawvalue'])
                lifetime = snap['lifetime']

                if lifetime is None:
                    continue

                if creation + lifetime <= ts:
                    dispatcher.call_task_sync('volume.snapshot.delete', snap['id'])

    plugin.register_schema_definition('volume', {
        'type': 'object',
        'title': 'volume',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'guid': {'type': 'string'},
            'type': {
                'type': 'string',
                'enum': ['zfs']
            },
            'topology': {'$ref': 'zfs-topology'},
            'encrypted': {'type': 'boolean'},
            'encryption': {'$ref': 'encryption'},
            'providers_presence': {
                'type': 'string',
                'enum': ['ALL', 'PART', 'NONE']
            },
            'properties': {'$ref': 'volume-properties'},
            'attributes': {'type': 'object'},
            'params': {'type': 'object'}
        }
    })

    plugin.register_schema_definition('volume-property', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'value': {'type': 'string'},
            'rawvalue': {'type': 'string'},
            'source': {
                'type': 'string',
                'enum': ['NONE', 'DEFAULT', 'LOCAL', 'INHERITED']
            }
        }
    })

    plugin.register_schema_definition('volume-properties', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'size': {'$ref': 'volume-property'},
            'capacity': {'$ref': 'volume-property'},
            'health': {'$ref': 'volume-property'},
            'version': {'$ref': 'volume-property'},
            'delegation': {'$ref': 'volume-property'},
            'failmode': {'$ref': 'volume-property'},
            'autoreplace': {'$ref': 'volume-property'},
            'dedupratio': {'$ref': 'volume-property'},
            'free': {'$ref': 'volume-property'},
            'allocated': {'$ref': 'volume-property'},
            'readonly': {'$ref': 'volume-property'},
            'comment': {'$ref': 'volume-property'},
            'expandsize': {'$ref': 'volume-property'},
            'fragmentation': {'$ref': 'volume-property'},
            'leaked': {'$ref': 'volume-property'}
        }
    })

    plugin.register_schema_definition('encryption', {
        'type': 'object',
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
            'volume': {'type': 'string'},
            'mountpoint': {'type': 'string'},
            'mounted': {'type': 'boolean'},
            'type': {
                'type': 'string',
                'enum': ['FILESYSTEM', 'VOLUME']
            },
            'volsize': {'type': ['integer', 'null']},
            'properties': {'$ref': 'volume-dataset-properties'},
            'permissions': {'$ref': 'permissions'},
            'permissions_type': {
                'type': 'string',
                'enum': ['PERM', 'ACL']
            }
        }
    })

    plugin.register_schema_definition('volume-dataset-properties', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'used': {'$ref': 'volume-property'},
            'available': {'$ref': 'volume-property'},
            'compression': {'$ref': 'volume-property'},
            'atime': {'$ref': 'volume-property'},
            'dedup': {'$ref': 'volume-property'},
            'quota': {'$ref': 'volume-property'},
            'refquota': {'$ref': 'volume-property'},
            'reservation': {'$ref': 'volume-property'},
            'refreservation': {'$ref': 'volume-property'},
            'casesensitivity': {'$ref': 'volume-property'},
            'volsize': {'$ref': 'volume-property'},
            'volblocksize': {'$ref': 'volume-property'},
            'refcompressratio': {'$ref': 'volume-property'},
            'numclones': {'$ref': 'volume-property'},
            'compressratio': {'$ref': 'volume-property'},
            'written': {'$ref': 'volume-property'},
            'referenced': {'$ref': 'volume-property'},
            'usedbyrefreservation': {'$ref': 'volume-property'},
            'usedbysnapshots': {'$ref': 'volume-property'},
            'usedbydataset': {'$ref': 'volume-property'},
            'usedbychildren': {'$ref': 'volume-property'},
            'logicalused': {'$ref': 'volume-property'},
            'logicalreferenced': {'$ref': 'volume-property'}
        }
    })

    plugin.register_schema_definition('volume-snapshot', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'volume': {'type': 'string'},
            'dataset': {'type': 'string'},
            'name': {'type': 'string'},
            'replicable': {'type': 'boolean'},
            'lifetime': {'type': ['integer', 'null']},
            'properties': {'$ref': 'volume-snapshot-properties'},
            'holds': {'type': 'object'}
        }
    })

    plugin.register_schema_definition('volume-snapshot-properties', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'used': {'$ref': 'volume-property'},
            'referenced': {'$ref': 'volume-property'},
            'compressratio': {'$ref': 'volume-property'},
            'clones': {'$ref': 'volume-property'},
            'creation': {'$ref': 'volume-property'}
        }
    })

    plugin.register_schema_definition('disks-allocation', {
        'type': 'object',
        'additionalProperties': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                'type': {
                    'type': 'string',
                    'enum': ['BOOT', 'VOLUME', 'ISCSI'],
                },
                'name': {'type': 'string'}
            }
        }
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
            'key': {'type': 'string'},
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
    plugin.register_task_handler('volume.snapshot.create', SnapshotCreateTask)
    plugin.register_task_handler('volume.snapshot.delete', SnapshotDeleteTask)
    plugin.register_task_handler('volume.snapshot.update', SnapshotConfigureTask)

    plugin.register_hook('volume.pre_destroy')
    plugin.register_hook('volume.pre_detach')
    plugin.register_hook('volume.pre_create')
    plugin.register_hook('volume.post_attach')

    plugin.register_hook('volume.pre_rename')
    plugin.register_hook('volume.post_rename')

    plugin.register_event_handler('entity-subscriber.zfs.pool.changed', on_pool_change)
    plugin.register_event_handler('fs.zfs.vdev.removed', on_vdev_remove)

    plugin.register_event_type('volume.changed')
    plugin.register_event_type('volume.dataset.changed')
    plugin.register_event_type('volume.snapshot.changed')

    for vol in dispatcher.call_sync('volume.query'):
        if vol.get('providers_presence', 'ALL') == 'NONE':
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
