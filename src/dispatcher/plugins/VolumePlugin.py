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
import uuid
from cache import EventCacheStore
from lib.system import system, SubprocessException
from lib.freebsd import fstyp
from task import Provider, Task, ProgressTask, TaskException, VerifyException, query
from freenas.dispatcher.rpc import (
    RpcException, description, accepts, returns, private, SchemaHelper as h
    )
from utils import first_or_default
from datastore import DuplicateKeyException
from freenas.utils import include, exclude, normalize
from freenas.utils.query import wrap
from freenas.utils.copytree import count_files, copytree
from cryptography.fernet import Fernet, InvalidToken


VOLUMES_ROOT = '/mnt'
DEFAULT_ACLS = [
    {'text': 'owner@:rwxpDdaARWcCos:fd:allow'},
    {'text': 'group@:rwxpDdaARWcCos:fd:allow'},
    {'text': 'everyone@:rxaRc:fd:allow'}
]
logger = logging.getLogger('VolumePlugin')
snapshots = None


@description("Provides access to volumes information")
class VolumeProvider(Provider):
    @query('volume')
    def query(self, filter=None, params=None):
        def is_upgraded(pool):
            if pool['properties.version.value'] != '-':
                return False

            for feat in pool['features']:
                if feat['state'] == 'DISABLED':
                    return False

            return True

        def extend_dataset(ds):
            ds = wrap(ds)
            return {
                'name': ds['name'],
                'type': ds['type'],
                'mountpoint': ds['mountpoint'],
                'volsize': ds.get('properties.volsize.rawvalue'),
                'properties': include(
                    ds['properties'],
                    'used', 'available', 'compression', 'atime', 'dedup',
                    'quota', 'refquota', 'reservation', 'refreservation',
                    'casesensitivity', 'volsize', 'volblocksize',
                ),
                'permissions_type':  ds.get('properties.org\\.freenas:permissions_type.value'),
            }

        def extend(vol):
            config = wrap(self.get_config(vol['name']))
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
                    'datasets': None,
                    'upgraded': None,
                    'topology': topology,
                    'root_vdev': config['root_vdev'],
                    'status': config['status'],
                    'scan': config['scan'],
                    'properties': config['properties']
                })

                if config['status'] != 'UNAVAIL':
                    vol.update({
                        'description': config.get('root_dataset.properties.org\\.freenas:description.value'),
                        'mountpoint': config['root_dataset.properties.mountpoint.value'],
                        'datasets': list(map(extend_dataset, flatten_datasets(config['root_dataset']))),
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

        return self.datastore.query('volumes', *(filter or []), callback=extend, **(params or {}))

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

    @accepts(str)
    @returns(str)
    def resolve_path(self, volname, path):
        volume = self.query([('name', '=', volname)], {'single': True})
        if not volume:
            raise RpcException(errno.ENOENT, 'Volume {0} not found'.format(volname))

        return os.path.join(volume['mountpoint'], path)

    @accepts(str, str)
    @returns(str)
    def get_dataset_path(self, volname, dsname):
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
        config = self.get_config(volname)
        if config:
            datasets = [d['name'] for d in flatten_datasets(config['root_dataset'])]
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
        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

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
            for dev in self.dispatcher.call_sync('zfs.pool.get_disks', pool['name']):
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
            for dev in self.dispatcher.call_sync('volume.get_volume_disks', vol['name']):
                if dev in disks:
                    ret[dev] = {
                        'type': 'VOLUME',
                        'name': vol['name']
                    }

        return ret

    @description("Returns Information about all the possible attributes of" +
                 " the Volume (name, guid, zfs properties, datasets, etc...)")
    @accepts(str)
    @returns(h.ref('zfs-pool'))
    def get_config(self, volume):
        return self.dispatcher.call_sync(
            'zfs.pool.query',
            [('name', '=', volume)],
            {'single': True}
        )

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


class SnapshotProvider(Provider):
    def query(self, filter=None, params=None):
        return snapshots.query(*(filter or []), **(params or {}))


@description("Creates new volume")
@accepts(h.ref('volume'), h.one_of(str, None))
class VolumeCreateTask(ProgressTask):
    def verify(self, volume, password=None):
        if self.datastore.exists('volumes', ('name', '=', volume['name'])):
            raise VerifyException(errno.EEXIST, 'Volume with same name already exists')

        return ['disk:{0}'.format(i) for i, _ in get_disks(volume['topology'])]

    def run(self, volume, password=None):
        name = volume['name']
        type = volume.get('type', 'zfs')
        params = volume.get('params') or {}
        mountpoint = params.pop(
            'mountpoint',
            os.path.join(VOLUMES_ROOT, volume['name'])
        )
        encryption = params.pop('encryption', False)
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
            subtasks.append(self.run_subtask('disk.format.gpt', dname, 'freebsd-zfs', {
                'blocksize': params.get('blocksize', 4096),
                'swapsize': params.get('swapsize', 2048) if dgroup == 'data' else 0
            }))

        self.join_subtasks(*subtasks)

        self.set_progress(20)

        if encryption:
            subtasks = []
            for dname, dgroup in get_disks(volume['topology']):
                subtasks.append(self.run_subtask('disk.geli.init', dname, {
                    'key': key,
                    'password': password
                }))
            self.join_subtasks(*subtasks)
            self.set_progress(30)

            subtasks = []
            for dname, dgroup in get_disks(volume['topology']):
                subtasks.append(self.run_subtask('disk.geli.attach', dname, {
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
                'zfs.configure',
                name, name,
                {'org.freenas:permissions_type': {'value': 'PERM'}}
            ))

            self.set_progress(60)
            self.join_subtasks(self.run_subtask('zfs.mount', name))
            self.set_progress(80)

            pool = self.dispatcher.call_sync('zfs.pool.query', [('name', '=', name)], {'single': True})
            id = self.datastore.insert('volumes', {
                'id': str(pool['guid']),
                'name': name,
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
@accepts(str, str, h.array(str), h.object(), h.one_of(str, None))
class VolumeAutoCreateTask(Task):
    def verify(self, name, type, disks, params=None, password=None):
        if self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.EEXIST,
                                  'Volume with same name already exists')

        return ['disk:{0}'.format(os.path.join('/dev', i)) for i in disks]

    def run(self, name, type, disks, params=None, password=None):
        vdevs = []
        if len(disks) % 3 == 0:
            for i in range(0, len(disks), 3):
                vdevs.append({
                    'type': 'raidz1',
                    'children': [{'type': 'disk', 'path': os.path.join('/dev', i)} for i in disks[i:i+3]]
                })
        elif len(disks) % 2 == 0:
            for i in range(0, len(disks), 2):
                vdevs.append({
                    'type': 'mirror',
                    'children': [{'type': 'disk', 'path': os.path.join('/dev', i)} for i in disks[i:i+2]]
                })
        else:
            vdevs = [{'type': 'disk', 'path': os.path.join('/dev', i)} for i in disks]

        self.join_subtasks(self.run_subtask('volume.create', {
            'name': name,
            'type': type,
            'topology': {'data': vdevs},
            'params': params},
            password
        ))


@description("Destroys active volume")
@accepts(str)
class VolumeDestroyTask(Task):
    def verify(self, name):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(id))

        try:
            disks = self.dispatcher.call_sync('volume.get_volume_disks', name)
            return ['disk:{0}'.format(d) for d in disks]
        except RpcException:
            return []

    def run(self, name):
        vol = self.datastore.get_one('volumes', ('name', '=', name))
        encryption = vol.get('encryption', {})
        config = self.dispatcher.call_sync('volume.get_config', name)

        self.dispatcher.run_hook('volume.pre_destroy', {'name': name})

        with self.dispatcher.get_lock('volumes'):
            if config:
                self.join_subtasks(self.run_subtask('zfs.umount', name))
                self.join_subtasks(self.run_subtask('zfs.pool.destroy', name))

            if encryption.get('key', None) is not None:
                subtasks = []
                if 'topology' in vol:
                    for dname, _ in get_disks(vol['topology']):
                        subtasks.append(self.run_subtask('disk.geli.kill', dname))
                    self.join_subtasks(*subtasks)

            self.datastore.delete('volumes', vol['id'])
            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'delete',
                'ids': [vol['id']]
            })


@description("Updates configuration of existing volume")
@accepts(str, h.ref('volume'), h.one_of(str, None))
class VolumeUpdateTask(Task):
    def verify(self, name, updated_params, password=None):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        topology = updated_params.get('topology')
        if not topology:
            disks = self.dispatcher.call_sync('volume.get_volume_disks', name)
            return ['disk:{0}'.format(d) for d in disks]

        return ['disk:{0}'.format(i) for i, _ in get_disks(topology)]

    def run(self, name, updated_params, password=None):
        if password is None:
            password = ''

        volume = self.datastore.get_one('volumes', ('name', '=', name))
        encryption = volume.get('encryption')
        if not volume:
            raise TaskException(errno.ENOENT, 'Volume {0} not found'.format(name))

        if 'name' in updated_params:
            # Renaming pool. Need to export and import again using different name
            new_name = updated_params['name']
            self.join_subtasks(self.run_subtask('zfs.pool.export', name))
            self.join_subtasks(self.run_subtask('zfs.pool.import', volume['id'], new_name))

            # Rename mountpoint
            self.join_subtasks(self.run_subtask('zfs.configure', new_name, new_name, {
                'mountpoint': {'value': '{0}/{1}'.format(VOLUMES_ROOT, new_name)}
            }))

            volume['name'] = new_name
            self.datastore.update('volumes', volume['id'], volume)

        if 'topology' in updated_params:
            new_vdevs = {}
            updated_vdevs = []
            params = {}
            subtasks = []
            old_topology = self.dispatcher.call_sync(
                'volume.query',
                [('name', '=', name)],
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
                                raise TaskException(errno.EINVAL,
                                                    'Password provided for volume {0} configuration update is not valid'
                                                    .format(name))
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
                                new_topology.append({'type': vdev['type'],
                                                     'children': vdev['children'],
                                                     'guid': vdev['guid']})

            for vdev, group in iterate_vdevs(new_vdevs):
                if vdev['type'] == 'disk':
                    subtasks.append(self.run_subtask('disk.format.gpt', vdev['path'], 'freebsd-zfs', {
                        'blocksize': params.get('blocksize', 4096),
                        'swapsize': params.get('swapsize', 2048) if group == 'data' else 0
                    }))

            for vdev in updated_vdevs:
                subtasks.append(self.run_subtask('disk.format.gpt', vdev['vdev']['path'], 'freebsd-zfs', {
                    'blocksize': params.get('blocksize', 4096),
                    'swapsize': params.get('swapsize', 2048)
                }))

            self.join_subtasks(*subtasks)

            if encryption['key'] is not None:
                subtasks = []
                for vdev, group in iterate_vdevs(new_vdevs):
                    subtasks.append(self.run_subtask('disk.geli.init', vdev['path'], {
                        'key': encryption['key'],
                        'password': password
                    }))
                self.join_subtasks(*subtasks)

                if encryption['slot'] is not 0:
                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask('disk.geli.ukey.set', vdev['path'], {
                            'key': encryption['key'],
                            'password': password,
                            'slot': 1
                        }))
                    self.join_subtasks(*subtasks)

                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask('disk.geli.ukey.del', vdev['path'], 0))
                    self.join_subtasks(*subtasks)

                vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})
                if vol.get('providers_presence', 'NONE') != 'NONE':
                    subtasks = []
                    for vdev, group in iterate_vdevs(new_vdevs):
                        subtasks.append(self.run_subtask('disk.geli.attach', vdev['path'], {
                            'key': encryption['key'],
                            'password': password
                        }))
                    self.join_subtasks(*subtasks)

            new_vdevs_gptids = convert_topology_to_gptids(self.dispatcher, new_vdevs)

            for vdev in updated_vdevs:
                vdev['vdev']['path'] = get_disk_gptid(self.dispatcher, vdev['vdev']['path'])

            self.join_subtasks(self.run_subtask(
                'zfs.pool.extend',
                name,
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

        if self.datastore.exists('volumes', ('name', '=', new_name)):
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
                    self.join_subtasks(self.run_subtask('disk.geli.attach', dname, attach_params))

                real_id = None
                pool_info = self.dispatcher.call_sync('volume.find')
                for pool in pool_info:
                    if pool['name'] == new_name and pool['status'] == "ONLINE":
                        real_id = pool['id']
                        break
                if real_id is None:
                    raise TaskException('Importable volume {0} not found'.format(new_name))

            else:
                salt = None
                digest = None
                real_id = id

            mountpoint = os.path.join(VOLUMES_ROOT, new_name)
            self.join_subtasks(self.run_subtask('zfs.pool.import', real_id, new_name, params))
            self.join_subtasks(self.run_subtask(
                'zfs.configure',
                new_name,
                new_name,
                {'mountpoint': {'value': mountpoint}}
            ))

            self.join_subtasks(self.run_subtask('zfs.mount', new_name))

            new_id = self.datastore.insert('volumes', {
                'id': real_id,
                'name': new_name,
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
    def verify(self, name):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', name)]

    def run(self, name):
        vol = self.datastore.get_one('volumes', ('name', '=', name))
        disks = self.dispatcher.call_sync('volume.get_volume_disks', name)
        self.join_subtasks(self.run_subtask('zfs.umount', name))
        self.join_subtasks(self.run_subtask('zfs.pool.export', name))

        encryption = vol.get('encryption')

        if encryption['key'] is not None:
            subtasks = []
            for dname in disks:
                subtasks.append(self.run_subtask('disk.geli.detach', dname))
            self.join_subtasks(*subtasks)

        self.datastore.delete('volumes', vol['id'])

        if encryption['key']:
            return encryption['key']
        else:
            return None


@description("Upgrades volume to newest ZFS version")
@accepts(str)
class VolumeUpgradeTask(Task):
    def verify(self, name):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', name)]

    def run(self, name):
        vol = self.datastore.get_one('volumes', ('name', '=', name))
        self.join_subtasks(self.run_subtask('zfs.pool.upgrade', name))
        self.dispatcher.dispatch_event('volume.changed', {
            'operation': 'update',
            'ids': [vol['id']]
        })


class VolumeAutoReplaceTask(Task):
    def verify(self, name, failed_vdev):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        return ['zpool:{0}'.format(name)]

    def run(self, name, failed_vdev):
        empty_disks = self.dispatcher.call_sync('disk.query', [('status.empty', '=', True)])
        vdev = self.dispatcher.call_sync('zfs.pool.vdev_by_guid', name, failed_vdev)
        minsize = vdev['stats']['size']

        if self.configstore.get('storage.hotsparing.strong_match'):
            pass
        else:
            matching_disks = sorted(empty_disks, key=lambda d: d['mediasize'])
            disk = first_or_default(lambda d: d['mediasize'] > minsize, matching_disks)

            if disk:
                self.join_subtasks(self.run_subtask('disk.format.gpt', disk['path'], 'freebsd-zfs', {'swapsize': 2048}))
                disk = self.dispatcher.call_sync('disk.query', [('id', '=', disk['id'])], {'single': True})
                self.join_subtasks(self.run_subtask('zfs.pool.replace', name, failed_vdev, {
                    'type': 'disk',
                    'path': disk['status']['data_partition_path']
                }))
            else:
                raise TaskException(errno.EBUSY, 'No matching disk to be used as spare found')


@description("Locks encrypted ZFS volume")
@accepts(str)
class VolumeLockTask(Task):
    def verify(self, name):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(name))

        if vol.get('providers_presence', 'NONE') == 'NONE':
            raise VerifyException(errno.EINVAL, 'Volume {0} does not have any unlocked providers'.format(name))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', name)]

    def run(self, name):
        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})
            self.join_subtasks(self.run_subtask('zfs.umount', name))
            self.join_subtasks(self.run_subtask('zfs.pool.export', name))

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if vdev_conf.get('encrypted', False) is True:
                        subtasks.append(self.run_subtask('disk.geli.detach', vdev['path']))
                else:
                    subtasks.append(self.run_subtask('disk.geli.detach', vdev['path']))
            self.join_subtasks(*subtasks)

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })


@description("Unlocks encrypted ZFS volume")
@accepts(str, h.one_of(str, None), h.object())
class VolumeUnlockTask(Task):
    def verify(self, name, password=None, params=None):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(name))

        if vol.get('providers_presence', 'ALL') == 'ALL':
            raise VerifyException(errno.EINVAL, 'Volume {0} does not have any locked providers'.format(name))

        if encryption['hashed_password'] is not None:
            if password is None:
                raise VerifyException(errno.EINVAL, 'Volume {0} is protected with password. Provide a valid password.'
                                      .format(name))
            if not is_password(password,
                               encryption.get('salt', ''),
                               encryption.get('hashed_password', '')):
                raise VerifyException(errno.EINVAL, 'Password provided for volume {0} unlock is not valid'.format(name))

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, name, password=None, params=None):
        with self.dispatcher.get_lock('volumes'):
            vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

            subtasks = []
            for vdev, _ in iterate_vdevs(vol['topology']):
                if vol['providers_presence'] == 'PART':
                    vdev_conf = self.dispatcher.call_sync('disk.get_disk_config', vdev)
                    if vdev_conf.get('encrypted', False) is False:
                        subtasks.append(self.run_subtask('disk.geli.attach', vdev['path'], {
                            'key': vol['encryption']['key'],
                            'password': password
                        }))
                else:
                    subtasks.append(self.run_subtask('disk.geli.attach', vdev['path'], {
                        'key': vol['encryption']['key'],
                        'password': password
                    }))
            self.join_subtasks(*subtasks)

            self.join_subtasks(self.run_subtask('zfs.pool.import', vol['id'], name, params))
            self.join_subtasks(self.run_subtask(
                'zfs.configure',
                name,
                name,
                {'mountpoint': {'value': vol['mountpoint']}}
            ))

            self.join_subtasks(self.run_subtask('zfs.mount', name))

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })


@description("Generates and sets new key for encrypted ZFS volume")
@accepts(str, h.one_of(str, None))
class VolumeRekeyTask(Task):
    def verify(self, name, password=None):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(name))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise VerifyException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(name))

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', name)]

    def run(self, name, password=None):
        with self.dispatcher.get_lock('volumes'):
            vol = self.datastore.get_one('volumes', ('name', '=', name))
            encryption = vol.get('encryption')
            disks = self.dispatcher.call_sync('volume.get_volume_disks', name)

            key = base64.b64encode(os.urandom(64)).decode('utf-8')
            slot = 0 if encryption['slot'] is 1 else 1
            if password is not None:
                salt, digest = get_digest(password)
            else:
                salt = None
                digest = None

            subtasks = []
            for dname in disks:
                subtasks.append(self.run_subtask('disk.geli.ukey.set', dname, {
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
            for dname in disks:
                subtasks.append(self.run_subtask('disk.geli.ukey.del', dname, slot))
            self.join_subtasks(*subtasks)

            self.dispatcher.dispatch_event('volume.changed', {
                'operation': 'update',
                'ids': [vol['id']]
            })


@description("Creates a backup file of Master Keys of encrypted volume")
@accepts(str, str)
class VolumeBackupKeysTask(Task):
    def verify(self, name, out_path=None):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(name))

        if vol.get('providers_presence', 'NONE') != 'ALL':
            raise VerifyException(errno.EINVAL, 'Every provider associated with volume {0} must be online'.format(name))

        if out_path is None:
            raise VerifyException(errno.EINVAL, 'Output file is not specified')

        return ['disk:{0}'.format(d) for d in self.dispatcher.call_sync('volume.get_volume_disks', name)]

    def run(self, name, out_path=None):
        with self.dispatcher.get_lock('volumes'):
            disks = self.dispatcher.call_sync('volume.get_volume_disks', name)
            out_data = {}

            subtasks = []
            for dname in disks:
                subtasks.append(self.run_subtask('disk.geli.mkey.backup', dname))
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
    def verify(self, name, password=None, in_path=None):
        if not self.datastore.exists('volumes', ('name', '=', name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(name))

        vol = self.dispatcher.call_sync('volume.query', [('name', '=', name)], {'single': True})

        encryption = vol.get('encryption')

        if encryption['key'] is None:
            raise VerifyException(errno.EINVAL, 'Volume {0} is not encrypted'.format(name))

        if vol.get('providers_presence', 'ALL') != 'NONE':
            raise VerifyException(errno.EINVAL, 'Volume {0} cannot have any online providers'.format(name))

        if in_path is None:
            raise VerifyException(errno.EINVAL, 'Input file is not specified')

        if password is None:
            raise VerifyException(errno.EINVAL, 'Password is not specified')

        return ['disk:{0}'.format(d) for d, _ in get_disks(vol['topology'])]

    def run(self, name, password=None, in_path=None):
        vol = self.datastore.get_one('volumes', ('name', '=', name))
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
                    raise TaskException(errno.EINVAL, 'Disk {0} is not a part of volume {1}'.format(disk['disk'], name))

                subtasks.append(self.run_subtask('disk.geli.mkey.restore', disk))

            self.join_subtasks(*subtasks)


@description("Creates a dataset in an existing volume")
@accepts(str, str, h.ref('dataset-type'), h.object())
class DatasetCreateTask(Task):
    def verify(self, pool_name, path, type, params=None):
        if not self.datastore.exists('volumes', ('name', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def run(self, pool_name, path, type, params=None):
        if params:
            normalize(params, {
                'properties': {}
            })

        if type == 'VOLUME':
            params['properties']['volsize'] = {'value': str(params['volsize'])}

        self.join_subtasks(self.run_subtask(
            'zfs.create_dataset',
            pool_name,
            path,
            type,
            {k: v['value'] for k, v in list(params['properties'].items())} if params else {}
        ))

        if params:
            props = {}
            if 'permissions_type' in params:
                props['org.freenas:permissions_type'] = {'value': params['permissions_type']}
                props['aclmode'] = {'value': 'restricted' if params['permissions_type'] == 'ACL' else 'passthrough'}

            self.join_subtasks(self.run_subtask('zfs.configure', pool_name, path, props))

        self.join_subtasks(self.run_subtask('zfs.mount', path))


@description("Deletes an existing Dataset from a Volume")
@accepts(str, str, bool)
class DatasetDeleteTask(Task):
    def verify(self, pool_name, path, recursive=False):
        if not self.datastore.exists('volumes', ('name', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def run(self, pool_name, path, recursive=False):
        if recursive:
            deps = self.dispatcher.call_sync('zfs.dataset.get_dependencies', path)

            for i in deps:
                if i['type'] == 'FILESYSTEM':
                    self.join_subtasks(self.run_subtask('zfs.umount', i['name']))

                self.join_subtasks(self.run_subtask('zfs.destroy', i['name']))

        self.join_subtasks(self.run_subtask('zfs.umount', path))
        self.join_subtasks(self.run_subtask('zfs.destroy', path))


@description("Configures/Updates an existing Dataset's properties")
@accepts(str, str, h.object())
class DatasetConfigureTask(Task):
    def verify(self, pool_name, path, updated_params):
        if not self.datastore.exists('volumes', ('name', '=', pool_name)):
            raise VerifyException(errno.ENOENT, 'Volume {0} not found'.format(pool_name))

        return ['zpool:{0}'.format(pool_name)]

    def switch_to_acl(self, pool_name, path):
        fs_path = self.dispatcher.call_sync('volume.get_dataset_path', pool_name, path)
        self.join_subtasks(
            self.run_subtask('zfs.configure', pool_name, path, {
                'aclmode': {'value': 'restricted'},
                'org.freenas:permissions_type': {'value': 'ACL'}
            }),
            self.run_subtask('file.set_permissions', fs_path, {
                'acl': DEFAULT_ACLS
            }, True)
        )

    def switch_to_chmod(self, pool_name, path):
        self.join_subtasks(self.run_subtask('zfs.configure', pool_name, path, {
            'aclmode': {'value': 'passthrough'},
            'org.freenas:permissions_type': {'value': 'PERMS'}
        }))

    def run(self, pool_name, path, updated_params):
        ds = wrap(self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', path)], {'single': True}))

        if 'name' in updated_params:
            self.join_subtasks(self.run_subtask('zfs.rename', ds['name'], updated_params['name']))
            ds['name'] = updated_params['name']

        if 'properties' in updated_params:
            props = exclude(updated_params['properties'], 'used', 'available', 'dedup', 'casesensitivity')
            self.join_subtasks(self.run_subtask('zfs.configure', pool_name, ds['name'], props))

        if 'permissions_type' in updated_params:
            oldtyp = ds['properties.org\\.freenas:permissions_type.value']
            typ = updated_params['permissions_type']

            if oldtyp != 'ACL' and typ == 'ACL':
                self.switch_to_acl(pool_name, ds['name'])

            if oldtyp != 'PERMS' and typ == 'PERMS':
                self.switch_to_chmod(pool_name, ds['name'])


class SnapshotCreateTask(Task):
    def verify(self, pool_name, dataset_name, snapshot_name, recursive=False):
        return ['zfs:{0}'.format(dataset_name)]

    def run(self, pool_name, dataset_name, snapshot_name, recursive=False):
        self.join_subtasks(self.run_subtask(
            'zfs.create_snapshot',
            pool_name,
            dataset_name,
            snapshot_name,
            recursive
        ))


class SnapshotDeleteTask(Task):
    def verify(self, pool_name, dataset_name, snapshot_name):
        return ['zfs:{0}'.format(dataset_name)]

    def run(self, pool_name, dataset_name, snapshot_name):
        self.join_subtasks(self.run_subtask(
            'zfs.delete_snapshot',
            pool_name,
            dataset_name,
            snapshot_name,
        ))


def flatten_datasets(root):
    for ds in root['children']:
        for c in flatten_datasets(ds):
            yield c

    del root['children']
    yield root


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
    config = dispatcher.call_sync('disk.get_disk_config', disk)
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
        dataset, _, name = snapshot['name'].partition('@')
        pool = dataset.partition('/')[0]

        if pool == boot_pool:
            return None

        return {
            'id': snapshot['name'],
            'pool': pool,
            'dataset': dataset,
            'name': name,
            'properties': include(
                snapshot['properties'],
                'used', 'referenced', 'compressratio', 'clones'
            ),
            'holds': snapshot['holds']
        }

    def on_pool_change(args):
        if args['operation'] == 'delete':
            for i in args['ids']:
                with dispatcher.get_lock('volumes'):
                    volume = dispatcher.call_sync('volume.query', [('name', '=', i)], {'single': True})
                    if (volume and volume.get('encrypted', False) == False) or not volume:
                        logger.info('Volume {0} is going away'.format(volume['name']))
                        dispatcher.datastore.delete('volumes', volume['id'])
                        dispatcher.dispatch_event('volume.changed', {
                            'operation': 'delete',
                            'ids': [volume['id']]
                        })

        if args['operation'] in ('create', 'update'):
            entities = filter(lambda i: i['guid'] != boot_pool['guid'], args['entities'])
            for i in entities:
                if args['operation'] == 'update':
                    volume = dispatcher.datastore.get_one('volumes', ('name', '=', i['name']))
                    if volume:
                        dispatcher.dispatch_event('volume.changed', {
                            'operation': 'update',
                            'ids': [volume['id']]
                        })
                    continue

                with dispatcher.get_lock('volumes'):
                    try:
                        dispatcher.datastore.insert('volumes', {
                            'id': i['guid'],
                            'name': i['name'],
                            'type': 'zfs',
                            'attributes': {}
                        })
                    except DuplicateKeyException:
                        # already inserted by task
                        continue

                    logger.info('New volume {0} <{1}>'.format(i['name'], i['guid']))

                    # Set correct mountpoint
                    dispatcher.call_task_sync('zfs.configure', i['name'], i['name'], {
                        'mountpoint': {'value': os.path.join(VOLUMES_ROOT, i['name'])}
                    })

                    if i['properties.altroot.source'] != 'DEFAULT':
                        # Ouch. That pool is created or imported with altroot.
                        # We need to export and reimport it to remove altroot property
                        dispatcher.call_task_sync('zfs.pool.export', i['name'])
                        dispatcher.call_task_sync('zfs.pool.import', i['guid'], i['name'])

                    dispatcher.dispatch_event('volume.changed', {
                        'operation': 'create',
                        'ids': [i['guid']]
                    })

    def on_snapshot_change(args):
        snapshots.propagate(args, callback=convert_snapshot)

    def on_dataset_change(args):
        dispatcher.dispatch_event('volume.changed', {
            'operation': 'update',
            'ids': [args['guid']]
        })

    def on_vdev_remove(args):
        dispatcher.call_sync('alert.emit', {
            'name': 'volume.disk_removed',
            'description': 'Some disk was removed from the system',
            'severity': 'WARNING'
        })

    plugin.register_schema_definition('volume', {
        'type': 'object',
        'title': 'volume',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'type': {
                'type': 'string',
                'enum': ['zfs']
            },
            'topology': {'$ref': 'zfs-topology'},
            'encrypted': {'type': 'boolean'},
            'providers_presence': {
                'type': 'string',
                'enum': ['ALL', 'PART', 'NONE']
            },
            'params': {'type': 'object'},
            'attributes': {'type': 'object'}
        }
    })

    plugin.register_schema_definition('dataset', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'mountpoint': {'type': 'string'},
            'type': {
                'type': 'string',
                'enum': ['FILESYSTEM', 'VOLUME']
            },
            'volsize': {'type': ['integer', 'null']},
            'properties': {'type': 'object'},
            'permissions_type': {
                'type': 'string',
                'enum': ['PERM', 'ACL']
            }
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
                'items': 'string'
            }
        }
    })

    plugin.register_provider('volume', VolumeProvider)
    plugin.register_provider('volume.snapshot', SnapshotProvider)
    plugin.register_task_handler('volume.create', VolumeCreateTask)
    plugin.register_task_handler('volume.create_auto', VolumeAutoCreateTask)
    plugin.register_task_handler('volume.destroy', VolumeDestroyTask)
    plugin.register_task_handler('volume.import', VolumeImportTask)
    plugin.register_task_handler('volume.import_disk', VolumeDiskImportTask)
    plugin.register_task_handler('volume.detach', VolumeDetachTask)
    plugin.register_task_handler('volume.update', VolumeUpdateTask)
    plugin.register_task_handler('volume.upgrade', VolumeUpgradeTask)
    plugin.register_task_handler('volume.autoreplace', VolumeAutoReplaceTask)
    plugin.register_task_handler('volume.lock', VolumeLockTask)
    plugin.register_task_handler('volume.unlock', VolumeUnlockTask)
    plugin.register_task_handler('volume.rekey', VolumeRekeyTask)
    plugin.register_task_handler('volume.keys.backup', VolumeBackupKeysTask)
    plugin.register_task_handler('volume.keys.restore', VolumeRestoreKeysTask)
    plugin.register_task_handler('volume.dataset.create', DatasetCreateTask)
    plugin.register_task_handler('volume.dataset.delete', DatasetDeleteTask)
    plugin.register_task_handler('volume.dataset.update', DatasetConfigureTask)
    plugin.register_task_handler('volume.snapshot.create', SnapshotCreateTask)
    plugin.register_task_handler('volume.snapshot.delete', SnapshotDeleteTask)

    plugin.register_hook('volume.pre_destroy')
    plugin.register_hook('volume.pre_detach')
    plugin.register_hook('volume.pre_create')
    plugin.register_hook('volume.pre_attach')

    plugin.register_event_handler('entity-subscriber.zfs.pool.changed', on_pool_change)
    plugin.register_event_handler('fs.zfs.vdev.removed', on_vdev_remove)
    plugin.register_event_handler('fs.zfs.dataset.created', on_dataset_change)
    plugin.register_event_handler('fs.zfs.dataset.deleted', on_dataset_change)
    plugin.register_event_handler('fs.zfs.dataset.renamed', on_dataset_change)

    plugin.register_event_type('volume.changed')
    plugin.register_event_type('volume.snapshot.changed')

    dispatcher.rpc.call_sync('alert.register_alert', 'volume.disk_removed', 'Volume disk removed')

    for vol in dispatcher.call_sync('volume.query'):
        if vol.get('providers_presence', 'ALL') == 'NONE':
            continue

        try:
            dispatcher.call_task_sync('zfs.mount', vol['name'], True)

            # XXX: check mountpoint property and correct if needed
        except TaskException as err:
            if err.code != errno.EBUSY:
                logger.warning('Cannot mount volume {0}: {1}'.format(vol['name'], str(err)))

    global snapshots
    snapshots = EventCacheStore(dispatcher, 'volume.snapshot')
    snapshots.populate(dispatcher.call_sync('zfs.snapshot.query'), callback=convert_snapshot)
    snapshots.ready = True
    plugin.register_event_handler(
        'entity-subscriber.zfs.snapshot.changed',
        on_snapshot_change
    )
