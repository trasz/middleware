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

import os
import re
import enum
import errno
import logging
import tempfile
import base64
import gevent
import gevent.monkey
from bsd import geom
from gevent.lock import RLock
from resources import Resource
from datetime import datetime, timedelta
from freenas.utils import first_or_default
from freenas.utils.query import wrap
from cam import CamDevice
from cache import CacheStore
from lib.geom import confxml
from lib.system import system, SubprocessException
from task import (
    Provider, Task, ProgressTask, TaskStatus, TaskException, VerifyException,
    query
)
from freenas.dispatcher.rpc import RpcException, accepts, returns, description, private
from freenas.dispatcher.rpc import SchemaHelper as h

# Note the following monkey patch is required for pySMART to work correctly
gevent.monkey.patch_subprocess()
from pySMART import Device, smart_health_assement


EXPIRE_TIMEOUT = timedelta(hours=24)
multipaths = -1
diskinfo_cache = CacheStore()
logger = logging.getLogger('DiskPlugin')
diskinfo_cache_lock = RLock()


class AcousticLevel(enum.IntEnum):
    DISABLED = 0
    MINIMUM = 1
    MEDIUM = 64
    MAXIMUM = 127


class SelfTestType(enum.Enum):
    SHORT = 'short'
    LONG = 'long'
    CONVEYANCE = 'conveyance'
    OFFLINE = 'offline'


class DiskProvider(Provider):
    @query('disk')
    def query(self, filter=None, params=None):
        def extend(disk):
            if disk.get('delete_at'):
                disk['online'] = False
            else:
                disk['online'] = self.is_online(disk['path'])
                disk['status'] = diskinfo_cache.get(disk['id'])

            return disk

        return wrap(self.datastore.query('disks', callback=extend)).query(*(filter or []), **(params or {}))

    @accepts(str)
    @returns(bool)
    def is_online(self, name):
        return os.path.exists(name)

    @accepts(str)
    @returns(str)
    def partition_to_disk(self, part_name):
        # Is it disk name?
        d = get_disk_by_path(part_name)
        if d:
            return part_name

        part = self.get_partition_config(part_name)
        return part['disk']

    @accepts(str)
    @returns(h.ref('disk-status'))
    def get_disk_config_by_id(self, id):
        disk = diskinfo_cache.get(id)
        if not disk:
            raise RpcException(errno.ENOENT, "Disk id: {0} not found".format(id))

        return disk

    @accepts(str)
    @returns(h.ref('disk-status'))
    def get_disk_config(self, name):
        disk = get_disk_by_path(name)
        if not disk:
            raise RpcException(errno.ENOENT, "Disk {0} not found".format(name))

        return disk

    @accepts(str)
    def get_partition_config(self, part_name):
        for name, disk in diskinfo_cache.itervalid():
            if 'partitions' not in disk:
                continue

            for part in disk['partitions']:
                if part_name in part['paths']:
                    result = part.copy()
                    result['disk'] = disk['path']
                    return result

        raise RpcException(errno.ENOENT, "Partition {0} not found".format(part_name))

    @private
    def update_disk_cache(self, disk):
        with self.dispatcher.get_lock('diskcache:{0}'.format(disk)):
            update_disk_cache(self.dispatcher, disk)

    @accepts(str)
    def path_to_id(self, path):
        disk_info = self.dispatcher.call_sync(
            'disk.query',
            [('path', '=', path), ('online', '=', True)],
            {'single': True}
        )
        return disk_info['id'] if disk_info else None


@description(
    "GPT formats the given disk with the filesystem type and parameters(optional) specified"
)
@accepts(str, str, h.object())
class DiskGPTFormatTask(Task):
    def describe(self, id, fstype, params=None):
        disk = disk_by_id(self.dispatcher, id)
        return "Formatting disk {0}".format(os.path.basename(disk['path']))

    def verify(self, id, fstype, params=None):
        disk = disk_by_id(self.dispatcher, id)
        if not get_disk_by_path(disk['path']):
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        if fstype not in ['freebsd-zfs']:
            raise VerifyException(errno.EINVAL, "Unsupported fstype {0}".format(fstype))

        allocation = self.dispatcher.call_sync(
            'volume.get_disks_allocation',
            [disk['path']]
        ).get(disk['path'])

        if allocation is not None:
            raise VerifyException(
                errno.EINVAL,
                "Cannot perform format operation on an allocated disk {0}".format(disk['path'])
            )

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, fstype, params=None):
        disk = disk_by_id(self.dispatcher, id)
        if params is None:
            params = {}

        blocksize = params.pop('blocksize', 4096)
        swapsize = params.pop('swapsize', 2048)
        bootcode = params.pop('bootcode', '/boot/pmbr-datadisk')

        try:
            system('/sbin/gpart', 'destroy', '-F', disk['path'])
        except SubprocessException:
            # ignore
            pass

        try:
            with self.dispatcher.get_lock('diskcache:{0}'.format(disk['path'])):
                system('/sbin/gpart', 'create', '-s', 'gpt', disk['path'])
                if swapsize > 0:
                    system('/sbin/gpart', 'add', '-a', str(blocksize), '-b', '128', '-s', '{0}M'.format(swapsize), '-t', 'freebsd-swap', disk['path'])
                    system('/sbin/gpart', 'add', '-a', str(blocksize), '-t', fstype, disk['path'])
                else:
                    system('/sbin/gpart', 'add', '-a', str(blocksize), '-b', '128', '-t', fstype, disk['path'])

                system('/sbin/gpart', 'bootcode', '-b', bootcode, disk['path'])

            self.dispatcher.call_sync('disk.update_disk_cache', disk['path'], timeout=120)
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Cannot format disk: {0}'.format(err.err))


@description('Formats given disk to be bootable and capable to be included in the Boot Pool')
@accepts(str)
class DiskBootFormatTask(Task):
    def describe(self, id):
        disk = disk_by_id(self.dispatcher, id)
        return "Formatting bootable disk {0}".format(disk['path'])

    def verify(self, id):
        disk = disk_by_id(self.dispatcher, id)
        if not get_disk_by_path(disk['path']):
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id):
        disk = disk_by_id(self.dispatcher, id)
        try:
            system('/sbin/gpart', 'destroy', '-F', disk['path'])
        except SubprocessException:
            # ignore
            pass

        try:
            system('/sbin/gpart', 'create', '-s', 'gpt', disk['path'])
            system('/sbin/gpart', 'add', '-t', 'bios-boot', '-i', '1', '-s', '512k', disk['path'])
            system('/sbin/gpart', 'add', '-t', 'freebsd-zfs', '-i', '2', '-a', '4k', disk['path'])
            system('/sbin/gpart', 'set', '-a', 'active', disk['path'])
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Cannot format disk: {0}'.format(err.err))


@description("Installs Bootloader (grub) on specified disk")
@accepts(str)
class DiskInstallBootloaderTask(Task):
    def describe(self, id):
        disk = disk_by_id(self.dispatcher, id)
        return "Installing bootloader on disk {0}".format(disk['path'])

    def verify(self, id):
        disk = disk_by_id(self.dispatcher, id)
        if not get_disk_by_path(disk['path']):
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id):
        try:
            disk = disk_by_id(self.dispatcher, id)
            system('/usr/local/sbin/grub-install', "--modules='zfs part_gpt'", disk['path'])
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Cannot install GRUB: {0}'.format(err.err))


@description("Erases the given Disk with erasure method specified (default: QUICK)")
@accepts(str, h.ref('disk-erase-method'))
class DiskEraseTask(Task):
    def __init__(self, dispatcher, datastore):
        super(DiskEraseTask, self).__init__(dispatcher, datastore)
        self.started = False
        self.mediasize = 0
        self.remaining = 0

    def verify(self, id, erase_method=None):
        disk = disk_by_id(self.dispatcher, id)
        if not get_disk_by_path(disk['path']):
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        allocation = self.dispatcher.call_sync(
            'volume.get_disks_allocation',
            [disk['path']]
        ).get(disk['path'])

        if allocation is not None:
            raise VerifyException(
                errno.EINVAL,
                "Cannot perform erase operation on an allocated disk {0}".format(disk['path'])
            )

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, erase_method=None):
        disk = disk_by_id(self.dispatcher, id)
        try:
            system('/sbin/zpool', 'labelclear', '-f', disk['path'])
            self.dispatcher.call_sync('disk.update_disk_cache', disk['path'], timeout=120)
            diskinfo = self.dispatcher.call_sync('disk.get_disk_config', disk['path'])
            if diskinfo.get('partitions'):
                system('/sbin/gpart', 'destroy', '-F', disk['path'])
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Cannot erase disk: {0}'.format(err.err))

        if not erase_method:
            erase_method = 'QUICK'

        zeros = b'\0' * (1024 * 1024)
        fd = os.open(disk['path'], os.O_WRONLY)

        if erase_method == 'QUICK':
            os.write(fd, zeros)
            os.lseek(fd, diskinfo['mediasize'] - len(zeros), os.SEEK_SET)
            os.write(fd, zeros)

        if erase_method in ('ZEROS', 'RANDOM'):
            self.mediasize = diskinfo['mediasize']
            self.remaining = self.mediasize
            self.started = True

            while self.remaining > 0:
                garbage = zeros if erase_method == 'ZEROS' else os.urandom(1024 * 1024)
                amount = min(len(garbage), self.remaining)
                os.write(fd, garbage[:amount])
                self.remaining -= amount

        os.close(fd)

    def get_status(self):
        if not self.started:
            return TaskStatus(0, 'Erasing disk...')

        return TaskStatus((self.mediasize - self.remaining) / float(self.mediasize), 'Erasing disk...')


@description("Configures online disk parameters")
@accepts(
    str,
    h.all_of(
        h.ref('disk'),
        h.no(h.required('name', 'serial', 'path', 'id', 'mediasize', 'status', 'description'))
    )
)
class DiskConfigureTask(Task):
    def verify(self, id, updated_fields):
        disk = disk_by_id(self.dispatcher, id)

        if not disk:
            raise VerifyException(errno.ENOENT, 'Disk {0} not found'.format(id))

        if not self.dispatcher.call_sync('disk.is_online', disk['path']):
            raise VerifyException(errno.EINVAL, 'Cannot configure offline disk')

        if not disk['status']['smart_capable']:
            if 'smart' in updated_fields or 'smart_options' in updated_fields:
                raise VerifyException(errno.EINVAL, 'Disk is not SMART capable')

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, updated_fields):
        disk = self.datastore.get_by_id('disks', id)
        disk.update(updated_fields)
        self.datastore.update('disks', disk['id'], disk)

        if {'standby_mode', 'apm_mode', 'acoustic_level'} & set(updated_fields):
            configure_disk(self.datastore, id)

        if 'smart' in updated_fields or 'smart_options' in updated_fields:
            if 'smart_options' in updated_fields:
                self.dispatcher.call_sync('etcd.generation.generate_group', 'smartd')
                self.dispatcher.call_sync('service.reload', 'smartd')
            elif 'smart' in updated_fields:
                # If just 'smart' (i.e. whether the device should be smart enabled or not was)
                # toggled then do not regenerate entier smartd conf for that, just toggle that
                # disk's smart enabled thing.
                disk_status = self.dispatcher.call_sync('disk.get_disk_config_by_id', id)
                device_smart_handle = Device(disk_status['gdisk_name'], abridged=True)
                if updated_fields['smart'] != device_smart_handle.smart_enabled:
                    toggle_result = device_smart_handle.smart_toggle(
                        'on' if updated_fields['smart'] else 'off'
                    )
                    if not toggle_result[0]:
                        raise TaskException(
                            errno.EINVAL,
                            "Tried to toggle {0}".format(disk['path']) +
                            " SMART enabled to: {0} and failed with error: {1}".format(
                                updated_fields['smart'],
                                toggle_result[1]
                            )
                        )
            self.dispatcher.call_sync('disk.update_disk_cache', disk['path'], timeout=120)

        self.dispatcher.dispatch_event('disk.changed', {
            'operation': 'update',
            'ids': [disk['id']]
        })


@description("Deletes offline disk configuration from database")
@accepts(str)
class DiskDeleteTask(Task):
    def verify(self, id):
        disk = self.datastore.get_by_id('disks', id)

        if not disk:
            raise VerifyException(errno.ENOENT, 'Disk {0} not found'.format(id))

        if self.dispatcher.call_sync('disk.is_online', disk['path']):
            raise VerifyException(errno.EINVAL, 'Cannot delete online disk')

        return ['disk:{0}'.format(os.path.basename(disk['path']))]

    def run(self, id):
        self.datastore.delete('disks', id)


@accepts(str, h.ref('disk-attach-params'))
class DiskGELIInitTask(Task):
    def describe(self, id, params=None):
        disk = disk_by_id(self.dispatcher, id)
        return "Creating encrypted partition for {0}".format(os.path.basename(disk['path']))

    def verify(self, id, params=None):
        disk = disk_by_id(self.dispatcher, id)
        if params is None:
            params = {}

        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        key = params.get('key', None)
        if key is None:
            raise VerifyException(errno.EINVAL, "No key for encryption specified")

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, params=None):
        if params is None:
            params = {}
        key = base64.b64decode(params.get('key', None))
        password = params.get('password', None)
        disk_info = disk_by_id(self.dispatcher, id)
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = disk_status.get('data_partition_path')
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        try:
            system('/sbin/geli', 'kill', data_partition_path)
        except SubprocessException:
            # ignore
            pass

        with tempfile.NamedTemporaryFile('wb') as keyfile:
            keyfile.write(key)
            keyfile.flush()
            try:
                if password is not None:
                    with tempfile.NamedTemporaryFile('w') as passfile:
                        passfile.write(password)
                        passfile.flush()
                        system('/sbin/geli', 'init', '-s', str(4096), '-K', keyfile.name, '-J', passfile.name,
                               '-B none', data_partition_path)
                else:
                    system('/sbin/geli', 'init', '-s', str(4096), '-K', keyfile.name, '-P', '-B none',
                           data_partition_path)
            except SubprocessException as err:
                raise TaskException(errno.EFAULT, 'Cannot init encrypted partition: {0}'.format(err.err))


@accepts(str, h.ref('disk-set-key-params'))
class DiskGELISetUserKeyTask(Task):
    def describe(self, id, params=None):
        disk = disk_by_id(self.dispatcher, id)
        return "Set new key for encrypted partition on {0}".format(os.path.basename(disk['path']))

    def verify(self, id, params=None):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        if params.get('key', None) is None:
            raise VerifyException(errno.EINVAL, "No key specified for operation")

        if params.get('slot', None) not in [0,1]:
            raise VerifyException(errno.EINVAL, "Chosen key slot value {0} is not in valid range [0-1]".
                                  format(params.get('slot', None)))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, params=None):
        if params is None:
            params = {}
        key = base64.b64decode(params.get('key', None))
        password = params.get('password', None)
        slot = params.get('slot', None)
        disk_info = disk_by_id(self.dispatcher, id)
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = os.path.join('/dev/gptid/', disk_status.get('data_partition_uuid'))
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        with tempfile.NamedTemporaryFile('wb') as keyfile:
            keyfile.write(key)
            keyfile.flush()
            try:
                if password is not None:
                    with tempfile.NamedTemporaryFile('w') as passfile:
                        passfile.write(password)
                        passfile.flush()
                        system('/sbin/geli', 'setkey', '-K', keyfile.name, '-J', passfile.name,
                               '-n', str(slot), data_partition_path)
                else:
                    system('/sbin/geli', 'setkey', '-K', keyfile.name, '-P', '-n', str(slot),
                           data_partition_path)
            except SubprocessException as err:
                raise TaskException(errno.EFAULT, 'Cannot set new key for encrypted partition: {0}'.format(err.err))


@accepts(str, int)
class DiskGELIDelUserKeyTask(Task):
    def describe(self, id, slot):
        disk = disk_by_id(self.dispatcher, id)
        return "Delete key of encrypted partition on {0}".format(os.path.basename(disk['path']))

    def verify(self, id, slot):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        if slot not in [0, 1]:
            raise VerifyException(errno.EINVAL, "Chosen key slot value {0} is not in valid range [0-1]".format(slot))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, slot):
        disk_info = disk_by_id(self.dispatcher, id)
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = os.path.join('/dev/gptid/', disk_status.get('data_partition_uuid'))
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        try:
            system('/sbin/geli', 'delkey', '-n', str(slot), data_partition_path)
        except SubprocessException as err:
            raise TaskException(errno.EFAULT, 'Cannot delete key of encrypted partition: {0}'.format(err.err))


@accepts(str)
@returns(h.ref('disk-metadata'))
class DiskGELIBackupMetadataTask(Task):
    def describe(self, id):
        disk = disk_by_id(self.dispatcher, id)
        return "Backup metadata of encrypted partition on {0}".format(os.path.basename(disk['path']))

    def verify(self, id):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id):
        disk_info = disk_by_id(self.dispatcher, id)
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = os.path.join('/dev/gptid/', disk_status.get('data_partition_uuid'))
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        with tempfile.NamedTemporaryFile('w+b') as metadata_file:
            try:
                system('/sbin/geli', 'backup', data_partition_path, metadata_file.name)
            except SubprocessException as err:
                raise TaskException(errno.EFAULT, 'Cannot backup metadata of encrypted partition: {0}'.format(err.err))

            metadata_file.seek(0)
            return {'disk': disk_info['path'], 'metadata': base64.b64encode(metadata_file.read()).decode('utf-8')}


@accepts(str, h.ref('disk-metadata'))
class DiskGELIRestoreMetadataTask(Task):
    def describe(self, id, metadata):
        return "Restore metadata of encrypted partition on {0}".format(os.path.basename(metadata.get('disk')))

    def verify(self, id, metadata):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, metadata):
        disk = metadata.get('disk')
        disk_info = self.dispatcher.call_sync('disk.query', [('path', 'in', disk),
                                                             ('online', '=', True)], {'single': True})
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = os.path.join('/dev/gptid/', disk_status.get('data_partition_uuid'))
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk))

        with tempfile.NamedTemporaryFile('w+b') as metadata_file:
            metadata_file.write(base64.b64decode(metadata.get('metadata').encode('utf-8')))
            metadata_file.flush()
            try:
                system('/sbin/geli', 'restore', '-f', metadata_file.name, data_partition_path)
            except SubprocessException as err:
                raise TaskException(errno.EFAULT, 'Cannot restore metadata of encrypted partition: {0}'.format(err.err))


@accepts(str, h.ref('disk-attach-params'))
class DiskGELIAttachTask(Task):
    def describe(self, id, params=None):
        disk = disk_by_id(self.dispatcher, id)
        return "Attach encrypted partition of {0}".format(os.path.basename(disk['path']))

    def verify(self, id, params=None):
        if params is None:
            params = {}
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        key = params.get('key', None)
        if key is None:
            raise VerifyException(errno.EINVAL, "No key for attach specified")

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id, params=None):
        if params is None:
            params = {}
        key = base64.b64decode(params.get('key', None))
        password = params.get('password', None)
        disk_info = disk_by_id(self.dispatcher, id)
        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = disk_status.get('data_partition_path')
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        with tempfile.NamedTemporaryFile('wb') as keyfile:
            keyfile.write(key)
            keyfile.flush()
            try:
                if password is not None:
                    with tempfile.NamedTemporaryFile('w') as passfile:
                        passfile.write(password)
                        passfile.flush()
                        system('/sbin/geli', 'attach', '-k', keyfile.name, '-j', passfile.name, data_partition_path)
                else:
                    system('/sbin/geli', 'attach', '-k', keyfile.name, '-p', data_partition_path)
                self.dispatcher.call_sync('disk.update_disk_cache', disk_info['path'], timeout=120)
            except SubprocessException as err:
                logger.warning('Cannot attach encrypted partition: {0}'.format(err.err))


@accepts(str)
class DiskGELIDetachTask(Task):
    def describe(self, id):
        disk = disk_by_id(self.dispatcher, id)
        return "Detach encrypted partition of {0}".format(os.path.basename(disk['path']))

    def verify(self, id):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id):
        disk_info = disk_by_id(self.dispatcher, id)

        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = disk_status.get('data_partition_path')
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        try:
            system('/sbin/geli', 'detach', '-f', data_partition_path)
            self.dispatcher.call_sync('disk.update_disk_cache', disk_info['path'], timeout=120)
        except SubprocessException as err:
            logger.warning('Cannot detach encrypted partition: {0}'.format(err.err))


@accepts(str)
class DiskGELIKillTask(Task):
    def describe(self, id):
        disk = disk_by_id(self.dispatcher, id)
        return "Kill encrypted partition of {0}".format(os.path.basename(disk['path']))

    def verify(self, id):
        disk = disk_by_id(self.dispatcher, id)
        if not disk:
            raise VerifyException(errno.ENOENT, "Disk {0} not found".format(id))

        return ['disk:{0}'.format(disk['path'])]

    def run(self, id):
        disk_info = disk_by_id(self.dispatcher, id)

        disk_status = disk_info.get('status', None)
        if disk_status is not None:
            data_partition_path = disk_status.get('data_partition_path')
        else:
            raise TaskException(errno.EINVAL, 'Cannot get disk status for: {0}'.format(disk_info['path']))

        try:
            system('/sbin/geli', 'kill', data_partition_path)
            self.dispatcher.call_sync('disk.update_disk_cache', disk_info['path'], timeout=120)
        except SubprocessException as err:
            logger.warning('Cannot kill encrypted partition: {0}'.format(err.err))


@description("Performs SMART test on disk")
@accepts(str, h.ref('disk-selftest-type'))
class DiskTestTask(ProgressTask):
    def verify(self, id, test_type):
        disk = diskinfo_cache.get(id)
        if not disk:
            raise VerifyException(errno.ENOENT, 'Disk {0} not found'.format(id))

        return ['disk:{0}'.format(disk['path'])]

    def handle_progress(self, progress):
        self.set_progress(progress)

    def run(self, id, test_type):
        try:
            diskinfo = self.dispatcher.call_sync('disk.get_disk_config_by_id', id)
        except RpcException:
            raise TaskException(errno.ENOENT, 'Disk {0} not found'.format(id))

        dev = Device(diskinfo['gdisk_name'])
        dev.run_selftest_and_wait(
            getattr(SelfTestType, test_type).value,
            progress_handler=self.handle_progress
        )
        self.dispatcher.call_sync('disk.update_disk_cache', diskinfo['path'], timeout=120)


@description("Performs the given SMART test on the disk IDs specified (in parallel)")
@accepts(h.array(str), h.ref('disk-selftest-type'))
class DiskParallelTestTask(ProgressTask):
    def verify(self, ids, test_type):
        res = []
        for i in ids:
            disk = diskinfo_cache.get(i)
            if not disk:
                raise VerifyException(errno.ENOENT, 'Disk {0} not found'.format(id))

            res.append('disk:{0}'.format(i['path']))

        return res

    def run(self, ids, test_type):
        subtasks = []
        disks = self.dispatcher.call_sync('disk.query', [('id', 'in', ids)])
        for d in disks:
            subtasks.append(self.run_subtask('disk.test', d['id'], test_type))

        self.join_subtasks(subtasks)


def get_twcli(controller):
    re_port = re.compile(r'^p(?P<port>\d+).*?\bu(?P<unit>\d+)\b', re.S | re.M)
    output, err = system("/usr/local/sbin/tw_cli", "/c{0}".format(controller), "show")

    units = {}
    for port, unit in re_port.findall(output):
        units[int(unit)] = int(port)

    return units


def device_to_identifier(name, serial=None):
    gdisk = geom.geom_by_name('DISK', name)
    if not gdisk:
        return None

    if 'lunid' in gdisk.provider.config:
        return "lunid:{0}".format(gdisk.provider.config['lunid'])

    if serial:
        return "serial:{0}".format(serial)

    gpart = geom.geom_by_name('PART', name)
    if gpart:
        for i in gpart.providers:
            if i.config['type'] in ('freebsd-zfs', 'freebsd-ufs'):
                return "uuid:{0}".format(i.config['rawuuid'])

    glabel = geom.geom_by_name('LABEL', name)
    if glabel and glabel.provider:
        return "label:{0}".format(glabel.provider.name)

    return "devicename:{0}".format(os.path.join('/dev', name))


def info_from_device(devname):
    disk_info = {
        'serial': None,
        'max_rotation': None,
        'smart_enabled': False,
        'smart_capable': False,
        'smart_status': None,
        'model': None,
        'is_ssd': False,
        'interface': None
    }

    # TODO, fix this to deal with above generated args for interface
    # whilst obtaining disk info, get smart health assessment via a greenlet
    health_assessment_greenlet = gevent.spawn(smart_health_assement, devname)
    dev_smart_info = Device(os.path.join('/dev/', devname), abridged=True)
    health_assessment_greenlet.join()
    disk_info['is_ssd'] = dev_smart_info.is_ssd
    disk_info['smart_capable'] = dev_smart_info.smart_capable
    disk_info['serial'] = dev_smart_info.serial
    if dev_smart_info.smart_capable:
        disk_info['model'] = dev_smart_info.model
        disk_info['max_rotation'] = dev_smart_info.rotation_rate
        disk_info['interface'] = dev_smart_info.interface
        disk_info['smart_enabled'] = dev_smart_info.smart_enabled
        if dev_smart_info.smart_enabled:
            disk_info['smart_status'] = health_assessment_greenlet.value

    return disk_info


def get_disk_by_path(path):
    for disk in diskinfo_cache.validvalues():
        if disk['path'] == path:
            return disk

        if disk['is_multipath']:
            if path in disk['multipath.members']:
                return disk

    return None


def get_disk_by_lunid(lunid):
    return wrap(first_or_default(lambda d: d['lunid'] == lunid, diskinfo_cache.validvalues()))


def clean_multipaths(dispatcher):
    global multipaths

    geom.scan()
    cls = geom.class_by_name('MULTIPATH')
    if cls:
        for i in cls.geoms:
            logger.info('Destroying multipath device %s', i.name)
            dispatcher.exec_and_wait_for_event(
                'system.device.detached',
                lambda args: args['path'] == '/dev/multipath/{0}'.format(i.name),
                lambda: system('/sbin/gmultipath', 'destroy', i.name)
            )

    multipaths = -1


def get_multipath_name():
    global multipaths

    multipaths += 1
    return 'multipath{0}'.format(multipaths)


def attach_to_multipath(dispatcher, disk, ds_disk, path):
    if not disk and ds_disk:
        logger.info("Device node %s <%s> is marked as multipath, creating single-node multipath", path, ds_disk['serial'])
        nodename = os.path.basename(ds_disk['path'])
        logger.info('Reusing %s path', nodename)

        # Degenerated single-disk multipath
        try:
            dispatcher.exec_and_wait_for_event(
                'system.device.attached',
                lambda args: args['path'] == '/dev/multipath/{0}'.format(nodename),
                lambda: system('/sbin/gmultipath', 'create', nodename, path)
            )
        except SubprocessException as e:
            logger.warning('Cannot create multipath: {0}'.format(e.err))
            return

        ret = {
            'is_multipath': True,
            'path': os.path.join('/dev/multipath', nodename),
        }
    elif disk:
        logger.info("Device node %s is another path to disk <%s> (%s)", path, disk['id'], disk['description'])
        if disk['is_multipath']:
            if path in disk['multipath.members']:
                # Already added
                return

            # Attach new disk
            try:
                system('/sbin/gmultipath', 'add', disk['multipath.node'], path)
            except SubprocessException as e:
                logger.warning('Cannot attach {0} to multipath: {0}'.format(path, e.err))
                return

            nodename = disk['multipath.node']
            ret = {
                'is_multipath': True,
                'path': os.path.join('/dev/multipath', disk['multipath.node']),
            }
        else:
            # Create new multipath
            logger.info('Creating new multipath device')

            # If disk was previously tied to specific cdev path (/dev/multipath[0-9]+)
            # reuse that path. Otherwise pick up first multipath device name available
            if ds_disk and ds_disk['is_multipath']:
                nodename = os.path.basename(ds_disk['path'])
                logger.info('Reusing %s path', nodename)
            else:
                nodename = get_multipath_name()
                logger.info('Using new %s path', nodename)

            try:
                dispatcher.exec_and_wait_for_event(
                    'system.device.attached',
                    lambda args: args['path'] == '/dev/multipath/{0}'.format(nodename),
                    lambda: system('/sbin/gmultipath', 'create', nodename, disk['path'], path)
                )
            except SubprocessException as e:
                logger.warning('Cannot create multipath: {0}'.format(e.err))
                return

            ret = {
                'is_multipath': True,
                'path': os.path.join('/dev/multipath', nodename),
            }

    geom.scan()
    gmultipath = geom.geom_by_name('MULTIPATH', nodename)
    ret['multipath'] = generate_multipath_info(gmultipath)
    return ret


def disk_by_id(dispatcher, id):
    return dispatcher.call_sync('disk.query', [('id', '=', id)], {'single': True})


def generate_partitions_list(gpart):
    if not gpart:
        return

    for p in gpart.providers:
        paths = [os.path.join("/dev", p.name)]
        if not p.config:
            continue

        label = p.config.get('label')
        uuid = p.config.get('rawuuid')
        eli = geom.geom_by_name('ELI', 'gptid/{0}.eli'.format(uuid))

        if label:
            paths.append(os.path.join("/dev/gpt", label))

        if uuid:
            if eli:
                elipath = uuid + '.eli'
                paths.append(os.path.join("/dev/gptid", elipath))
            else:
                paths.append(os.path.join("/dev/gptid", uuid))

        yield {
            'name': p.name,
            'paths': paths,
            'mediasize': int(p.mediasize),
            'uuid': uuid,
            'type': p.config['type'],
            'label': p.config.get('label'),
            'encrypted': True if eli else False
        }


def generate_multipath_info(gmultipath):
    return {
        'status': gmultipath.config['State'],
        'mode': gmultipath.config['Mode'],
        'uuid': gmultipath.config['UUID'],
        'node': gmultipath.name,
        'members': {os.path.join('/dev', c.provider.name): c.config['State'] for c in gmultipath.consumers}
    }


def update_disk_cache(dispatcher, path):
    geom.scan()
    name = re.match('/dev/(.*)', path).group(1)
    gdisk = geom.geom_by_name('DISK', name)
    gpart = geom.geom_by_name('PART', name)
    gmultipath = None

    # Handle diskid labels
    if gpart is None:
        glabel = geom.geom_by_name('LABEL', name)
        if glabel and glabel.provider and glabel.provider.name.startswith('diskid/'):
            gpart = geom.geom_by_name('PART', glabel.provider.name)

    if name.startswith('multipath/'):
        multipath_name = re.match('multipath/(.*)', name).group(1)
        gmultipath = geom.geom_by_name('MULTIPATH', multipath_name)

    disk = get_disk_by_path(path)
    if not disk:
        return

    old_id = disk['id']

    if gmultipath:
        # Path represents multipath device (not disk device)
        # MEDIACHANGE event -> use first member for hardware queries
        cons = next(gmultipath.consumers)
        gdisk = cons.provider.geom

    if not gdisk:
        return

    disk_info = info_from_device(gdisk.name)
    serial = disk_info['serial']

    provider = gdisk.provider
    partitions = list(generate_partitions_list(gpart))
    identifier = device_to_identifier(gdisk.name, serial)
    data_part = first_or_default(lambda x: x['type'] == 'freebsd-zfs', partitions)
    data_uuid = data_part["uuid"] if data_part else None

    encrypted = False
    if data_part:
        if data_part["encrypted"]:
            data_path = data_uuid + '.eli'
            encrypted = True
        else:
            data_path = data_uuid

    swap_part = first_or_default(lambda x: x['type'] == 'freebsd-swap', partitions)
    swap_uuid = swap_part["uuid"] if swap_part else None

    disk.update({
        'mediasize': provider.mediasize,
        'sectorsize': provider.sectorsize,
        'max_rotation': disk_info['max_rotation'],
        'smart_capable': disk_info['smart_capable'],
        'smart_enabled': disk_info['smart_enabled'],
        'smart_status': disk_info['smart_status'],
        'id': identifier,
        'schema': gpart.config.get('scheme') if gpart else None,
        'empty': len(partitions) == 0,
        'partitions': partitions,
        'data_partition_uuid': data_uuid,
        'data_partition_path': os.path.join("/dev/gptid", data_path) if data_uuid else None,
        'swap_partition_uuid': swap_uuid,
        'swap_partition_path': os.path.join("/dev/gptid", swap_uuid) if swap_uuid else None,
        'encrypted': encrypted,
        'gdisk_name': gdisk.name,
    })

    if gmultipath:
        disk['multipath'] = generate_multipath_info(gmultipath)

    # Purge old cache entry if identifier has changed
    if old_id != identifier:
        logger.debug('Removing disk cache entry for <%s> because identifier changed', old_id)
        diskinfo_cache.remove(old_id)
        diskinfo_cache.put(identifier, disk)
        dispatcher.datastore.delete('disks', old_id)
        dispatcher.dispatch_event('disk.changed', {
            'operation': 'rename',
            'ids': [[old_id, identifier]]
        })

    persist_disk(dispatcher, disk)


def generate_disk_cache(dispatcher, path):
    diskinfo_cache_lock.acquire()
    geom.scan()
    name = os.path.basename(path)
    gdisk = geom.geom_by_name('DISK', name)
    multipath_info = None

    disk_info = info_from_device(gdisk.name)
    serial = disk_info['serial']
    identifier = device_to_identifier(name, serial)
    ds_disk = dispatcher.datastore.get_by_id('disks', identifier)

    # Path repesents disk device (not multipath device) and has NAA ID attached
    lunid = gdisk.provider.config.get('lunid')
    if lunid:
        # Check if device could be part of multipath configuration
        d = get_disk_by_lunid(lunid)
        if (d and d['path'] != path) or (ds_disk and ds_disk['is_multipath']):
            multipath_info = attach_to_multipath(dispatcher, d, ds_disk, path)

    provider = gdisk.provider
    try:
        camdev = CamDevice(gdisk.name)
    except RuntimeError:
        camdev = None

    disk = wrap({
        'path': path,
        'is_multipath': False,
        'description': provider.config['descr'],
        'serial': serial,
        'lunid': provider.config.get('lunid'),
        'model': disk_info['model'],
        'interface': disk_info['interface'],
        'is_ssd': disk_info['is_ssd'],
        'id': identifier,
        'controller': camdev.__getstate__() if camdev else None,
    })

    if multipath_info:
        disk.update(multipath_info)
        path = multipath_info['path']

    diskinfo_cache.put(identifier, disk)
    update_disk_cache(dispatcher, path)
    configure_disk(dispatcher.datastore, identifier)

    logger.info('Added <%s> (%s) to disk cache', identifier, disk['description'])
    diskinfo_cache_lock.release()


def purge_disk_cache(dispatcher, path):
    geom.scan()
    delete = False
    disk = get_disk_by_path(path)

    if not disk:
        return

    if disk['is_multipath']:
        # Looks like one path was removed
        logger.info('Path %s to disk <%s> (%s) was removed', path, disk['id'], disk['description'])
        disk['multipath.members'].remove(path)

        # Was this last path?
        if len(disk['multipath.members']) == 0:
            logger.info('Disk %s <%s> (%s) was removed (last path is gone)', path, disk['id'], disk['description'])
            diskinfo_cache.remove(disk['id'])
            delete = True
        else:
            diskinfo_cache.put(disk['id'], disk)

    else:
        logger.info('Disk %s <%s> (%s) was removed', path, disk['id'], disk['description'])
        diskinfo_cache.remove(disk['id'])
        delete = True

    if delete:
        # Mark disk for auto-delete
        ds_disk = dispatcher.datastore.get_by_id('disks', disk['id'])
        ds_disk['delete_at'] = datetime.utcnow() + EXPIRE_TIMEOUT
        dispatcher.datastore.update('disks', ds_disk['id'], ds_disk)

    dispatcher.dispatch_event('disk.changed', {
        'operation': 'update',
        'ids': [disk['id']]
    })


def persist_disk(dispatcher, disk):
    ds_disk = dispatcher.datastore.get_by_id('disks', disk['id'])
    new = ds_disk is None
    ds_disk = ds_disk or {}
    ds_disk.update({
        'lunid': disk['lunid'],
        'path': disk['path'],
        'mediasize': disk['mediasize'],
        'serial': disk['serial'],
        'is_multipath': disk['is_multipath'],
        'data_partition_uuid': disk['data_partition_uuid'],
        'delete_at': None
    })

    if 'standby_mode' not in ds_disk:
        ds_disk.update({'standby_mode': 0})

    if 'apm_mode' not in ds_disk:
        ds_disk.update({'apm_mode': 0})

    if 'acoustic_level' not in ds_disk:
        ds_disk.update({'acoustic_level': 'DISABLED'})

    if 'smart' not in ds_disk:
        ds_disk.update({'smart': True if disk['smart_capable'] else False})

    if 'smart_options' not in ds_disk:
        ds_disk.update({'smart_options': None})

    dispatcher.datastore.upsert('disks', disk['id'], ds_disk)
    dispatcher.dispatch_event('disk.changed', {
        'operation': 'create' if new else 'update',
        'ids': [disk['id']]
    })


def configure_disk(datastore, id):
    disk = datastore.get_by_id('disks', id)
    acc_level = getattr(AcousticLevel, disk.get('acoustic_level', 'DISABLED')).value
    powermgmt = disk.get('apm_mode', 0)
    try:
        system('/usr/local/sbin/ataidle', '-P', str(powermgmt), '-A', str(acc_level), disk['path'])
    except SubprocessException as err:
        logger.warning('Cannot configure power management for disk {0}: {1}'.format(id, err.err))

    if disk.get('standby_mode'):
        def configure_standby(mode):
            try:
                system(
                    '/usr/local/sbin/ataidle',
                    '-I',
                    mode,
                    disk['path']
                )
            except SubprocessException as err:
                logger.warning('Cannot configure standby mode for disk {0}: {1}', id, err.err)

        standby_mode = str(disk['standby_mode'])
        gevent.spawn_later(60, configure_standby, standby_mode)


def _depends():
    return ['DevdPlugin']


def _init(dispatcher, plugin):
    def on_device_attached(args):
        path = args['path']
        if re.match(r'^/dev/(da|ada|vtbd|multipath/multipath)[0-9]+$', path):
            if not dispatcher.resource_exists('disk:{0}'.format(path)):
                dispatcher.register_resource(Resource('disk:{0}'.format(path)))

        if re.match(r'^/dev/(da|ada|vtbd)[0-9]+$', path):
            # Regenerate disk cache
            logger.info("New disk attached: {0}".format(path))
            with dispatcher.get_lock('diskcache:{0}'.format(path)):
                generate_disk_cache(dispatcher, path)

    def on_device_detached(args):
        path = args['path']
        if re.match(r'^/dev/(da|ada|vtbd)[0-9]+$', path):
            logger.info("Disk %s detached", path)
            purge_disk_cache(dispatcher, path)

        if re.match(r'^/dev/(da|ada|vtbd|multipath/multipath)[0-9]+$', path):
            dispatcher.unregister_resource('disk:{0}'.format(path))

    def on_device_mediachange(args):
        # Regenerate caches
        path = args['path']
        if re.match(r'^/dev/(da|ada|vtbd|multipath/multipath)[0-9]+$', path):
            with dispatcher.get_lock('diskcache:{0}'.format(path)):
                logger.info('Updating disk cache for device %s', args['path'])
                update_disk_cache(dispatcher, args['path'])

    plugin.register_schema_definition('disk', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'serial': {'type': 'string'},
            'mediasize': {'type': 'integer'},
            'smart': {'type': 'boolean'},
            'smart_options': {'type': 'string'},
            'standby_mode': {'type': ['integer', 'null']},
            'apm_mode': {'type': ['integer', 'null']},
            'acoustic_level': {
                'type': 'string',
                'enum': ['DISABLED', 'MINIMUM', 'MEDIUM', 'MAXIMUM']
            },
            'status': {'$ref': 'disk-status'},
        }
    })

    plugin.register_schema_definition('disk-status', {
        'type': 'object',
        'properties': {
            'mediasize': {'type': 'integer'},
            'sectorsize': {'type': 'integer'},
            'description': {'type': 'string'},
            'serial': {'type': 'string'},
            'lunid': {'type': 'string'},
            'max_rotation': {'type': 'integer'},
            'smart_capable': {'type': 'boolean'},
            'smart_enabled': {'type': 'boolean'},
            'smart_status': {'type': 'string'},
            'model': {'type': 'string'},
            'interface': {'type': 'string'},
            'is_ssd': {'type': 'boolean'},
            'is_multipath': {'type': 'boolean'},
            'is_encrypted': {'type': 'boolean'},
            'id': {'type': 'string'},
            'schema': {'type': ['string', 'null']},
            'controller': {'type': 'object'},
            'empty': {'type': 'boolean'},
            'partitions': {
                'type': 'array',
                'items': {'$ref': 'disk-partition'}
            },
            'multipath': {
                'type': 'object',
                'properties': {
                    'status': {'type': 'string'},
                    'node': {'type': 'string'},
                    'members': {
                        'type': 'array',
                        'items': {'type': 'string'},
                    },
                }
            },
            'data_partition_uuid': {'type': 'string'},
            'data_partition_path': {'type': 'string'},
            'swap_partition_uuid': {'type': 'string'},
            'swap_partition_path': {'type': 'string'},
            'encrypted': {'type': 'boolean'},
            'gdisk_name': {'type': 'string'},
        }
    })

    plugin.register_schema_definition('disk-partition', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'paths': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'mediasize': {'type': 'integer'},
            'uuid': {'type': 'string'},
            'type': {'type': 'string'},
            'label': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('disk-erase-method', {
        'type': 'string',
        'enum': ['QUICK', 'ZEROS', 'RANDOM']
    })

    plugin.register_schema_definition('disk-selftest-type', {
        'type': 'string',
        'enum': list(SelfTestType.__members__.keys())
    })

    plugin.register_schema_definition('disk-attach-params', {
        'type': 'object',
        'properties': {
            'key': {'type': 'string'},
            'password': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('disk-set-key-params', {
        'type': 'object',
        'properties': {
            'key': {'type': 'string'},
            'password': {'type': 'string'},
            'slot': {'type': 'integer'}
        }
    })

    plugin.register_schema_definition('disk-metadata', {
        'type': 'object',
        'properties': {
            'disk': {'type': 'string'},
            'metadata': {'type': 'string'}
        }
    })

    plugin.register_provider('disk', DiskProvider)
    plugin.register_event_handler('system.device.attached', on_device_attached)
    plugin.register_event_handler('system.device.detached', on_device_detached)
    plugin.register_event_handler('system.device.mediachange', on_device_mediachange)
    plugin.register_task_handler('disk.erase', DiskEraseTask)
    plugin.register_task_handler('disk.format.gpt', DiskGPTFormatTask)
    plugin.register_task_handler('disk.format.boot', DiskBootFormatTask)
    plugin.register_task_handler('disk.geli.init', DiskGELIInitTask)
    plugin.register_task_handler('disk.geli.ukey.set', DiskGELISetUserKeyTask)
    plugin.register_task_handler('disk.geli.ukey.del', DiskGELIDelUserKeyTask)
    plugin.register_task_handler('disk.geli.mkey.backup', DiskGELIBackupMetadataTask)
    plugin.register_task_handler('disk.geli.mkey.restore', DiskGELIRestoreMetadataTask)
    plugin.register_task_handler('disk.geli.attach', DiskGELIAttachTask)
    plugin.register_task_handler('disk.geli.detach', DiskGELIDetachTask)
    plugin.register_task_handler('disk.geli.kill', DiskGELIKillTask)
    plugin.register_task_handler('disk.install_bootloader', DiskInstallBootloaderTask)
    plugin.register_task_handler('disk.update', DiskConfigureTask)
    plugin.register_task_handler('disk.delete', DiskDeleteTask)
    plugin.register_task_handler('disk.test', DiskTestTask)
    plugin.register_task_handler('disk.parallel_test', DiskParallelTestTask)

    plugin.register_event_type('disk.changed')

    # Start with marking all disks as unavailable
    for i in dispatcher.datastore.query('disks'):
        if not i.get('delete_at'):
            i['delete_at'] = datetime.utcnow() + EXPIRE_TIMEOUT

        dispatcher.datastore.update('disks', i['id'], i)

    # Destroy all existing multipaths
    clean_multipaths(dispatcher)

    # Generate cache for all disks
    for i in dispatcher.rpc.call_sync('system.device.get_devices', 'disk'):
        on_device_attached({'path': i['path']})
