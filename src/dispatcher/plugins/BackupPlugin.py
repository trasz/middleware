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
import errno
import socket
import logging
import threading
import hashlib
from datetime import datetime
from freenas.dispatcher.jsonenc import dumps, loads
from freenas.dispatcher.fd import FileDescriptor
from freenas.dispatcher.rpc import RpcException, accepts, returns, description, SchemaHelper as h
from task import Provider, Task, ProgressTask, VerifyException, TaskException
from freenas.utils import normalize, first_or_default
from freenas.utils.query import wrap


logger = logging.getLogger(__name__)
MANIFEST_FILENAME = 'FREENAS_MANIFEST'


class BackupProvider(Provider):
    def query(self, filter=None, params=None):
        def extend(backup):
            return backup

        return self.datastore.query('backup', *(filter or []), callback=extend, **(params or {}))

    @description("Returns list of supported backup providers")
    @accepts()
    @returns(h.ref('backup-types'))
    def supported_types(self):
        result = {}
        for p in list(self.dispatcher.plugins.values()):
            if p.metadata and p.metadata.get('type') == 'backup':
                result[p.metadata['method']] = {}

        return result


@accepts(h.all_of(
    h.ref('backup'),
    h.required('name', 'provider', 'dataset')
))
class CreateBackupTask(Task):
    def verify(self, backup):
        if 'id' in backup and self.datastore.exists('backup', ('id', '=', backup['id'])):
            raise VerifyException('Backup with ID {0} already exists'.format(backup['id']))

        if self.datastore.exists('backup', ('name', '=', backup['name'])):
            raise VerifyException('Backup with name {0} already exists'.format(backup['name']))

        return ['system']

    def run(self, backup):
        normalize(backup, {
            'properties': {}
        })

        id = self.datastore.insert('backup', backup)

        self.dispatcher.emit_event('backup.changed', {
            'operation': 'create',
            'ids': [id]
        })

        return id


@accepts(str, h.all_of(
    h.ref('backup'),
    h.forbidden('id', 'provider', 'dataset')
))
class UpdateBackupTask(Task):
    def verify(self, id, updated_params):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        backup = self.datastore.get_by_id('backup', id)
        backup.update(updated_params)
        self.datastore.update('backup', id, backup)

        self.dispatcher.emit_event('backup.changed', {
            'operation': 'update',
            'ids': [id]
        })

        return id


@accepts(str)
class DeleteBackupTask(Task):
    def verify(self, id):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def run(self, id):
        self.datastore.delete('backup', id)
        self.dispatcher.emit_event('backup.changed', {
            'operation': 'delete',
            'ids': [id]
        })


class BackupQueryTask(ProgressTask):
    def verify(self, id):
        return []

    def download(self, provider, props, path):
        rfd, wfd = os.pipe()
        fd = os.fdopen(rfd, 'rb')
        result = None

        def worker():
            nonlocal result
            result = fd.read()

        thr = threading.Thread(target=worker)
        thr.start()
        self.join_subtasks(self.run_subtask('backup.{0}.get'.format(provider), props, path, FileDescriptor(wfd)))
        thr.join(timeout=1)

        return result.decode('utf-8')

    def run(self, id):
        backup = self.datastore.get_by_id('backup', id)
        if not backup:
            raise RpcException(errno.ENOENT, 'Backup {0} not found'.format(id))

        dirlist, = self.join_subtasks(self.run_subtask(
            'backup.{0}.list'.format(backup['provider']),
            backup['properties']
        ))

        if not any(e['name'] == MANIFEST_FILENAME for e in dirlist):
            return None

        data = self.download(backup['provider'], backup['properties'], MANIFEST_FILENAME)
        try:
            manifest = loads(data)
        except ValueError:
            raise RpcException(errno.EINVAL, 'Invalid backup manifest')

        return manifest


@accepts(str, bool, bool)
class BackupSyncTask(ProgressTask):
    def verify(self, id, snapshot=True, dry_run=False):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def generate_manifest(self, backup, previous_manifest, actions):
        def make_snapshot_entry(action):
            snapname = '{0}@{1}'.format(action['localfs'], action['snapshot'])
            filename = hashlib.md5(snapname.encode('utf-8')).hexdigest()
            snap = wrap(self.dispatcher.call_sync(
                'volume.snapshot.query',
                [('id', '=', snapname)],
                {'single': True}
            ))

            return {
                'name': snapname,
                'anchor': action.get('anchor'),
                'incremental': action['incremental'],
                'created_at': datetime.fromtimestamp(int(snap['properties.creation.rawvalue'])),
                'uuid': snap.get('properties.org\\.freenas:uuid.value'),
                'filename': filename
            }

        snaps = previous_manifest['snapshots'][:] if previous_manifest else []
        new_snaps = []

        for a in actions:
            if a['type'] == 'SEND_STREAM':
                snap = make_snapshot_entry(a)
                snaps.append(snap)
                new_snaps.append(snap)

        manifest = {
            'hostname': socket.gethostname(),
            'dataset': backup['dataset'],
            'snapshots': snaps
        }

        return manifest, new_snaps

    def upload(self, provider, props, path, data):
        rfd, wfd = os.pipe()
        fd = os.fdopen(wfd, 'wb')

        def worker():
            x = fd.write(data.encode('utf-8'))
            os.close(wfd)
            logger.info('written {0} bytes'.format(x))

        thr = threading.Thread(target=worker)
        thr.start()
        self.dispatcher.call_task_sync('backup.{0}.put'.format(provider), props, path, FileDescriptor(rfd))
        thr.join(timeout=1)

    def run(self, id, snapshot=True, dry_run=False):
        # Check for previous manifest
        manifest = None
        snapshots = []
        backup = self.datastore.get_by_id('backup', id)

        try:
            manifest, = self.join_subtasks(self.run_subtask('backup.query', id))
            if manifest:
                snapshots = manifest['snapshots']
        except RpcException:
            pass

        self.set_progress(0, 'Calculating send delta')

        (actions, send_size), = self.join_subtasks(self.run_subtask(
            'replication.calculate_delta',
            backup['dataset'],
            backup['dataset'],
            snapshots,
            True,
            True
        ))

        if dry_run:
            return actions

        new_manifest, snaps = self.generate_manifest(backup, manifest, actions)

        for idx, i in enumerate(snaps):
            rfd, wfd = os.pipe()
            progress = float(idx) / len(snaps) * 100
            self.set_progress(progress, 'Uploading stream of {0}'.format(i['name']))
            self.join_subtasks(
                self.run_subtask('zfs.send', i['name'], i.get('anchor'), i.get('anchor'), FileDescriptor(wfd)),
                self.run_subtask(
                    'backup.{0}.put'.format(backup['provider']),
                    backup['properties'],
                    i['filename'],
                    FileDescriptor(rfd)
                )
            )

        self.set_progress(100, 'Writing backup manifest')
        self.upload(backup['provider'], backup['properties'], MANIFEST_FILENAME, dumps(new_manifest, indent=4))


class BackupRestoreTask(ProgressTask):
    def verify(self, id, dataset=None, snapshot=None):
        return []

    def run(self, id, dataset=None, snapshot=None):
        backup = self.datastore.get_by_id('backup', id)
        if not backup:
            raise TaskException(errno.ENOENT, 'Backup {0} not found'.format(backup['id']))

        manifest, = self.join_subtasks(self.run_subtask('backup.query', id))
        if not manifest:
            raise TaskException(errno.ENOENT, 'No valid backup found in specified location')

        if not dataset:
            dataset = manifest['dataset']

        created_datasets = []
        snapshots = manifest['snapshots']
        unique_datasets = list(set(map(lambda s: s['name'].split('@')[0], snapshots)))
        unique_datasets.sort(key=lambda d: d.count('/'))
        provider = backup['provider']

        for i in unique_datasets:
            snaps = list(filter(lambda s: s['name'].split('@')[0] == i, snapshots))
            snap = first_or_default(lambda s: not s['incremental'], snaps)
            local_dataset = i.replace(manifest['dataset'], dataset, 1)

            while True:
                logger.info('Receiving {0} into {1}'.format(snap['name'], local_dataset))

                if local_dataset != dataset and local_dataset not in created_datasets:
                    self.join_subtasks(self.run_subtask(
                        'zfs.create_dataset', local_dataset.split('/')[0], local_dataset, 'FILESYSTEM'
                    ))

                    created_datasets.append(local_dataset)

                rfd, wfd = os.pipe()
                self.join_subtasks(
                    self.run_subtask(
                        'backup.{0}.get'.format(provider),
                        backup['properties'],
                        snap['filename'],
                        FileDescriptor(wfd)
                    ),
                    self.run_subtask('zfs.receive', local_dataset, FileDescriptor(rfd), True)
                )

                if snap['name'] == snapshot:
                    break

                snap = first_or_default(lambda s: '{0}@{1}'.format(i, s['anchor']) == snap['name'], snaps)
                if not snap:
                    break


def _init(dispatcher, plugin):
    plugin.register_schema_definition('backup', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'dataset': {'type': 'string'},
            'recursive': {'type': 'boolean'},
            'compression': {'$ref': 'backup-compression-type'},
            'provider': {'type': 'string'},
            'properties': {'$ref': 'backup-properties'}
        }
    })

    plugin.register_schema_definition('backup-compression-type', {
        'type': 'string',
        'enum': ['NONE', 'GZIP']
    })

    plugin.register_schema_definition('backup-state', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'hostname': {'type': 'string'},
            'dataset': {'type': 'string'},
            'snapshots': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'name': {'type': 'string'},
                        'anchor': {'type': ['string', 'null']},
                        'incremental': {'type': 'boolean'},
                        'created_at': {'type': 'string'},
                        'uuid': {'type': 'string'},
                        'compression': {'$ref': 'backup-compression-type'},
                        'filename': {'type': 'string'}
                    }
                }
            }
        }
    })

    plugin.register_schema_definition('backup-types', {
        'type': 'object',
        'additionalProperties': {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
            }
        }
    })

    plugin.register_schema_definition('backup-file', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'name': {'type': 'string'},
            'size': {'type': 'integer'},
            'content_type': {'type': ['string', 'null']}
        }
    })

    def update_backup_properties_schema():
        plugin.register_schema_definition('backup-properties', {
            'discriminator': 'type',
            'oneOf': [
                {'$ref': 'backup-{0}'.format(name)} for name in dispatcher.call_sync('backup.supported_types')
            ]
        })

    plugin.register_event_type('backup.changed')

    plugin.register_provider('backup', BackupProvider)
    plugin.register_task_handler('backup.create', CreateBackupTask)
    plugin.register_task_handler('backup.update', UpdateBackupTask)
    plugin.register_task_handler('backup.delete', DeleteBackupTask)
    plugin.register_task_handler('backup.sync', BackupSyncTask)
    plugin.register_task_handler('backup.query', BackupQueryTask)
    plugin.register_task_handler('backup.restore', BackupRestoreTask)

    update_backup_properties_schema()
    dispatcher.register_event_handler('server.plugin.loaded', update_backup_properties_schema)
