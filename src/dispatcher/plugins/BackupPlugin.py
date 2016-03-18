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
import json
import gevent
import logging
from gevent.fileobject import FileObjectThread
from freenas.dispatcher.client import FileDescriptor
from freenas.dispatcher.rpc import RpcException, accepts, returns, description, SchemaHelper as h
from task import Provider, Task, ProgressTask, VerifyException, TaskException
from freenas.utils import normalize


logger = logging.getLogger(__name__)
MANIFEST_FILENAME = 'FREENAS_MANIFEST'


class BackupProvider(Provider):
    def query(self, filter=None, params=None):
        def extend(backup):
            return backup

        return self.datastore.query('backup', *(filter or []), callback=extend, **(params or {}))

    def download(self, backup, path):
        rfd, wfd = os.pipe()
        fd = FileObjectThread(rfd, 'rb')
        result = None

        def worker():
            nonlocal result
            result = fd.read()

        thr = gevent.spawn(worker)
        self.dispatcher.call_task_sync('backup.ssh.get', backup, path, FileDescriptor(wfd))

        thr.join(timeout=1)
        os.close(rfd)

        return result.decode('utf-8')

    def get_state(self, id):
        backup = self.datastore.get_by_id('backup', id)
        if not backup:
            raise RpcException(errno.ENOENT, 'Backup {0} not found'.format(id))

        dirlist = self.dispatcher.call_sync(
            'backup.{0}.list_objects'.format(backup['provider']),
            backup['properties']
        )

        if MANIFEST_FILENAME not in dirlist:
            return None

        data = self.download(backup['properties'], MANIFEST_FILENAME)
        try:
            manifest = json.loads(data)
        except ValueError:
            raise RpcException('Invalid backup manifest')

        return manifest

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


@accepts(str)
class BackupSyncTask(ProgressTask):
    def verify(self, id):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def run(self, id):
        pass


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
                        'created_at': {'type': 'string'},
                        'uuid': {'type': 'string'},
                        'compression': {'$ref': 'backup-compression-type'},
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

    update_backup_properties_schema()
    dispatcher.register_event_handler('server.plugin.loaded', update_backup_properties_schema)
