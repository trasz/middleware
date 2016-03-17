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

import errno
from dispatcher.rpc import accepts, returns, description, SchemaHelper as h
from task import Provider, Task, ProgressTask, VerifyException, TaskException


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
    h.required('name')
))
class CreateBackupTask(Task):
    def verify(self, backup):
        if 'id' in backup and self.datastore.exists('backup', ('id', '=', backup['id'])):
            raise VerifyException('Backup with ID {0} already exists'.format(backup['id']))

        if self.datastore.exists('backup', ('name', '=', backup['id'])):
            raise VerifyException('Backup with name {0} already exists'.format(backup['name']))

        return ['system']

    def run(self, backup):
        id = self.datastore.insert('backup', backup)
        return id


@accepts(str, h.ref('backup'))
class UpdateBackupTask(Task):
    def verify(self, id, updated_params):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def run(self, id, updated_params):
        pass


@accepts(str)
class DeleteBackupTask(Task):
    def verify(self, id):
        if not self.datastore.exists('backup', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Backup {0} not found'.format(id))

        return ['system']

    def run(self, id):
        pass


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
            'compression': {
                'type': 'string',
                'enum': ['NONE', 'GZIP']
            },
            'provider': {
                'type': 'string',
                'enum': []
            },
            'properties': {'$ref': 'backup-properties'}
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
        plugin.register_schema_definition('share-properties', {
            'discriminator': 'type',
            'oneOf': [
                {'$ref': 'backup-{0}'.format(name)} for name in dispatcher.call_sync('backup.supported_types')
            ]
        })

    plugin.register_event_type('backup.changed')

    plugin.register_task_handler('backup.create', CreateBackupTask)
    plugin.register_task_handler('backup.update', UpdateBackupTask)
    plugin.register_task_handler('backup.delete', DeleteBackupTask)
    plugin.register_task_handler('backup.sync', BackupSyncTask)

    update_backup_properties_schema()
    dispatcher.register_event_handler('server.plugin.loaded', update_backup_properties_schema)
