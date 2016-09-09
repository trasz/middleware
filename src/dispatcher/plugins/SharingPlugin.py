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

import os
import errno
from freenas.dispatcher.rpc import description, accepts, returns, private
from freenas.dispatcher.rpc import SchemaHelper as h, generator
from task import Task, TaskException, TaskDescription, VerifyException, Provider, RpcException, query, TaskWarning
from freenas.utils import normalize, in_directory, remove_unchanged, query as q
from utils import split_dataset, save_config, load_config, delete_config


@description("Provides information on shares")
class SharesProvider(Provider):
    @query('share')
    @generator
    def query(self, filter=None, params=None):
        def extend(share):
            perms = None
            path = self.translate_path(share['id'])
            if share['target_type'] in ('DIRECTORY', 'DATASET', 'FILE'):
                try:
                    perms = self.dispatcher.call_sync('filesystem.stat', path)
                except RpcException:
                    pass

            share['filesystem_path'] = path
            share['permissions'] = perms['permissions'] if perms else None
            return share

        return q.query(
            self.datastore.query('shares', callback=extend),
            *(filter or []),
            stream=True,
            **(params or {})
        )

    @description("Returns list of supported sharing providers")
    @accepts()
    @returns(h.ref('share-types'))
    def supported_types(self):
        result = {}
        for p in list(self.dispatcher.plugins.values()):
            if p.metadata and p.metadata.get('type') == 'sharing':
                result[p.metadata['method']] = {
                    'subtype': p.metadata['subtype'],
                    'perm_type': p.metadata.get('perm_type')
                }

        return result

    @description("Returns list of clients connected to particular share")
    @accepts(str)
    @returns(h.array(h.ref('share-client')))
    def get_connected_clients(self, id):
        share = self.datastore.get_by_id('shares', id)
        if not share:
            raise RpcException(errno.ENOENT, 'Share not found')

        return self.dispatcher.call_sync('share.{0}.get_connected_clients'.format(share['type']), id)

    @description("Get shares dependent on provided filesystem path")
    @accepts(str, bool, bool)
    @returns(h.array(h.ref('share')))
    def get_dependencies(self, path, enabled_only=True, recursive=True):
        result = []
        if enabled_only:
            shares = self.datastore.query('shares', ('enabled', '=', True))
        else:
            shares = self.datastore.query('shares')

        for i in shares:
            target_path = self.translate_path(i['id'])
            if recursive:
                if in_directory(target_path, path):
                    result.append(i)
            else:
                if target_path == path:
                    result.append(i)

        return result

    @private
    def translate_path(self, share_id):
        share = self.datastore.get_by_id('shares', share_id)
        return self.dispatcher.call_sync('share.expand_path', share['target_path'], share['target_type'])

    @private
    def expand_path(self, path, type):
        root = self.dispatcher.call_sync('volume.get_volumes_root')
        if type == 'DATASET':
            return os.path.join(root, path)

        if type == 'ZVOL':
            return os.path.join('/dev/zvol', path)

        if type in ('DIRECTORY', 'FILE'):
            return path

        raise RpcException(errno.EINVAL, 'Invalid share target type {0}'.format(type))

    @private
    def get_directory_path(self, share_id):
        share = self.datastore.get_by_id('shares', share_id)
        return self.dispatcher.call_sync('share.get_dir_by_path', share['target_path'], share['target_type'])

    @private
    def get_dir_by_path(self, path, type):
        root = self.dispatcher.call_sync('volume.get_volumes_root')
        if type == 'DATASET':
            return os.path.join(root, path)

        if type == 'ZVOL':
            return os.path.dirname(os.path.join(root, path))

        if type == 'DIRECTORY':
            return path

        if type == 'FILE':
            return os.path.dirname(path)

        raise RpcException(errno.EINVAL, 'Invalid share target type {0}'.format(type))


@description("Creates new share")
@accepts(h.all_of(
    h.ref('share'),
    h.required('name', 'type', 'target_type', 'target_path', 'properties')
))
class CreateShareTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating share"

    def describe(self, share):
        return TaskDescription("Creating share {name}", name=share.get('name') if share else '')

    def verify(self, share):
        if not self.dispatcher.call_sync('share.supported_types').get(share['type']):
            raise VerifyException(errno.ENXIO, 'Unknown sharing type {0}'.format(share['type']))

        return ['system']

    def run(self, share):
        if share['target_type'] == 'ZVOL':
            parent_ds = '/'.join(share['target_path'].split('/')[:-1])
            shareable = bool(self.dispatcher.call_sync('volume.dataset.query', [('name', '=', parent_ds)]))
        else:
            share_path = self.dispatcher.call_sync('share.expand_path', share['target_path'], share['target_type'])
            if share['target_type'] != 'FILE':
                share_path = os.path.dirname(share_path)
            shareable = os.path.exists(share_path)

        if not shareable:
            raise TaskException(errno.ENOENT, 'Selected share target {0} does not exist or cannot be created'.format(
                share['target_path']
            ))

        root = self.dispatcher.call_sync('volume.get_volumes_root')
        share_type = self.dispatcher.call_sync('share.supported_types').get(share['type'])

        assert share_type['subtype'] in ('FILE', 'BLOCK'),\
            "Unsupported Share subtype: {0}".format(share_type['subtype'])

        if self.datastore.exists(
            'shares',
            ('type', '=', share['type']),
            ('name', '=', share['name'])
        ):
            raise TaskException(errno.EEXIST, 'Share {0} of type {1} already exists'.format(
                share['name'],
                share['type']
            ))

        normalize(share, {
            'enabled': True,
            'immutable': False,
            'description': ''
        })

        if share['target_type'] in ('DATASET', 'ZVOL'):
            dataset = share['target_path']
            pool = share['target_path'].split('/')[0]
            path = os.path.join(root, dataset)

            if not self.dispatcher.call_sync('zfs.dataset.query', [('name', '=', dataset)], {'single': True}):
                if share_type['subtype'] == 'FILE':
                    self.join_subtasks(self.run_subtask('volume.dataset.create', {
                        'volume': pool,
                        'id': dataset,
                        'permissions_type': share_type['perm_type'],
                    }))

                if share_type['subtype'] == 'BLOCK':
                    self.join_subtasks(self.run_subtask('volume.dataset.create', {
                        'volume': pool,
                        'id': dataset,
                        'type': 'VOLUME',
                        'volsize': share['properties']['size'],
                    }))
            else:
                if share_type['subtype'] == 'FILE':
                    self.run_subtask('volume.dataset.update', dataset, {
                        'permissions_type': share_type['perm_type']
                    })

        elif share['target_type'] == 'DIRECTORY':
            # Verify that target directory exists
            path = share['target_path']
            if not os.path.isdir(path):
                raise TaskException(errno.ENOENT, "Target directory {0} doesn't exist".format(path))

        elif share['target_type'] == 'FILE':
            # Verify that target file exists
            path = share['target_path']
            if not os.path.isfile(path):
                raise TaskException(errno.ENOENT, "Target file {0} doesn't exist".format(path))

        else:
            raise AssertionError('Invalid target type')

        if share.get('permissions'):
            self.join_subtasks(self.run_subtask('file.set_permissions', path, share.pop('permissions')))

        ids = self.join_subtasks(self.run_subtask('share.{0}.create'.format(share['type']), share))
        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'create',
            'ids': ids
        })

        new_share = self.datastore.get_by_id('shares', ids[0])
        path = self.dispatcher.call_sync('share.get_directory_path', new_share['id'])
        try:
            save_config(
                path,
                '{0}-{1}'.format(new_share['type'], new_share['name']),
                new_share
            )
        except OSError as err:
            self.add_warning(TaskWarning(errno.ENXIO, 'Cannot save backup config file: {0}'.format(str(err))))

        return ids[0]


@description("Updates existing share")
@accepts(str, h.ref('share'))
class UpdateShareTask(Task):
    @classmethod
    def early_describe(cls):
        return "Updating share"

    def describe(self, id, updated_fields):
        share = self.datastore.get_by_id('shares', id)
        return TaskDescription("Updating share {name}", name=share.get('name', id) if share else id)

    def verify(self, id, updated_fields):
        return ['system']

    def run(self, id, updated_fields):
        share = self.datastore.get_by_id('shares', id)
        if not share:
            raise TaskException(errno.ENOENT, 'Share not found')

        if share['immutable']:
            raise TaskException(errno.EACCES, 'Cannot modify immutable share {0}.'.format(id))

        if 'name' in updated_fields or 'type' in updated_fields:
            share.update(updated_fields)
            if self.datastore.exists(
                'shares',
                ('id', '!=', id),
                ('type', '=', share['type']),
                ('name', '=', share['name'])
            ):
                raise TaskException(errno.EEXIST, 'Share {0} of type {1} already exists'.format(
                    share['name'],
                    share['type']
                ))

        path_after_update = updated_fields.get('target_path', share['target_path'])
        type_after_update = updated_fields.get('target_type', share['target_type'])
        permissions = updated_fields.pop('permissions')
        share_path = self.dispatcher.call_sync('share.expand_path', path_after_update, type_after_update)

        if not os.path.exists(share_path):
            raise TaskException(
                errno.ENOENT,
                'Selected share target {0} does not exist'.format(path_after_update)
            )

        share = self.datastore.get_by_id('shares', id)
        remove_unchanged(updated_fields, share)

        path = self.dispatcher.call_sync('share.get_directory_path', share['id'])
        try:
            delete_config(
                path,
                '{0}-{1}'.format(share['type'], share['name'])
            )
        except OSError:
            pass

        if 'type' in updated_fields:
            old_share_type = share['type']
            new_share_type = self.dispatcher.call_sync('share.supported_types').get(updated_fields['type'])
            if share['target_type'] == 'DATASET':
                pool, dataset = split_dataset(share['target_path'])
                self.join_subtasks(
                    self.run_subtask('volume.dataset.update', dataset, {
                        'permissions_type': new_share_type['perm_type']
                    })
                )

            share.update(updated_fields)
            self.join_subtasks(self.run_subtask('share.{0}.delete'.format(old_share_type), id))
            self.join_subtasks(self.run_subtask('share.{0}.create'.format(updated_fields['type']), share))
        else:
            self.join_subtasks(self.run_subtask('share.{0}.update'.format(share['type']), id, updated_fields))

        if permissions:
            path = self.dispatcher.call_sync('share.translate_path', id)
            self.join_subtasks(self.run_subtask('file.set_permissions', path, permissions))

        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'update',
            'ids': [share['id']]
        })

        updated_share = self.datastore.get_by_id('shares', id)
        path = self.dispatcher.call_sync('share.get_directory_path', updated_share['id'])
        try:
            save_config(
                path,
                '{0}-{1}'.format(updated_share['type'], updated_share['name']),
                updated_share
            )
        except OSError as err:
            self.add_warning(TaskWarning(errno.ENXIO, 'Cannot save backup config file: {0}'.format(str(err))))


@description("Imports existing share")
@accepts(str, str, str)
class ImportShareTask(Task):
    @classmethod
    def early_describe(cls):
        return "Importing share"

    def describe(self, config_path, name, type):
        return TaskDescription("Importing share {name} from {config_path}", name=name, config_path=config_path)

    def verify(self, config_path, name, type):
        try:
            share = load_config(config_path, '{0}-{1}'.format(type, name))
        except FileNotFoundError:
            raise VerifyException(
                errno.ENOENT,
                'There is no share {0} of type {1} at {2} to be imported.'.format(name, type, config_path)
            )
        except ValueError:
            raise VerifyException(
                errno.EINVAL,
                'Cannot read configuration file. File is not a valid JSON file'
            )

        if share['type'] != type:
            raise VerifyException(
                errno.EINVAL,
                'Share type {0} does not match configuration file entry type {1}'.format(type, share['type'])
            )

        if not self.dispatcher.call_sync('share.supported_types').get(share['type']):
            raise VerifyException(errno.ENXIO, 'Unknown sharing type {0}'.format(share['type']))

        return ['system']

    def run(self, config_path, name, type):

        share = load_config(config_path, '{0}-{1}'.format(type, name))
        if self.datastore.exists(
            'shares',
            ('type', '=', share['type']),
            ('name', '=', share['name'])
        ):
            raise TaskException(errno.EEXIST, 'Share {0} of type {1} already exists'.format(
                share['name'],
                share['type']
            ))

        ids = self.join_subtasks(self.run_subtask('share.{0}.import'.format(share['type']), share))

        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'create',
            'ids': ids
        })

        return ids[0]


@description("Sets share immutable")
@accepts(str, bool)
class ShareSetImmutableTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating share\'s immutable property'

    def describe(self, id, immutable):
        share = self.datastore.get_by_id('shares', id)
        return TaskDescription(
            'Setting {name} share\'s immutable property to {value}',
            name=share.get('name', id) if share else id,
            value='on' if immutable else 'off'
        )

    def verify(self, id, immutable):
        return ['system']

    def run(self, id, immutable):
        if not self.datastore.exists('shares', id):
            raise TaskException(errno.ENOENT, 'Share {0} does not exist'.format(id))

        share = self.datastore.get_by_id('shares', id)
        share['immutable'] = immutable
        share['enabled'] = not immutable
        self.datastore.update('shares', id, share)
        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'update',
            'ids': [share['id']]
        })


@description("Deletes share")
@accepts(str)
class DeleteShareTask(Task):
    @classmethod
    def early_describe(cls):
        return "Deleting share"

    def describe(self, id):
        share = self.datastore.get_by_id('shares', id)
        return TaskDescription("Deleting share {name}", name=share.get('name', id) if share else id)

    def verify(self, id):
        return ['system']

    def run(self, id):
        share = self.datastore.get_by_id('shares', id)
        if not share:
            raise TaskException(errno.ENOENT, 'Share not found')

        path = self.dispatcher.call_sync('share.get_directory_path', share['id'])

        try:
            delete_config(
                path,
                '{0}-{1}'.format(share['type'], share['name'])
            )
        except OSError:
            pass

        self.join_subtasks(self.run_subtask('share.{0}.delete'.format(share['type']), id))
        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description("Export share")
@accepts(str)
class ExportShareTask(Task):
    @classmethod
    def early_describe(cls):
        return "Exporting share"

    def describe(self, id):
        share = self.datastore.get_by_id('shares', id)
        return TaskDescription("Exporting share {name}", name=share.get('name', id) if share else id)

    def verify(self, id):
        return ['system']

    def run(self, id):
        share = self.datastore.get_by_id('shares', id)
        if not share:
            raise TaskException(errno.ENOENT, 'Share not found')

        self.join_subtasks(self.run_subtask('share.{0}.delete'.format(share['type']), id))
        self.dispatcher.dispatch_event('share.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description("Deletes all shares dependent on specified volume/dataset")
@accepts(str)
class DeleteDependentShares(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting shares related to system path'

    def describe(self, path):
        return TaskDescription('Deleting shares related to system path {name}', name=path)

    def verify(self, path):
        return ['system']

    def run(self, path):
        subtasks = []
        for i in self.dispatcher.call_sync('share.get_dependencies', path):
            subtasks.append(self.run_subtask('share.delete', i['id']))

        self.join_subtasks(*subtasks)


@private
@description("Updates all shares related to specified volume/dataset")
@accepts(str, h.ref('share'))
class UpdateRelatedShares(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating shares related to system path'

    def describe(self, path, updated_fields):
        return TaskDescription('Updating shares related to system path {name}', name=path)

    def verify(self, path, updated_fields):
        return ['system']

    def run(self, path, updated_fields):
        subtasks = []
        for i in self.dispatcher.call_sync('share.get_dependencies', path, False):
            subtasks.append(self.run_subtask('share.update', i['id'], updated_fields))

        self.join_subtasks(*subtasks)


@description("Kills client connections from specified IP address")
@accepts(str, str)
class ShareTerminateConnectionTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Killing connections to share'

    def describe(self, share_type, address):
        return TaskDescription('Killing {address} connections to {name} share', address=address, name=share_type)

    def verify(self, share_type, address):
        return ['system']

    def run(self, share_type, address):
        self.join_subtasks(self.run_subtask('share.{0}.terminate_connection'.format(share_type), address))


def _depends():
    return ['VolumePlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('share', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'enabled': {'type': 'boolean'},
            'immutable': {'type': 'boolean'},
            'type': {'type': 'string'},
            'target_type': {'$ref': 'share-targettype'},
            'target_path': {'type': 'string'},
            'filesystem_path': {
                'type': 'string',
                'readOnly': True
            },
            'permissions': {
                'oneOf': [
                    {'$ref': 'permissions'},
                    {'type': 'null'}
                ]
            },
            'properties': {'$ref': 'share-properties'}
        }
    })

    plugin.register_schema_definition('share-targettype', {
        'type': 'string',
        'enum': ['DATASET', 'ZVOL', 'DIRECTORY', 'FILE']
    })

    plugin.register_schema_definition('share-client', {
        'type': 'object',
        'properties': {
            'host': {'type': 'string'},
            'share': {'type': 'string'},
            'user': {'type': ['string', 'null']},
            'connected_at': {'type': ['string', 'null']},
            'extra': {
                'type': 'object'
            }
        }
    })

    plugin.register_schema_definition('share-types', {
        'type': 'object',
        'additionalProperties': {
            'type': 'object',
            'properties': {
                'subtype': {'$ref': 'share-types-subtype'},
                'perm_type': { 'oneOf': [
                    {'$ref': 'share-types-permtype'},
                    {'type': 'null'}
                ]},
            },
            'additionalProperties': False
        }
    })

    plugin.register_schema_definition('share-types-subtype', {
        'type': 'string',
        'enum': ['FILE', 'BLOCK']
    })

    plugin.register_schema_definition('share-types-permtype', {
        'type': 'string',
        'enum': ['PERM', 'ACL']
    })

    def volume_pre_destroy(args):
        path = dispatcher.call_sync('volume.resolve_path', args['name'], '')
        dispatcher.call_task_sync('share.delete_dependent', path)
        dispatcher.call_task_sync('share.delete_dependent', os.path.join('/dev/zvol', args['name']))
        return True

    def volume_rename(args):
        for share in dispatcher.call_sync('share.query'):
            new_path = share['target_path']
            if share['target_path'].startswith(args['name']):
                new_path = new_path.replace(args['name'], args['new_name'], 1)

            elif share['target_type'] in ('DIRECTORY', 'FILE'):
                if share['target_path'].startswith(args['mountpoint']):
                    new_path = new_path.replace(args['mountpoint'], args['new_mountpoint'], 1)

            if new_path is not share['target_path']:
                dispatcher.call_task_sync('share.update', share['id'], {'target_path': new_path})
        return True

    def set_related_enabled(name, enabled):
        pool_properties = dispatcher.call_sync(
            'zfs.pool.query',
            [('name', '=', name)],
            {'single': True, 'select': 'properties'}
        )
        if pool_properties.get('readonly', 'off') == 'off':
            path = dispatcher.call_sync('volume.resolve_path', name, '')
            dispatcher.call_task_sync('share.update_related', path, {'enabled': enabled})
            dispatcher.call_task_sync('share.update_related', os.path.join('/dev/zvol', name), {'enabled': enabled})

    def volume_detach(args):
        set_related_enabled(args['name'], False)
        return True

    def volume_attach(args):
        set_related_enabled(args['name'], True)
        return True

    def update_share_properties_schema():
        plugin.register_schema_definition('share-properties', {
            'discriminator': 'type',
            'oneOf': [
                {'$ref': 'share-{0}'.format(name)} for name in dispatcher.call_sync('share.supported_types')
            ]
        })

    # Register providers
    plugin.register_provider('share', SharesProvider)

    # Register task handlers
    plugin.register_task_handler('share.create', CreateShareTask)
    plugin.register_task_handler('share.update', UpdateShareTask)
    plugin.register_task_handler('share.delete', DeleteShareTask)
    plugin.register_task_handler('share.export', ExportShareTask)
    plugin.register_task_handler('share.import', ImportShareTask)
    plugin.register_task_handler('share.immutable.set', ShareSetImmutableTask)
    plugin.register_task_handler('share.delete_dependent', DeleteDependentShares)
    plugin.register_task_handler('share.update_related', UpdateRelatedShares)
    plugin.register_task_handler('share.terminate_connection', ShareTerminateConnectionTask)

    # Register Event Types
    plugin.register_event_type(
        'share.changed',
        schema={
            'type': 'object',
            'properties': {
                'operation': {'type': 'string', 'enum': ['create', 'delete', 'update']},
                'ids': {'type': 'array', 'items': 'string'},
            },
            'additionalProperties': False
        }
    )

    update_share_properties_schema()
    dispatcher.register_event_handler('server.plugin.loaded', update_share_properties_schema)

    # Register Hooks
    plugin.attach_hook('volume.pre_destroy', volume_pre_destroy)
    plugin.attach_hook('volume.pre_detach', volume_detach)
    plugin.attach_hook('volume.post_attach', volume_attach)
    plugin.attach_hook('volume.post_rename', volume_rename)
