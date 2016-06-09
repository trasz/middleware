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
import logging
from freenas.dispatcher.client import Client
from paramiko import AuthenticationException
from utils import get_replication_client, call_task_and_check_state
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, private
from task import Task, Provider, TaskException, TaskWarning, VerifyException, query, TaskDescription


logger = logging.getLogger('PairingPlugin')

REPL_USR_HOME = '/var/tmp/replication'
AUTH_FILE = os.path.join(REPL_USR_HOME, '.ssh/authorized_keys')

ssh_port = None


@description('Provides information about known peers')
class PeerProvider(Provider):
    @query('peer')
    def query(self, filter=None, params=None):
        return self.datastore.query('peers', *(filter or []), **(params or {}))

    @private
    def get_ssh_keys(self):
        key_paths = ['/etc/ssh/ssh_host_rsa_key.pub', '/etc/replication/key.pub']
        keys = []
        try:
            for key_path in key_paths:
                with open(key_path) as f:
                     keys.append(f.read())
        except FileNotFoundError:
            raise RpcException(errno.ENOENT, 'Key file {0} not found'.format(key_path))

        return [i for i in keys]

    @private
    def credentials_types(self):
        return ['replication', 'ssh', 'amazon-s3']


@description('Creates a peer entry')
@accepts(h.all_of(
    h.ref('peer'),
    h.required('name', 'address', 'type', 'credentials')
))
class PeerCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Creating peer entry'

    def describe(self, peer):
        return TaskDescription('Creating peer entry {name}', name=peer.get('name', ''))

    def verify(self, peer):
        if 'name' not in peer:
            raise VerifyException(errno.EINVAL, 'Name has to be specified')

        if 'address' not in peer:
            raise VerifyException(errno.EINVAL, 'Address has to be specified')

        if peer.get('type') not in self.dispatcher.call_sync('peer.credentials_types'):
            raise VerifyException(errno.EINVAL, 'Unknown credentials type {0}'.format(peer.get('type')))

        return ['system']

    def run(self, peer):
        if self.datastore.exists('peers', ('name', '=', peer['name'])):
            raise TaskException(errno.EINVAL, 'Peer entry {0} already exists'.format(peer['name']))

        if peer['type'] == 'replication':
            self.join_subtasks(self.run_subtask('peer.replication.create', peer))
        else:
            id = self.datastore.insert('peers', peer)
            self.dispatcher.dispatch_event('peer.changed', {
                'operation': 'create',
                'ids': [id]
            })


@description('Updates peer entry')
@accepts(str, h.ref('peer'))
class PeerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating peer entry'

    def describe(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Updating peer entry {name}', name=peer.get('name', ''))

    def verify(self, id, updated_fields):
        if 'type' in updated_fields:
            raise VerifyException(errno.EINVAL, 'Type of peer cannot be updated')

        return ['system']

    def run(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        if peer['type'] == 'replication':
            self.join_subtasks(self.run_subtask('peer.replication.update', id, updated_fields))
        else:
            peer.update(updated_fields)
            self.datastore.update('peers', id, peer)
            self.dispatcher.dispatch_event('peer.changed', {
                'operation': 'update',
                'ids': [id]
            })


@description('Deletes peer entry')
@accepts(str)
class PeerDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Deleting peer entry'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Deleting peer entry {name}', name=peer.get('name', ''))

    def verify(self, id):
        return ['system']

    def run(self, id):
        if not self.datastore.exists('peers', ('id', '=', id)):
            raise TaskException(errno.EINVAL, 'Peer entry {0} does not exist'.format(id))

        peer = self.datastore.get_by_id('peers', id)

        if peer['type'] == 'replication':
            self.join_subtasks(self.run_subtask('peer.replication.delete', id))
        else:
            self.datastore.delete('peers', id)
            self.dispatcher.dispatch_event('peer.changed', {
                'operation': 'delete',
                'ids': [id]
            })


@description('Exchanges SSH keys with remote machine for replication purposes')
@accepts(h.all_of(
    h.ref('peer'),
    h.required('name', 'address', 'type', 'credentials')
))
class ReplicationPeerCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Exchanging SSH keys with remote host'

    def describe(self, peer):
        return TaskDescription('Exchanging SSH keys with the remote {name}', name=peer.get('address', ''))

    def verify(self, peer):
        return ['system']

    def run(self, peer):
        if self.datastore.exists('peers', ('address', '=', peer['address']), ('type', '=', 'replication')):
            raise TaskException(errno.EEXIST, 'Replication peer entry for {0} already exists'.format(peer['address']))

        remote = peer.get('address')
        credentials = peer['credentials']
        username = credentials.get('username')
        port = credentials.get('port', 22)
        password = credentials.get('password')

        if not username:
            raise TaskException(errno.EINVAL, 'Username has to be specified')

        if not remote:
            raise TaskException(errno.EINVAL, 'Address of remote host has to be specified')

        if not password:
            raise TaskException(errno.EINVAL, 'Password has to be specified')

        remote_client = Client()
        try:
            try:
                remote_client.connect('ws+ssh://{0}@{1}'.format(username, remote), port=port, password=password)
                remote_client.login_service('replicator')
            except (AuthenticationException, OSError, ConnectionRefusedError):
                raise TaskException(errno.ECONNABORTED, 'Cannot connect to {0}:{1}'.format(remote, port))

            local_keys = self.dispatcher.call_sync('peer.get_ssh_keys')
            remote_keys = remote_client.call_sync('peer.get_ssh_keys')
            ip_at_remote_side = remote_client.call_sync('management.get_sender_address').split(',', 1)[0]

            remote_host_key = remote + ' ' + remote_keys[0].rsplit(' ', 1)[0]
            local_host_key = ip_at_remote_side + ' ' + local_keys[0].rsplit(' ', 1)[0]

            local_ssh_config = self.dispatcher.call_sync('service.sshd.get_config')

            if remote_client.call_sync('peer.query', [('name', '=', peer['name'])]):
                raise TaskException(errno.EEXIST, 'Peer entry {0} already exists at {1}'.format(peer['name'], remote))

            peer['credentials'] = {
                'pubkey': remote_keys[1],
                'hostkey': remote_host_key,
                'port': port
            }

            self.join_subtasks(self.run_subtask(
                'peer.replication.create_local',
                peer
            ))

            peer['address'] = ip_at_remote_side
            peer['credentials'] = {
                'pubkey': local_keys[1],
                'hostkey': local_host_key,
                'port': local_ssh_config['port']
            }

            id = self.datastore.query('peers', ('name', '=', peer['name']), select='id')
            try:
                call_task_and_check_state(
                    remote_client,
                    'peer.replication.create_local',
                    peer
                )
            except TaskException:
                self.datastore.delete('peers', id)
                self.dispatcher.dispatch_event('peer.changed', {
                    'operation': 'delete',
                    'ids': [id]
                })
                raise
        finally:
            remote_client.disconnect()


@private
@description('Creates replication peer entry in database')
@accepts(h.ref('peer'))
class ReplicationPeerCreateLocalTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Creating replication peer entry'

    def describe(self, peer):
        return TaskDescription('Creating replication peer entry {name}', name=peer['name'])

    def verify(self, peer):
        return ['system']

    def run(self, peer):
        if self.datastore.exists('replication.peers', ('name', '=', peer['name'])):
            raise TaskException(errno.EEXIST, 'Replication peer entry {0} already exists'.format(peer['name']))

        id = self.datastore.insert('peers', peer)

        with open(AUTH_FILE, 'a') as auth_file:
            auth_file.write(peer['pubkey'])

        self.dispatcher.dispatch_event('peer.changed', {
            'operation': 'create',
            'ids': [id]
        })


@description('Removes replication peer entries from both ends of replication link')
@accepts(str)
class ReplicationPeerDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Removing replication peer entries'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Removing replication peer entries: {name}', name=peer['name'])

    def verify(self, id):
        return ['system']

    def run(self, id):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'Peer entry {0} does not exist'.format(id))

        remote = peer['address']
        remote_client = None
        try:
            try:
                remote_client = get_replication_client(self.dispatcher, remote)

                call_task_and_check_state(
                    remote_client,
                    'peer.replication.delete_local',
                    id
                )
            except RpcException as e:
                self.add_warning(TaskWarning(
                    e.code,
                    'Remote {0} is unreachable. Delete operation is performed at local side only.'.format(remote)
                ))
            except ValueError as e:
                self.add_warning(TaskWarning(
                    errno.EINVAL,
                    str(e)
                ))

            self.join_subtasks(self.run_subtask(
                'peer.replication.delete_local',
                id
            ))

        finally:
            if remote_client:
                remote_client.disconnect()


@private
@description('Removes local replication peer entry from database')
@accepts(str)
class ReplicationPeerDeleteLocalTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Removing replication peer entry'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Removing replication peer entry {name}', name=peer['name'])

    def verify(self, id):
        return ['system']

    def run(self, id):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'Replication peer entry {0} does not exist'.format(peer['name']))
        peer_pubkey = peer['credentials']['pubkey']
        self.datastore.delete('peers', id)

        with open(AUTH_FILE, 'r') as auth_file:
            auth_keys = auth_file.read()

        new_auth_keys = ''
        for line in auth_keys.splitlines():
            if peer_pubkey not in line:
                new_auth_keys = new_auth_keys + '\n' + line

        with open(AUTH_FILE, 'w') as auth_file:
            auth_file.write(new_auth_keys)

        self.dispatcher.dispatch_event('peer.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@private
@description('Updates replication peer entry in database')
@accepts(str, h.ref('peer'))
class ReplicationPeerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating replication peer entry'

    def describe(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Updating replication peer entry {name}', name=peer['name'])

    def verify(self, id, updated_fields):
        return ['system']

    def run(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'Replication peer entry {0} does not exist'.format(id))

        if 'address' in updated_fields:
            raise TaskException(errno.EINVAL, 'Address of replication peer cannot be updated')

        if 'type' in updated_fields:
            raise TaskException(errno.EINVAL, 'Type of replication peer cannot be updated')

        if 'id' in updated_fields:
            raise TaskException(errno.EINVAL, 'ID of replication peer cannot be updated')

        peer.update(updated_fields)

        self.datastore.update('peers', id, peer)
        self.dispatcher.dispatch_event('peer.changed', {
            'operation': 'update',
            'ids': [id]
        })


@private
@description('Updates ssh port in number in remote replication peer entry')
@accepts(str, int)
class ReplicationPeerUpdatePortTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating SSH port in remote peer'

    def describe(self, id, port):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Updating SSH port in remote peer {name}', name=peer['name'])

    def verify(self, id, port):
        return ['system']

    def run(self, id, port):
        peer = self.datastore.get_by_id('peers', id)
        remote_client = None
        if not peer:
            raise TaskException(errno.ENOENT, 'Replication peer entry {0} does not exist'.format(id))

        try:
            remote_client = get_replication_client(self.dispatcher, peer['address'])
            remote_peer = remote_client.call_sync('peer.query', [('id', '=', peer['id'])], {'single': True})
            if not remote_peer:
                raise TaskException(errno.ENOENT, 'Remote side of peer {0} does not exist'.format(peer['name']))

            remote_peer['credentials']['port'] = port

            call_task_and_check_state(
                remote_client,
                'peer.replication.delete_local',
                id
            )
            call_task_and_check_state(
                remote_client,
                'peer.replication.create_local',
                remote_peer
            )
        finally:
            if remote_client:
                remote_client.disconnect()


def _depends():
    return ['SSHPlugin']


def _init(dispatcher, plugin):
    global ssh_port
    ssh_port = dispatcher.call_sync('service.sshd.get_config')['port']

    # Register schemas
    plugin.register_schema_definition('peer', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'address': {'type': 'string'},
            'id': {'type': 'string'},
            'type': {'enum': ['replication', 'ssh', 'amazon-s3']},
            'credentials': {'$ref': 'peer-credentials'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('replication-credentials', {
        'type': 'object',
        'properties': {
            'port': {'type': 'number'},
            'pubkey': {'type': 'string'},
            'hostkey': {'type': 'string'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('ssh-credentials', {
        'type': 'object',
        'properties': {
            'username': {'type': 'string'},
            'port': {'type': 'number'},
            'password': {'type': 'string'},
            'pubkey': {'type': 'string'},
            'hostkey': {'type': 'string'},
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('amazon-s3-credentials', {
        'type': 'object',
        'properties': {
            'access_key': {'type': 'string'},
            'secret_key': {'type ': 'string'},
            'region': {'type': ['string', 'null']},
            'bucket': {'type': 'string'},
            'folder': {'type': ['string', 'null']}
        },
        'additionalProperties': False
    })

    # Register providers
    plugin.register_provider('peer', PeerProvider)

    # Register credentials schema
    plugin.register_schema_definition('peer-credentials', {
        'discriminator': 'name',
        'oneOf': [
            {'$ref': '{0}-credentials'.format(name)} for name in dispatcher.call_sync('peer.credentials_types')
        ]
    })

    # Register tasks
    plugin.register_task_handler("peer.create", PeerCreateTask)
    plugin.register_task_handler("peer.update", PeerUpdateTask)
    plugin.register_task_handler("peer.delete", PeerUpdateTask)
    plugin.register_task_handler("peer.replication.create", ReplicationPeerCreateTask)
    plugin.register_task_handler("peer.replication.create_local", ReplicationPeerCreateLocalTask)
    plugin.register_task_handler("peer.replication.delete", ReplicationPeerDeleteTask)
    plugin.register_task_handler("peer.replication.delete_local", ReplicationPeerDeleteLocalTask)
    plugin.register_task_handler("peer.replication.update", ReplicationPeerUpdateTask)
    plugin.register_task_handler("peer.replication.update_remote_port", ReplicationPeerUpdatePortTask)

    # Register event types
    plugin.register_event_type('peer.changed')

    # Event handlers methods
    def on_ssh_change(args):
        global ssh_port
        new_ssh_port = dispatcher.call_sync('service.sshd.get_config')['port']
        if ssh_port != new_ssh_port:
            ssh_port = new_ssh_port
            ids = dispatcher.call_sync('peer.query', [('type', '=', 'replication')], {'select': 'id'})
            try:
                for id in ids:
                    dispatcher.call_task_sync('peer.replication.update_remote_port', id, new_ssh_port)
            except RpcException:
                pass

    # Register event handlers
    plugin.register_event_handler('service.sshd.changed', on_ssh_change)

    # Create home directory and authorized keys file for replication user
    if not os.path.exists(REPL_USR_HOME):
        os.mkdir(REPL_USR_HOME)
    ssh_dir = os.path.join(REPL_USR_HOME, '.ssh')
    if not os.path.exists(ssh_dir):
        os.mkdir(ssh_dir)
    with open(AUTH_FILE, 'w') as auth_file:
        for host in dispatcher.call_sync('peer.query', [('type', '=', 'replication')]):
            auth_file.write(host['credentials']['pubkey'])
