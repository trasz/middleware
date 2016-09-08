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
import socket
import errno
import logging
from freenas.dispatcher.client import Client
from paramiko import AuthenticationException
from utils import get_replication_client, call_task_and_check_state
from freenas.utils import exclude
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, private, generator
from task import Task, Provider, TaskException, TaskWarning, VerifyException, query, TaskDescription


logger = logging.getLogger('PairingPlugin')

REPL_USR_HOME = '/var/tmp/replication'
AUTH_FILE = os.path.join(REPL_USR_HOME, '.ssh/authorized_keys')

ssh_port = None


@description('Provides information about known peers')
class PeerProvider(Provider):
    @query('peer')
    @generator
    def query(self, filter=None, params=None):
        return self.datastore.query_stream('peers', *(filter or []), **(params or {}))

    @private
    def get_ssh_keys(self):
        key_path = None
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
        return ['freenas', 'ssh', 'amazon-s3']


@description('Creates a peer entry')
@accepts(h.all_of(
    h.ref('peer'),
    h.required('address', 'type', 'credentials')
))
class PeerCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Creating peer entry'

    def describe(self, peer):
        return TaskDescription('Creating peer entry {name}', name=peer.get('name', ''))

    def verify(self, peer):
        if 'address' not in peer:
            raise VerifyException(errno.EINVAL, 'Address has to be specified')

        if peer.get('type') not in self.dispatcher.call_sync('peer.credentials_types'):
            raise VerifyException(errno.EINVAL, 'Unknown credentials type {0}'.format(peer.get('type')))

        return ['system']

    def run(self, peer):
        ips = self.dispatcher.call_sync('network.config.get_my_ips')

        if peer['address'] in ips:
            raise TaskException(
                errno.EINVAL,
                'Please specify a remote address. {0} is a local machine address'.format(peer['address'])
            )

        if peer['type'] == 'freenas':
            self.join_subtasks(self.run_subtask('peer.freenas.create', peer))
        else:
            if 'name' not in peer:
                raise TaskException(errno.EINVAL, 'Name has to be specified')

            if self.datastore.exists('peers', ('name', '=', peer['name']), ('type', '=', peer['type'])):
                raise TaskException(errno.EINVAL, 'Peer entry {0} already exists'.format(peer['name']))

            if peer['type'] != peer['credentials']['type']:
                raise TaskException(errno.EINVAL, 'Peer type and credentials type must match')

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
        if peer['type'] == 'freenas':
            self.join_subtasks(self.run_subtask('peer.freenas.update', id, updated_fields))
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

        if peer['type'] == 'freenas':
            self.join_subtasks(self.run_subtask('peer.freenas.delete', id))
        else:
            self.datastore.delete('peers', id)
            self.dispatcher.dispatch_event('peer.changed', {
                'operation': 'delete',
                'ids': [id]
            })


@description('Exchanges SSH keys with remote FreeNAS machine')
@accepts(h.all_of(
    h.ref('peer'),
    h.required('address', 'type', 'credentials')
))
class FreeNASPeerCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Exchanging SSH keys with remote host'

    def describe(self, peer):
        return TaskDescription('Exchanging SSH keys with the remote {name}', name=peer.get('address', ''))

    def verify(self, peer):
        return ['system']

    def run(self, peer):
        if self.datastore.exists('peers', ('address', '=', peer['address']), ('type', '=', 'freenas')):
            raise TaskException(errno.EEXIST, 'FreeNAS peer entry for {0} already exists'.format(peer['address']))

        if peer['credentials']['type'] != 'ssh':
            raise TaskException(errno.EINVAL, 'SSH credentials type is needed to perform FreeNAS peer pairing')

        hostid = self.dispatcher.call_sync('system.info.host_uuid')
        hostname = self.dispatcher.call_sync('system.general.get_config')['hostname']
        remote_peer_name = peer.get('name', hostname)
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

            remote_hostname = remote_client.call_sync('system.general.get_config')['hostname']

            remote_host_key = remote_keys[0].rsplit(' ', 1)[0]
            local_host_key = local_keys[0].rsplit(' ', 1)[0]

            local_ssh_config = self.dispatcher.call_sync('service.sshd.get_config')

            if remote_client.call_sync('peer.query', [('id', '=', hostid)]):
                raise TaskException(errno.EEXIST, 'Peer entry of {0} already exists at {1}'.format(hostname, remote))

            peer['credentials'] = {
                'pubkey': remote_keys[1],
                'hostkey': remote_host_key,
                'port': port,
                'type': 'freenas'
            }

            local_id = remote_client.call_sync('system.info.host_uuid')
            peer['id'] = local_id
            peer['name'] = peer.get('name', remote_hostname)
            ip = socket.gethostbyname(peer['address'])
            peer['address'] = remote_hostname

            self.join_subtasks(self.run_subtask(
                'peer.freenas.create_local',
                peer,
                ip
            ))

            peer['id'] = hostid
            peer['name'] = remote_peer_name

            peer['address'] = hostname
            peer['credentials'] = {
                'pubkey': local_keys[1],
                'hostkey': local_host_key,
                'port': local_ssh_config['port'],
                'type': 'freenas'
            }

            try:
                call_task_and_check_state(
                    remote_client,
                    'peer.freenas.create_local',
                    peer,
                    ip_at_remote_side
                )
            except TaskException:
                self.datastore.delete('peers', local_id)
                self.dispatcher.dispatch_event('peer.changed', {
                    'operation': 'delete',
                    'ids': [local_id]
                })
                raise
        finally:
            remote_client.disconnect()


@private
@description('Creates FreeNAS peer entry in database')
@accepts(h.ref('peer'), str)
class FreeNASPeerCreateLocalTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Creating FreeNAS peer entry'

    def describe(self, peer, ip):
        return TaskDescription('Creating FreeNAS peer entry {name}', name=peer['name'])

    def verify(self, peer, ip):
        return ['system']

    def run(self, peer, ip):
        def ping(address, port):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((address, port))
            finally:
                s.close()

        if self.datastore.exists('peers', ('name', '=', peer['id'])):
            raise TaskException(errno.EEXIST, 'FreeNAS peer entry {0} already exists'.format(peer['name']))

        try:
            ping(peer['address'], peer['credentials']['port'])
        except socket.error:
            try:
                ping(ip, peer['credentials']['port'])
                peer['address'] = ip
            except socket.error as err:
                raise TaskException(err.errno, '{0} is not reachable. Check connection'.format(peer['address']))

        if ip and socket.gethostbyname(peer['address']) != socket.gethostbyname(ip):
            raise TaskException(
                errno.EINVAL,
                'Resolved peer {0} IP {1} does not match desired peer IP {2}'.format(
                    peer['address'],
                    socket.gethostbyname(peer['address']),
                    ip
                )
            )

        id = self.datastore.insert('peers', peer)

        with open(AUTH_FILE, 'a') as auth_file:
            auth_file.write(peer['credentials']['pubkey'])

        self.dispatcher.dispatch_event('peer.changed', {
            'operation': 'create',
            'ids': [id]
        })


@description('Removes FreeNAS peer entry')
@accepts(str)
class FreeNASPeerDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Removing FreeNAS peer entry'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Removing FreeNAS peer entry: {name}', name=peer['name'])

    def verify(self, id):
        return ['system']

    def run(self, id):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'Peer entry {0} does not exist'.format(id))

        remote = peer['address']
        remote_client = None
        hostid = self.dispatcher.call_sync('system.info.host_uuid')
        try:
            try:
                remote_client = get_replication_client(self.dispatcher, remote)

                call_task_and_check_state(
                    remote_client,
                    'peer.freenas.delete_local',
                    hostid
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
                'peer.freenas.delete_local',
                id
            ))

        finally:
            if remote_client:
                remote_client.disconnect()


@private
@description('Removes local FreeNAS peer entry from database')
@accepts(str)
class FreeNASPeerDeleteLocalTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Removing FreeNAS peer entry'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Removing FreeNAS peer entry {name}', name=peer['name'])

    def verify(self, id):
        return ['system']

    def run(self, id):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'FreeNAS peer entry {0} does not exist'.format(peer['name']))
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
@description('Updates FreeNAS peer entry in database')
@accepts(str, h.ref('peer'))
class FreeNASPeerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating FreeNAS peer entry'

    def describe(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Updating FreeNAS peer entry {name}', name=peer['name'])

    def verify(self, id, updated_fields):
        return ['system']

    def run(self, id, updated_fields):
        peer = self.datastore.get_by_id('peers', id)
        if not peer:
            raise TaskException(errno.ENOENT, 'FreeNAS peer entry {0} does not exist'.format(id))

        if 'address' in updated_fields:
            raise TaskException(errno.EINVAL, 'Address of FreeNAS peer cannot be updated')

        if 'type' in updated_fields:
            raise TaskException(errno.EINVAL, 'Type of FreeNAS peer cannot be updated')

        if 'id' in updated_fields:
            raise TaskException(errno.EINVAL, 'ID of FreeNAS peer cannot be updated')

        peer.update(updated_fields)

        self.datastore.update('peers', id, peer)
        self.dispatcher.dispatch_event('peer.changed', {
            'operation': 'update',
            'ids': [id]
        })


@private
@description('Updates remote FreeNAS peer entry')
@accepts(str)
class FreeNASPeerUpdateRemoteTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Updating remote FreeNAS peer'

    def describe(self, id):
        peer = self.datastore.get_by_id('peers', id)
        return TaskDescription('Updating remote FreeNAS peer {name}', name=peer['name'])

    def verify(self, id):
        return ['system']

    def run(self, id):
        peer = self.datastore.get_by_id('peers', id)
        hostid = self.dispatcher.call_sync('system.info.host_uuid')
        remote_client = None
        if not peer:
            raise TaskException(errno.ENOENT, 'FreeNAS peer entry {0} does not exist'.format(id))

        try:
            remote_client = get_replication_client(self.dispatcher, peer['address'])
            remote_peer = remote_client.call_sync('peer.query', [('id', '=', hostid)], {'single': True})
            if not remote_peer:
                raise TaskException(errno.ENOENT, 'Remote side of peer {0} does not exist'.format(peer['name']))

            ip_at_remote_side = remote_client.call_sync('management.get_sender_address').split(',', 1)[0]
            hostname = self.dispatcher.call_sync('system.general.get_config')['hostname']
            port = self.dispatcher.call_sync('service.sshd.get_config')['port']

            remote_peer['credentials']['port'] = port
            remote_peer['address'] = hostname

            call_task_and_check_state(
                remote_client,
                'peer.freenas.delete_local',
                hostid
            )

            remote_peer = exclude(remote_peer, 'created_at', 'updated_at')

            call_task_and_check_state(
                remote_client,
                'peer.freenas.create_local',
                remote_peer,
                ip_at_remote_side
            )
        finally:
            if remote_client:
                remote_client.disconnect()


def _depends():
    return ['SSHPlugin', 'SystemInfoPlugin']


def _init(dispatcher, plugin):
    global ssh_port
    global hostname
    ssh_port = dispatcher.call_sync('service.sshd.get_config')['port']
    hostname = dispatcher.call_sync('system.general.get_config')['hostname']

    # Register schemas
    plugin.register_schema_definition('peer', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'address': {'type': 'string'},
            'id': {'type': 'string'},
            'type': {'enum': ['freenas', 'ssh', 'amazon-s3']},
            'credentials': {'$ref': 'peer-credentials'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('freenas-credentials', {
        'type': 'object',
        'properties': {
            'type': {'enum': ['freenas']},
            'port': {'type': 'number'},
            'pubkey': {'type': 'string'},
            'hostkey': {'type': 'string'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('ssh-credentials', {
        'type': 'object',
        'properties': {
            'type': {'enum': ['ssh']},
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
            'type': {'enum': ['amazon-s3']},
            'access_key': {'type': 'string'},
            'secret_key': {'type': 'string'},
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
        'discriminator': 'type',
        'oneOf': [
            {'$ref': '{0}-credentials'.format(name)} for name in dispatcher.call_sync('peer.credentials_types')
        ]
    })

    # Register tasks
    plugin.register_task_handler("peer.create", PeerCreateTask)
    plugin.register_task_handler("peer.update", PeerUpdateTask)
    plugin.register_task_handler("peer.delete", PeerDeleteTask)
    plugin.register_task_handler("peer.freenas.create", FreeNASPeerCreateTask)
    plugin.register_task_handler("peer.freenas.create_local", FreeNASPeerCreateLocalTask)
    plugin.register_task_handler("peer.freenas.delete", FreeNASPeerDeleteTask)
    plugin.register_task_handler("peer.freenas.delete_local", FreeNASPeerDeleteLocalTask)
    plugin.register_task_handler("peer.freenas.update", FreeNASPeerUpdateTask)
    plugin.register_task_handler("peer.freenas.update_remote", FreeNASPeerUpdateRemoteTask)

    # Register event types
    plugin.register_event_type('peer.changed')

    # Event handlers methods
    def on_connection_change(args):
        global ssh_port
        global hostname
        new_ssh_port = dispatcher.call_sync('service.sshd.get_config')['port']
        new_hostname = dispatcher.call_sync('system.general.get_config')['hostname']
        if ssh_port != new_ssh_port or hostname != new_hostname:
            logger.debug('Address or SSH port has been updated. Populating change to FreeNAS peers')
            ssh_port = new_ssh_port
            hostname = new_hostname
            ids = dispatcher.call_sync('peer.query', [('type', '=', 'freenas')], {'select': 'id'})
            try:
                for id in ids:
                    dispatcher.call_task_sync('peer.freenas.update_remote', id)
            except RpcException:
                pass

    # Register event handlers
    plugin.register_event_handler('service.sshd.changed', on_connection_change)
    plugin.register_event_handler('system.general.changed', on_connection_change)

    # Create home directory and authorized keys file for replication user
    if not os.path.exists(REPL_USR_HOME):
        os.mkdir(REPL_USR_HOME)
    ssh_dir = os.path.join(REPL_USR_HOME, '.ssh')
    if not os.path.exists(ssh_dir):
        os.mkdir(ssh_dir)
    with open(AUTH_FILE, 'w') as auth_file:
        for host in dispatcher.call_sync('peer.query', [('type', '=', 'freenas')]):
            auth_file.write(host['credentials']['pubkey'])
