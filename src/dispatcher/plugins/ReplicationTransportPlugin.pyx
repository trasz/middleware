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

import cython
import libzfs
import errno
import socket
import logging
import os
from freenas.dispatcher.client import Client
from paramiko import AuthenticationException
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from utils import get_replication_client
from task import Task, Provider, TaskException, TaskWarning, VerifyException, query

logger = logging.getLogger('ReplicationTransportPlugin')


class HostProvider(Provider):
    @query('known-host')
    def query(self, filter=None, params=None):
        return self.datastore.query('replication.known_hosts', *(filter or []), **(params or {}))

    def get_keys(self):
        key_paths = ['/etc/ssh/ssh_host_dsa_key.pub', '/etc/replication/key.pub']
        keys = []
        try:
            for key_path in key_paths:
                with open(key_path) as f:
                     keys.append(f.read())
        except FileNotFoundError:
            raise RpcException(errno.ENOENT, 'Key file {0} not found'.format(key_path))

        return [i for i in keys]

@private
@description('Set up a TCP connection for replication purposes')
@accepts(h.ref('replication-transport'))
class TransportCreateTask(Task):
    def describe(self, transport):
        return 'Setting up a replication transport'

    def verify(self, transport):

        return ['system']

    def run(self, afp):
        return


@private
@description('Exchange keys with remote machine for replication purposes')
@accepts(str, str, str)
class HostsPairCreateTask(Task):
    def describe(self, username, remote, password):
        return 'Exchange keys with remote machine for replication purposes'

    def verify(self, username, remote, password):
        if self.datastore.exists('replication.known_hosts', ('id', '=', remote)):
            raise VerifyException(errno.EEXIST, 'Known hosts entry for {0} already exists'.format(remote))

        return ['system']

    def run(self, username, remote, password):
        remote_client = Client()
        try:
            remote_client.connect('ws+ssh://{0}@{1}'.format(username, remote), password=password)
            remote_client.login_service('replicator')
        except AuthenticationException:
            raise TaskException(errno.EAUTH, 'Cannot connect to {0}'.format(remote))
        except (OSError, ConnectionRefusedError):
            raise TaskException(errno.ECONNREFUSED, 'Cannot connect to {0}'.format(remote))

        local_keys = self.dispatcher.call_sync('replication.host.get_keys')
        remote_keys = remote_client.call_sync('replication.host.get_keys')
        ip_at_remote_side = remote_client.call_sync('management.get_sender_address')[0]

        remote_host_key = remote + ' ' + remote_keys[0].rsplit(' ', 1)[0]
        local_host_key = ip_at_remote_side + ' ' + local_keys[0].rsplit(' ', 1)[0]

        remote_client.call_task_sync(
            'replication.known_host.create',
            {
                'name': ip_at_remote_side,
                'id': ip_at_remote_side,
                'pubkey': local_keys[1],
                'hostkey': local_host_key
            }
        )

        self.join_subtasks(self.run_subtask(
            'replication.known_host.create',
            {
                'name': remote,
                'id': remote,
                'pubkey': remote_keys[1],
                'hostkey': remote_host_key
            }
        ))


@private
@description('Create known host entry in database')
@accepts(h.ref('known-host'))
class KnownHostCreateTask(Task):
    def verify(self, known_host):
        if self.datastore.exists('replication.known_hosts', ('id', '=', known_host['name'])):
            raise VerifyException(errno.EEXIST, 'Known hosts entry for {0} already exists'.format(known_host['name']))

        return ['system']

    def run(self, known_host):
        id = self.datastore.insert('replication.known_hosts', known_host)

        user = self.dispatcher.call_sync('user.query', [('username', '=', 'replication')], {'single': True})
        if not user:
            raise TaskException(errno.ENOENT, 'User replication does not exist')

        pubkey_entry = user.get('sshpubkey')

        self.join_subtasks(self.run_subtask(
            'user.update',
            user['id'],
            {
                'sshpubkey': pubkey_entry if pubkey_entry else '' + '\n' + known_host['pubkey']
            }
        ))

        self.dispatcher.dispatch_event('replication.host.changed', {
            'operation': 'create',
            'ids': [id]
        })

@private
@description('Remove keys making local and remote accessible from each other for replication user')
@accepts(str)
class HostsPairDeleteTask(Task):
    def verify(self, remote):
        if not self.datastore.exists('replication.known_hosts', ('id', '=', remote)):
            raise VerifyException(errno.ENOENT, 'Known hosts entry for {0} does not exist'.format(remote))

        return ['system']

    def run(self, remote):
        try:
            remote_client = get_replication_client(self.dispatcher, remote)

            ip_at_remote_side = remote_client.call_sync('management.get_sender_address')[0]
            remote_client.call_task_sync(
                'replication.known_host.delete',
                ip_at_remote_side
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
            'replication.known_host.delete',
            remote
        ))


@private
@description('Remove known host entry from database')
@accepts(str)
class KnownHostDeleteTask(Task):
    def verify(self, name):
        if not self.datastore.exists('replication.known_hosts', ('id', '=', name)):
            raise VerifyException(errno.ENOENT, 'Known hosts entry for {0} does not exist'.format(name))

        return ['system']

    def run(self, name):
        known_host = self.dispatcher.call_sync('replication.host.query', [('id', '=', name)], {'single': True})
        known_host_pubkey = known_host['pubkey']
        self.datastore.delete('replication.known_hosts', name)

        user = self.dispatcher.call_sync('user.query', [('username', '=', 'replication')], {'single': True})
        if user:
            pubkey = ''
            for line in user.get('sshpubkey', '').splitlines():
                if not known_host_pubkey in line:
                    pubkey = pubkey + '\n' + line

            self.join_subtasks(self.run_subtask(
                'user.update',
                user['id'],
                {
                    'sshpubkey': pubkey
                }
            ))

        self.dispatcher.dispatch_event('replication.host.changed', {
            'operation': 'delete',
            'ids': [name]
        })


def _init(dispatcher, plugin):
    # Register schemas
    plugin.register_schema_definition('replication-transport', {
        'type': 'object',
        'properties': {
            'server_address': {'type': 'string'},
            'server_port': {'type': 'integer'},
            'buffer_size': {'type': 'integer'},
            'auth_token_size': {'type': 'integer'},
            'transport_plugins': {
                'type': ['array', 'null'],
                'items': {'$ref': 'replication-transport-plugin'},
            }
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('replication-transport-plugin', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'params': {'type': 'object'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('known-host', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'id': {'type': 'string'},
            'pubkey': {'type': 'string'},
            'hostkey': {'type': 'string'}
        },
        'additionalProperties': False
    })

    # Register providers
    plugin.register_provider('replication.host', HostProvider)

    # Register tasks
    plugin.register_task_handler("replication.transport.create", TransportCreateTask)
    plugin.register_task_handler("replication.hosts_pair.create", HostsPairCreateTask)
    plugin.register_task_handler("replication.known_host.create", KnownHostCreateTask)
    plugin.register_task_handler("replication.hosts_pair.delete", HostsPairDeleteTask)
    plugin.register_task_handler("replication.known_host.delete", KnownHostDeleteTask)

    # Register event handlers
    plugin.register_event_type('replication.host.changed')
