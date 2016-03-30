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
import base64
from freenas.utils import first_or_default
from freenas.dispatcher.client import Client, FileDescriptor
from paramiko import AuthenticationException
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from utils import get_replication_client
from task import Task, Provider, TaskException, TaskWarning, VerifyException, query
from libc.stdlib cimport malloc, free
from posix.unistd cimport read, write
from libc.stdint cimport *


logger = logging.getLogger('ReplicationTransportPlugin')


REPL_HOME = '/var/tmp/replication'
AUTH_FILE = os.path.join(REPL_HOME, '.ssh/authorized_keys')


cdef int read_fd(int fd, uint8_t *buf, int nbytes, int curr_pos):
    cdef int ret
    cdef int done = 0

    while True:
        try:
            with nogil:
                ret = read(fd, <uint8_t *>(buf + curr_pos), nbytes - done)
        except IOError as e:
            if e.errno in (errno.EINTR, e.errno == errno.EAGAIN):
                continue
            else:
                raise

        done += ret
        if (done == nbytes) or (ret == 0):
            return done


cdef int write_fd(int fd, uint8_t *buf, int nbytes):
    cdef int ret
    cdef int done = 0

    while True:
        try:
            with nogil:
                ret = write(fd, <uint8_t *>(buf + done), nbytes - done)
        except IOError as e:
            if e.errno in (errno.EINTR, errno.EAGAIN):
                continue
            else:
                raise

        done += ret
        if ret == 0:
            raise IOError

        if done == nbytes:
            return done


class HostProvider(Provider):
    @query('known-host')
    def query(self, filter=None, params=None):
        return self.datastore.query('replication.known_hosts', *(filter or []), **(params or {}))

    @private
    def get_keys(self):
        key_paths = ['/etc/ssh/ssh_host_rsa_key.pub', '/etc/replication/key.pub']
        keys = []
        try:
            for key_path in key_paths:
                with open(key_path) as f:
                     keys.append(f.read())
        except FileNotFoundError:
            raise RpcException(errno.ENOENT, 'Key file {0} not found'.format(key_path))

        return [i for i in keys]


class TransportProvider(Provider):
    @private
    def plugin_types(self):
        return ['compress', 'decompress', 'encrypt', 'decrypt', 'throttle']


@private
@description('Send side of replication transport layer')
@accepts(
    object,
    h.all_of(
        h.ref('replication-transport'),
        h.required('client_address')
    )
)
class TransportSendTask(Task):
    def verify(self, fd, transport):
        client_address = transport.get('client_address')
        if not client_address:
            raise VerifyException(errno.ENOENT, 'Please specify address of a remote')

        if 'server_address' in transport:
            raise VerifyException(errno.EINVAL, 'Server address cannot be specified')

        host = self.dispatcher.call_sync(
            'replication.known_host.query',
            [('name', '=', client_address)],
            {'single': True}
        )
        if not host:
            raise VerifyException(
                errno.ENOENT,
                'Client address {0} is not on local known replication hosts list'.format(client_address)
            )

        return []

    def run(self, fd, transport):
        cdef uint8_t *buffer
        cdef int curr_pos = 0
        cdef int ret
        cdef int magic = 0xdeadbeef
        try:
            buffer_size = transport.get('buffer_size', 1024*1024)
            buffer = <uint8_t *>malloc(buffer_size * sizeof(uint8_t))

            client_address = transport.get('client_address')
            remote_client = get_replication_client(self.dispatcher, client_address)
            server_address = remote_client.call_sync('management.get_sender_address')[0]
            server_port = transport.get('server_port', 0)

            for conn_option in socket.getaddrinfo(server_address, server_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, sock_type, proto, canonname, addr = conn_option
                try:
                    sock = socket.socket(af, sock_type, proto)
                except OSError:
                    sock = None
                    continue
                try:
                    sock.bind(addr)
                    sock.listen(1)
                except OSError:
                    sock.close()
                    sock = None
                    continue
                break

            if sock is None:
                raise TaskException(errno.EACCES, 'Could not open a socket at address {0}'.format(server_address))

            token_size = transport.get('auth_token_size', 1024)
            token = transport.get('auth_token')
            if token:
                actual_size = len(base64.b64decode(token.encode('utf-8')))
                if actual_size != token_size:
                    raise TaskException(
                        errno.EINVAL,
                        'Provided token size {0} does not match token size parameter value {1}'.format(
                            actual_size,
                            token_size
                        )
                    )
            else:
                token = base64.b64encode(os.urandom(token_size)).decode('utf-8')
                transport['auth_token'] = token

            sock_addr = sock.getsockname()
            transport['server_address'] = sock_addr[0]
            transport['server_port'] = sock_addr[1]
            transport['buffer_size'] = buffer_size

            recv_task_id = remote_client.call_task_async('replication.transport.receive', transport)

            conn, addr = sock.accept()
            if addr != client_address:
                raise TaskException(
                    errno.EINVAL,
                    'Connection from an unexpected address {0} - desired {1}'.format(
                        addr,
                        client_address
                    )
                )

            conn_fd = conn.fileno()

            try:
                ret = read_fd(conn_fd, buffer, token_size - curr_pos, curr_pos)
                curr_pos += ret
                if ret != token_size:
                    raise OSError
            except OSError:
                raise TaskException(
                    errno.ECONNABORTED,
                    'Connection with {0} aborted before authentication'.format(client_address)
                )
            recvd_token = <bytes> buffer[:curr_pos]
            curr_pos = 0

            if base64.b64decode(token.encode('utf-8')) != recvd_token:
                raise TaskException(
                    errno.EAUTH,
                    'Transport layer authentication failed. Expected token {0}, was {1}'.format(
                        token,
                        recvd_token.decode('utf-8')
                    )
                )

            plugins = transport.get('transport_plugins', [])
            header_rd, header_wr = os.pipe()
            last_rd_fd = header_rd
            subtasks = []
            raw_subtasks = []

            for type in ['compress', 'encrypt', 'throttle']:
                plugin = first_or_default(lambda p: p['name'] == type, plugins)
                if plugin:
                    rd, wr = os.pipe()
                    plugin['read_fd'] = FileDescriptor(last_rd_fd)
                    plugin['write_fd'] = FileDescriptor(wr)
                    last_rd_fd = rd
                    raw_subtasks.append(('replication.transport.{0}'.format(type), plugin))

            if len(raw_subtasks):
                raw_subtasks[-1]['write_fd'] = conn_fd
                for subtask in raw_subtasks:
                    subtasks.append(self.run_subtask(subtask))
            else:
                header_wr = conn_fd
            try:
                while True:
                    ret = read_fd(fd, buffer, buffer_size, 0)
                    write_fd(header_wr, <uint8_t *> magic, sizeof(magic))
                    write_fd(header_wr, <uint8_t *> ret, sizeof(ret))
                    if ret == 0:
                        break
                    else:
                        write_fd(header_wr, buffer, ret)
            except IOError:
                raise TaskException(errno.ECONNABORTED, 'Transport connection closed unexpectedly')

        finally:
            free(buffer)


@private
@description('Receive side of replication transport layer')
@accepts(h.ref('replication-transport'))
class TransportReceiveTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return


@private
@description('Compress the input stream and pass it to the output')
@accepts(h.ref('compress-plugin'))
class TransportCompressTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return


@private
@description('Decompress the input stream and pass it to the output')
@accepts(h.ref('decompress-plugin'))
class TransportDecompressTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return


@private
@description('Encrypt the input stream and pass it to the output')
@accepts(h.ref('encrypt-plugin'))
class TransportEncryptTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return


@private
@description('Decrypt the input stream and pass it to the output')
@accepts(h.ref('decrypt-plugin'))
class TransportDecryptTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return


@private
@description('Limit throughput to one buffer size per second')
@accepts(h.ref('throttle-plugin'))
class TransportThrottleTask(Task):
    def verify(self, transport):
        return []

    def run(self, transport):
        return



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

        with open(AUTH_FILE, 'a') as auth_file:
            auth_file.write(known_host['pubkey'])

        self.dispatcher.dispatch_event('replication.host.changed', {
            'operation': 'create',
            'ids': [id]
        })

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

        with open(AUTH_FILE, 'r') as auth_file:
            auth_keys = auth_file.read()

        new_auth_keys = ''
        for line in auth_keys.splitlines():
            if not known_host_pubkey in line:
                new_auth_keys = new_auth_keys + '\n' + line

        with open(AUTH_FILE, 'w') as auth_file:
            auth_file.write(new_auth_keys)

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
            'client_address': {'type': 'string'},
            'server_port': {'type': 'integer'},
            'buffer_size': {'type': 'integer'},
            'auth_token': {'type': 'string'},
            'auth_token_size': {'type': 'integer'},
            'estimated_size': {'type': 'integer'},
            'transport_plugins': {
                'type': ['array', 'null'],
                'items': {'$ref': 'replication-transport-plugin'}
            }
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('compress-plugin', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'type': {'type': 'string'},
            'read_fd': {'type': 'object'},
            'write_fd': {'type': 'object'},
            'level': {'type': 'integer'},
            'buffer_size': {'type': 'integer'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('decompress-plugin', {
        'discriminator': 'name',
        'oneOf': [
            {'$ref': 'compress-plugin'}
        ]
    })

    plugin.register_schema_definition('encrypt-plugin', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'type': {'type': 'string'},
            'read_fd': {'type': 'object'},
            'write_fd': {'type': 'object'},
            'key': {'type': 'string'},
            'renewal_interval': {'type': 'integer'},
            'buffer_size': {'type': 'integer'}
        },
        'additionalProperties': False
    })

    plugin.register_schema_definition('decrypt-plugin', {
        'discriminator': 'name',
        'oneOf': [
            {'$ref': 'encrypt-plugin'}
        ]
    })

    plugin.register_schema_definition('throttle-plugin', {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
            'buffer_size': {'type': 'integer'},
            'read_fd': {'type': 'object'},
            'write_fd': {'type': 'object'}
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
    plugin.register_provider('replication.transport', TransportProvider)
    plugin.register_provider('replication.host', HostProvider)

    # Register transport plugin schema
    plugin.register_schema_definition('replication-transport-plugin', {
        'discriminator': 'name',
        'oneOf': [
            {'$ref': '{0}-plugin'.format(name)} for name in dispatcher.call_sync('replication.transport.plugin_types')
        ]
    })

    # Register tasks
    plugin.register_task_handler("replication.transport.send", TransportSendTask)
    plugin.register_task_handler("replication.transport.receive", TransportReceiveTask)
    plugin.register_task_handler("replication.transport.compress", TransportCompressTask)
    plugin.register_task_handler("replication.transport.decompress", TransportDecompressTask)
    plugin.register_task_handler("replication.transport.encrypt", TransportEncryptTask)
    plugin.register_task_handler("replication.transport.decrypt", TransportDecryptTask)
    plugin.register_task_handler("replication.transport.throttle", TransportThrottleTask)
    plugin.register_task_handler("replication.hosts_pair.create", HostsPairCreateTask)
    plugin.register_task_handler("replication.known_host.create", KnownHostCreateTask)
    plugin.register_task_handler("replication.hosts_pair.delete", HostsPairDeleteTask)
    plugin.register_task_handler("replication.known_host.delete", KnownHostDeleteTask)

    # Register event handlers
    plugin.register_event_type('replication.host.changed')

    #Create home directory and authorized keys file for replication user
    if not os.path.exists(REPL_HOME):
        os.mkdir(REPL_HOME)
    ssh_dir = os.path.join(REPL_HOME, '.ssh')
    if not os.path.exists(ssh_dir):
        os.mkdir(ssh_dir)
    with open(AUTH_FILE, 'w') as auth_file:
        for host in dispatcher.call_sync('replication.host.query'):
            auth_file.write(host['pubkey'])

