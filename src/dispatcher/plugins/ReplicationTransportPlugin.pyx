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
import threading
import time
import base64
from freenas.utils import first_or_default
from freenas.dispatcher.client import Client
from freenas.dispatcher.fd import FileDescriptor
from paramiko import AuthenticationException
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, description, accepts, returns, private
from utils import get_replication_client
from task import Task, ProgressTask, Provider, TaskException, TaskWarning, VerifyException, query
from libc.stdlib cimport malloc, free
from posix.unistd cimport read, write
from libc.stdint cimport *


logger = logging.getLogger('ReplicationTransportPlugin')


REPL_HOME = '/var/tmp/replication'
AUTH_FILE = os.path.join(REPL_HOME, '.ssh/authorized_keys')


cdef uint32_t read_fd(int fd, void *buf, uint32_t nbytes, uint32_t curr_pos):
    cdef uint32_t ret
    cdef uint32_t done = 0

    while True:
        try:
            with nogil:
                ret = read(fd, <uint8_t *>(buf + curr_pos + done), nbytes - done)
        except IOError as e:
            if e.errno in (errno.EINTR, e.errno == errno.EAGAIN):
                continue
            else:
                raise

        done += ret
        if (done == nbytes) or (ret == 0):
            return done


cdef uint32_t write_fd(int fd, void *buf, uint32_t nbytes):
    cdef uint32_t ret
    cdef uint32_t done = 0

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
    FileDescriptor,
    h.all_of(
        h.ref('replication-transport'),
        h.required('client_address', 'receive_properties')
    )
)
class TransportSendTask(Task):
    def __init__(self, dispatcher, datastore):
        super(TransportSendTask, self).__init__(dispatcher, datastore)
        self.recv_finished = threading.Event()

    def verify(self, fd, transport):
        client_address = transport.get('client_address')
        if not client_address:
            raise VerifyException(errno.ENOENT, 'Please specify address of a remote')

        if 'server_address' in transport:
            raise VerifyException(errno.EINVAL, 'Server address cannot be specified')

        host = self.dispatcher.call_sync(
            'replication.host.query',
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
        cdef uint8_t *token_buffer
        cdef uint32_t *buffer
        cdef uint32_t ret
        cdef uint32_t buffer_size
        cdef uint32_t header_size = 2 * sizeof(uint32_t)
        cdef uint32_t token_size

        sock = None
        conn = None
        fds = []
        try:
            buffer_size = transport.get('buffer_size', 1024*1024)
            buffer = <uint32_t *>malloc((buffer_size + header_size) * sizeof(uint8_t))

            client_address = transport.get('client_address')
            remote_client = get_replication_client(self.dispatcher, client_address)
            server_address = remote_client.call_sync('management.get_sender_address').split(',', 1)[0]
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
                    sock.settimeout(30)
                    sock.listen(1)
                except socket.timeout:
                    raise TaskException(
                        errno.ETIMEDOUT,
                        'Timeout while waiting for connection from {0}'.format(client_address)
                    )
                except OSError:
                    sock.close()
                    sock = None
                    continue
                break

            if sock is None:
                raise TaskException(errno.EACCES, 'Could not open a socket at address {0}'.format(server_address))
            logger.debug('Created a TCP socket at {0}:{1}'.format(*addr))

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
            transport['auth_token_size'] = token_size

            recv_task_id = remote_client.call_task_async(
                'replication.transport.receive',
                transport,
                callback=self.get_recv_status,
                timeout=604800
            )

            conn, addr = sock.accept()
            if addr[0] != client_address:
                raise TaskException(
                    errno.EINVAL,
                    'Connection from an unexpected address {0} - desired {1}'.format(
                        addr[0],
                        client_address
                    )
                )
            logger.debug('New connection from {0}:{1} to {2}:{3}'.format(*(addr + sock_addr)))

            conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buffer_size)

            conn_fd = conn.fileno()
            fds.append(conn_fd)

            token_buffer = <uint8_t *>malloc(token_size * sizeof(uint8_t))
            try:
                ret = read_fd(conn_fd, token_buffer, token_size, 0)
                if ret != token_size:
                    raise OSError
            except OSError:
                raise TaskException(
                    errno.ECONNABORTED,
                    'Connection with {0} aborted before authentication'.format(client_address)
                )
            recvd_token = <bytes> token_buffer[:token_size]

            if base64.b64decode(token.encode('utf-8')) != recvd_token:
                raise TaskException(
                    errno.EAUTH,
                    'Transport layer authentication failed. Expected token {0}, was {1}'.format(
                        token,
                        recvd_token.decode('utf-8')
                    )
                )
            logger.debug('{0}:{1} connection authentication finished successfully'.format(*addr))

            plugins = transport.get('transport_plugins', [])
            header_rd, header_wr = os.pipe()
            fds.append(header_rd)
            fds.append(header_wr)
            last_rd_fd = header_rd
            subtasks = []
            raw_subtasks = []

            for type in ['compress', 'encrypt', 'throttle']:
                plugin = first_or_default(lambda p: p['name'] == type, plugins)
                if plugin:
                    rd, wr = os.pipe()
                    fds.append(rd)
                    fds.append(wr)
                    plugin['read_fd'] = FileDescriptor(last_rd_fd)
                    plugin['write_fd'] = FileDescriptor(wr)
                    last_rd_fd = rd
                    raw_subtasks.append(['replication.transport.{0}'.format(type), plugin])
                    logger.debug('Registered {0} transport layer plugin for {1}:{2} connection'.format(type, *addr))

            if len(raw_subtasks):
                logger.debug('Starting plugins for {0}:{1} connection'.format(*addr))
                raw_subtasks[-1][-1]['write_fd'] = FileDescriptor(conn_fd)
                for subtask in raw_subtasks:
                    subtasks.append(self.run_subtask(*subtask))
            else:
                header_wr = conn_fd

            logger.debug(
                'Transport layer plugins registration finished for {0}:{1} connection. Starting transfer.'.format(*addr)
            )
            buffer[0] = 0xdeadbeef
            try:
                while True:
                    ret = read_fd(fd.fd, buffer, buffer_size, header_size)
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Got {0} bytes of payload ({1}:{2})'.format(ret, *addr))
                    buffer[1] = ret

                    if ret == 0:
                        write_fd(header_wr, buffer, header_size)
                        break
                    else:
                        write_fd(header_wr, buffer, ret + header_size)

                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Written {0} bytes -> TCP socket ({1}:{2})'.format(ret, *addr))
            except IOError:
                raise TaskException(errno.ECONNABORTED, 'Transport connection closed unexpectedly')

            logger.debug('All data fetched for transfer to {0}:{1}. Waiting for plugins to close.'.format(*addr))

            self.join_subtasks(*subtasks)

            logger.debug('Waiting for receive task at {0}:{1} to finish'.format(*addr))
            self.recv_finished.wait()

            logger.debug('Send to {0}:{1} finished. Closing connection'.format(*addr))
            remote_client.disconnect()

        finally:
            free(buffer)
            free(token_buffer)
            if sock:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            if conn:
                conn.shutdown(socket.SHUT_RDWR)
                conn.close()
            try:
                for fd in fds:
                    os.close(fd)
            except OSError:
                pass

    def get_recv_status(self, status):
        self.recv_finished.set()
        if status.get('state') != 'FINISHED':
            args = status['args'][0]
            raise TaskException(
                errno.ECONNABORTED,
                'Receive process connected to {0}:{1} finished unexpectedly'.format(
                    args['server_address'],
                    args['server_port']
                )
            )


@private
@description('Receive side of replication transport layer')
@accepts(h.ref('replication-transport'))
class TransportReceiveTask(ProgressTask):
    def __init__(self, dispatcher, datastore):
        super(TransportReceiveTask, self).__init__(dispatcher, datastore)
        self.done = 0
        self.estimated_size = 0
        self.running = True
        self.addr = None

    def verify(self, transport):
        if 'auth_token' not in transport:
            raise VerifyException(errno.ENOENT, 'Authentication token is not specified')

        if 'server_address' not in transport:
            raise VerifyException(errno.ENOENT, 'Server address is not specified')

        if 'server_port' not in transport:
            raise VerifyException(errno.ENOENT, 'Server port is not specified')

        server_address = transport.get('server_address')

        host = self.dispatcher.call_sync(
            'replication.host.query',
            [('name', '=', server_address)],
            {'single': True}
        )
        if not host:
            raise VerifyException(
                errno.ENOENT,
                'Server address {0} is not on local known replication hosts list'.format(server_address)
            )

        return []

    def run(self, transport):
        cdef uint32_t *buffer
        cdef uint32_t *header_buffer
        cdef uint8_t *token_buf
        cdef uint32_t ret
        cdef uint32_t length
        cdef uint32_t magic = 0xdeadbeef
        cdef uint32_t buffer_size
        cdef uint32_t header_size = 2 * sizeof(uint32_t)

        sock = None
        fds = []
        try:
            buffer_size = transport.get('buffer_size', 1024*1024)
            buffer = <uint32_t *>malloc(buffer_size * sizeof(uint8_t))
            header_buffer = <uint32_t *>malloc(header_size * sizeof(uint8_t))

            self.estimated_size = transport.get('estimated_size', 0)
            server_address = transport.get('server_address')
            server_port = transport.get('server_port')
            token = base64.b64decode(transport.get('auth_token').encode('utf-8'))
            token_size = transport.get('auth_token_size')
            token_buf = token

            if len(token) != token_size:
                raise TaskException(
                    errno.EINVAL,
                    'Token size {0} does not match token size field {1}'.format(len(token), token_size)
                )

            for conn_option in socket.getaddrinfo(server_address, server_port, socket.AF_UNSPEC, socket.SOCK_STREAM):
                af, sock_type, proto, canonname, addr = conn_option
                try:
                    sock = socket.socket(af, sock_type, proto)
                except OSError:
                    sock = None
                    continue
                try:
                    sock.connect(addr)
                except OSError:
                    sock.close()
                    sock = None
                    continue
                break

            if sock is None:
                raise TaskException(errno.EACCES, 'Could not connect to a socket at address {0}'.format(server_address))
            self.addr = addr
            logger.debug('Connected to a TCP socket at {0}:{1}'.format(*addr))

            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size)

            conn_fd = sock.fileno()
            fds.append(conn_fd)

            plugins = transport.get('transport_plugins', [])
            last_rd_fd = conn_fd
            subtasks = []

            for type in ['encrypt', 'compress']:
                plugin = first_or_default(lambda p: p['name'] == type, plugins)
                if plugin:
                    if plugin['name'] == 'compress':
                        plugin['name'] = 'decompress'
                    elif plugin['name'] == 'encrypt':
                        plugin['name'] = 'decrypt'
                    rd, wr = os.pipe()
                    fds.append(rd)
                    fds.append(wr)
                    plugin['read_fd'] = FileDescriptor(last_rd_fd)
                    plugin['write_fd'] = FileDescriptor(wr)
                    last_rd_fd = rd
                    subtasks.append(self.run_subtask('replication.transport.{0}'.format(type), plugin))
                    logger.debug('Registered {0} transport layer plugin for {1}:{2} connection'.format(type, *addr))

            try:
                write_fd(conn_fd, token_buf, token_size)
            except IOError:
                raise TaskException(errno.ECONNABORTED, 'Transport connection closed unexpectedly')
            logger.debug('Authentication token sent to {0}:{1}'.format(*addr))

            zfs_rd, zfs_wr = os.pipe()
            fds.append(zfs_wr)
            fds.append(zfs_rd)
            recv_props = transport.get('receive_properties')
            subtasks.append(self.run_subtask(
                'zfs.receive',
                recv_props['name'],
                FileDescriptor(zfs_rd),
                recv_props.get('force', False),
                recv_props.get('nomount', False),
                recv_props.get('props', None),
                recv_props.get('limitds', None)
            ))

            progress_t = threading.Thread(target=self.count_progress)
            progress_t.start()
            logger.debug('Started zfs.receive task for {0}:{1} connection'.format(*addr))

            try:
                while True:
                    ret = read_fd(last_rd_fd, header_buffer, header_size, 0)
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Got {0} bytes of header ({1}:{2})'.format(ret, *addr))
                    if ret != header_size:
                        raise IOError
                    if header_buffer[0] != magic:
                        raise TaskException(
                            errno.EINVAL,
                            'Bad magic {0} received. Expected {1}'.format(header_buffer[0], magic)
                        )
                    length = header_buffer[1]
                    if length == 0:
                        IF REPLICATION_TRANSPORT_DEBUG:
                            logger.debug('Received header with 0 payload length. Ending connection.')
                        break

                    ret = read_fd(last_rd_fd, buffer, length, 0)
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Got {0} bytes of payload ({1}:{2})'.format(ret, *addr))
                    if ret != length:
                        raise IOError

                    self.done += ret

                    write_fd(zfs_wr, buffer, length)
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Written {0} bytes of payload -> zfs.receive ({1}:{2})'.format(length, *addr))

            except IOError:
                raise TaskException(errno.ECONNABORTED, 'Transport connection closed unexpectedly')

            self.running = False
            logger.debug('All data fetched for transfer from {0}:{1}. Waiting for plugins to close.'.format(*addr))
            self.join_subtasks(*subtasks)
            progress_t.join()
            logger.debug('Receive from {0}:{1} finished. Closing connection'.format(*addr))

        finally:
            free(buffer)
            free(header_buffer)
            if sock:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            try:
                for fd in fds:
                    os.close(fd)
            except OSError:
                pass

    def count_progress(self):
        last_done = 0
        progress = 0
        start_time = time.time()
        while self.running:
            if self.estimated_size:
                progress = int((float(self.done) / float(self.estimated_size)) * 100)
                if progress > 100:
                    progress = 100
            self.set_progress(progress, 'Transfer speed {0} B/s'.format(self.done - last_done))
            last_done = self.done
            time.sleep(1)

        transfer_speed = int(float(self.done) / float(time.time() - start_time))
        logger.debug('Overall transfer speed {0} B/s - {1}:{2}'.format(transfer_speed, *self.addr))


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
    def verify(self, plugin):
        if 'read_fd' not in plugin:
            raise VerifyException(errno.ENOENT, 'Read file descriptor is not specified')

        if 'write_fd' not in plugin:
            raise VerifyException(errno.ENOENT, 'Write file descriptor is not specified')

        return []

    def run(self, plugin):
        cdef uint8_t *buffer
        cdef uint32_t buffer_size
        cdef uint32_t ret
        cdef uint32_t done = 0
        cdef uint8_t running = 1
        cdef int rd_fd
        cdef int wr_fd

        timer_ovf = threading.Event()

        def timer():
            while running:
                time.sleep(1)
                done = 0
                timer_ovf.set()

        try:
            buffer_size = plugin.get('buffer_size', 50*1024*1024)
            buffer = <uint8_t *>malloc(buffer_size * sizeof(uint8_t))
            rd_fd = plugin.get('read_fd').fd
            wr_fd = plugin.get('write_fd').fd

            timer_t = threading.Thread(target=timer)
            timer_t.start()

            while True:
                try:
                    with nogil:
                        ret = read(rd_fd, buffer + done, buffer_size - done)
                except IOError as e:
                    if e.errno in (errno.EINTR, e.errno == errno.EAGAIN):
                        continue

                if ret == 0:
                    running = 0
                    break

                write_fd(wr_fd, buffer, ret)

                done += ret
                if done == buffer_size:
                    timer_ovf.wait()
                    timer_ovf.clear()

        finally:
            free(buffer)
            try:
                os.close(wr_fd)
                os.close(rd_fd)
            except OSError:
                pass


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
        ip_at_remote_side = remote_client.call_sync('management.get_sender_address').split(',', 1)[0]

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

            ip_at_remote_side = remote_client.call_sync('management.get_sender_address').split(',', 1)[0]
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
            },
            'receive_properties':{
                'name': {'type': 'string'},
                'force': {'type': 'boolean'},
                'nomount': {'type': 'boolean'},
                'props': {'type': 'object'},
                'limitds': {'type': 'object'}
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
            'name': {
                'type': 'string',
                'enum': ['throttle']
            },
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
