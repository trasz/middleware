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
from libc.stdio cimport *
from libc.errno cimport *


cdef extern from "openssl/ossl_typ.h" nogil:
    ctypedef struct EVP_CIPHER_CTX:
        pass

    ctypedef struct EVP_CIPHER:
        pass

    ctypedef struct ENGINE:
        pass


cdef extern from "openssl/conf.h" nogil:
    void OPENSSL_config(const char *config_name)
    void OPENSSL_no_config()


cdef extern from "openssl/evp.h" nogil:
    void OpenSSL_add_all_algorithms()
    void OpenSSL_add_all_ciphers()
    void OpenSSL_add_all_digests()
    void EVP_cleanup()
    EVP_CIPHER_CTX *EVP_CIPHER_CTX_new()
    int EVP_EncryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *type, ENGINE *impl, unsigned char *key, unsigned char *iv)
    int EVP_EncryptUpdate(EVP_CIPHER_CTX *ctx, unsigned char *outb, int *outl, unsigned char *inb, int inl)
    int EVP_EncryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *out, int *outl)
    int EVP_DecryptInit_ex(EVP_CIPHER_CTX *ctx, const EVP_CIPHER *type, ENGINE *impl, unsigned char *key, unsigned char *iv)
    int EVP_DecryptUpdate(EVP_CIPHER_CTX *ctx, unsigned char *outb, int *outl, unsigned char *inb, int inl)
    int EVP_DecryptFinal_ex(EVP_CIPHER_CTX *ctx, unsigned char *outm, int *outl)
    void EVP_CIPHER_CTX_free(EVP_CIPHER_CTX *ctx)
    const EVP_CIPHER *EVP_aes_256_ctr()


cdef extern from "openssl/err.h" nogil:
    void ERR_load_crypto_strings()
    void ERR_free_strings()
    void ERR_print_errors_fp(FILE *fp)


logger = logging.getLogger('ReplicationTransportPlugin')


REPL_HOME = '/var/tmp/replication'
AUTH_FILE = os.path.join(REPL_HOME, '.ssh/authorized_keys')


encryption_data = {}


cipher_types = {
    'AES256': {
        'function': <uintptr_t> &EVP_aes_256_ctr,
        'key_size': 256,
        'iv_size': 128
    }
}


cdef uint32_t read_fd(int fd, void *buf, uint32_t nbytes, uint32_t curr_pos) nogil:
    cdef uint32_t ret
    cdef uint32_t done = 0

    while True:
        ret = read(fd, <uint8_t *>(buf + curr_pos + done), nbytes - done)
        if ret == -1:
            if errno in (EINTR, EAGAIN):
                continue
            else:
                return ret

        done += ret
        if (done == nbytes) or (ret == 0):
            return done


cdef uint32_t write_fd(int fd, void *buf, uint32_t nbytes) nogil:
    cdef uint32_t ret
    cdef uint32_t done = 0

    while True:
        ret = write(fd, <uint8_t *>(buf + done), nbytes - done)
        if ret == -1:
            if errno in (EINTR, EAGAIN):
                continue
            else:
                return ret

        done += ret
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
            raise RpcException(ENOENT, 'Key file {0} not found'.format(key_path))

        return [i for i in keys]


class TransportProvider(Provider):
    @private
    def plugin_types(self):
        return ['compress', 'decompress', 'encrypt', 'decrypt', 'throttle']

    def set_encryption_data(self, key, data):
        encryption_data[key] = data

    def get_encryption_data(self, key):
        return encryption_data.pop(key)


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
        self.finished = threading.Event()
        self.addr = None
        self.recv_status = None
        self.header_t_status = None

    def verify(self, fd, transport):
        client_address = transport.get('client_address')
        if not client_address:
            raise VerifyException(ENOENT, 'Please specify address of a remote')

        if 'server_address' in transport:
            raise VerifyException(EINVAL, 'Server address cannot be specified')

        host = self.dispatcher.call_sync(
            'replication.host.query',
            [('name', '=', client_address)],
            {'single': True}
        )
        if not host:
            raise VerifyException(
                ENOENT,
                'Client address {0} is not on local known replication hosts list'.format(client_address)
            )

        return []

    def run(self, fd, transport):
        cdef uint8_t *token_buffer
        cdef uint32_t ret
        cdef uint32_t token_size

        sock = None
        conn = None
        fds = []
        try:
            buffer_size = transport.get('buffer_size', 1024*1024)

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
                        ETIMEDOUT,
                        'Timeout while waiting for connection from {0}'.format(client_address)
                    )
                except OSError:
                    sock.close()
                    sock = None
                    continue
                break

            if sock is None:
                raise TaskException(EACCES, 'Could not open a socket at address {0}'.format(server_address))
            logger.debug('Created a TCP socket at {0}:{1}'.format(*addr))

            token_size = transport.get('auth_token_size', 1024)
            token = transport.get('auth_token')
            if token:
                actual_size = len(base64.b64decode(token.encode('utf-8')))
                if actual_size != token_size:
                    raise TaskException(
                        EINVAL,
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
                    EINVAL,
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
            ret = read_fd(conn_fd, token_buffer, token_size, 0)
            if ret != token_size:
                raise TaskException(
                    ECONNABORTED,
                    'Connection with {0} aborted before authentication'.format(client_address)
                )
            recvd_token = <bytes> token_buffer[:token_size]

            if base64.b64decode(token.encode('utf-8')) != recvd_token:
                raise TaskException(
                    ECONNABORTED,
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
                    if type == 'encrypt':
                        plugin['auth_token'] = token
                        plugin['remote'] = client_address
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
            self.addr = addr

            header_t = threading.Thread(target=self.pack_headers, args=(fd, header_wr, buffer_size))
            header_t.start()

            def check_recv_status():
                if self.recv_status.get('state') != 'FINISHED':
                    close_fds(fds)
                    raise TaskException(
                        ECONNABORTED,
                        'Receive process connected to {0}:{1} finished unexpectedly'.format(*addr)
                    )
                else:
                    logger.debug('Receive task at {0}:{1} finished'.format(*addr))

            def check_header_t_status():
                if self.header_t_status[1] == -1:
                    close_fds(fds)
                    raise TaskException(
                        self.header_t_status[2],
                        'Header write failed during transmission to {0}:{1}'.format(*addr)
                    )
                if self.header_t_status[0] == -1:
                    close_fds(fds)
                    raise TaskException(
                        self.header_t_status[2],
                        'Data read failed during transmission to {0}:{1}'.format(*addr)
                    )

            self.finished.wait()
            if self.recv_status:
                check_recv_status()
                header_t.join()
                check_header_t_status()
            else:
                self.finished.clear()
                check_header_t_status()
                self.finished.wait()
                check_recv_status()

            logger.debug('All data fetched for transfer to {0}:{1}. Waiting for plugins to close.'.format(*addr))
            self.join_subtasks(*subtasks)

            logger.debug('Send to {0}:{1} finished. Closing connection'.format(*addr))
            remote_client.disconnect()

        finally:
            free(token_buffer)
            if sock:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            if conn:
                conn.shutdown(socket.SHUT_RDWR)
                conn.close()
            close_fds(fds)

    def get_recv_status(self, status):
        self.recv_status = status
        self.finished.set()

    def pack_headers(self, r_fd, w_fd, buf_size):
        cdef uint32_t *buffer
        cdef uint32_t ret
        cdef uint32_t ret_wr
        cdef uint32_t buffer_size = buf_size
        cdef uint32_t header_size = 2 * sizeof(uint32_t)
        cdef int rd_fd = r_fd.fd
        cdef int wr_fd = w_fd
        try:
            with nogil:
                buffer = <uint32_t *>malloc((buffer_size + header_size) * sizeof(uint8_t))

                buffer[0] = 0xdeadbeef
            while True:
                with nogil:
                    ret = read_fd(rd_fd, buffer, buffer_size, header_size)
                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Got {0} bytes of payload ({1}:{2})'.format(ret, *self.addr))

                with nogil:
                    buffer[1] = ret

                    if ret == -1:
                        break
                    if ret == 0:
                        ret_wr = write_fd(wr_fd, buffer, header_size)
                        break
                    else:
                        ret_wr = write_fd(wr_fd, buffer, ret + header_size)

                    if ret_wr == -1:
                        break

                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Written {0} bytes -> TCP socket ({1}:{2})'.format(ret, *self.addr))

            self.finished.set()
            self.header_t_status = (ret, ret_wr, errno)

        finally:
            free(buffer)


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
        self.header_t_status = None

    def verify(self, transport):
        if 'auth_token' not in transport:
            raise VerifyException(ENOENT, 'Authentication token is not specified')

        if 'server_address' not in transport:
            raise VerifyException(ENOENT, 'Server address is not specified')

        if 'server_port' not in transport:
            raise VerifyException(ENOENT, 'Server port is not specified')

        server_address = transport.get('server_address')

        host = self.dispatcher.call_sync(
            'replication.host.query',
            [('name', '=', server_address)],
            {'single': True}
        )
        if not host:
            raise VerifyException(
                ENOENT,
                'Server address {0} is not on local known replication hosts list'.format(server_address)
            )

        return []

    def run(self, transport):
        cdef uint8_t *token_buf
        cdef uint32_t ret

        sock = None
        fds = []
        try:
            buffer_size = transport.get('buffer_size', 1024*1024)

            self.estimated_size = transport.get('estimated_size', 0)
            server_address = transport.get('server_address')
            server_port = transport.get('server_port')
            token = base64.b64decode(transport.get('auth_token').encode('utf-8'))
            token_size = transport.get('auth_token_size')
            token_buf = token

            if len(token) != token_size:
                raise TaskException(
                    EINVAL,
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
                raise TaskException(EACCES, 'Could not connect to a socket at address {0}'.format(server_address))
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
                        plugin['auth_token'] = transport.get('auth_token').encode('utf-8')
                    rd, wr = os.pipe()
                    fds.append(rd)
                    fds.append(wr)
                    plugin['read_fd'] = FileDescriptor(last_rd_fd)
                    plugin['write_fd'] = FileDescriptor(wr)
                    last_rd_fd = rd
                    subtasks.append(self.run_subtask('replication.transport.{0}'.format(type), plugin))
                    logger.debug('Registered {0} transport layer plugin for {1}:{2} connection'.format(type, *addr))

            ret = write_fd(conn_fd, token_buf, token_size)
            if ret == -1:
                raise TaskException(ECONNABORTED, 'Transport connection closed unexpectedly')
            elif ret != token_size:
                raise TaskException(EINVAL, 'Transport failed to write token to socket')
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
            logger.debug('Started zfs receive task for {0}:{1} connection'.format(*addr))

            header_t = threading.Thread(target=self.unpack_headers, args=(last_rd_fd, zfs_wr, buffer_size))
            header_t.start()
            header_t.join()

            if self.header_t_status[2] != 0xdeadbeef:
                raise TaskException(
                    EINVAL,
                    'Bad magic {0} received. Expected {1}'.format(self.header_t_status[2], 0xdeadbeef)
                )
            if self.header_t_status[0] == -1:
                raise TaskException(
                    self.header_t_status[3],
                    'Data read failed during transmission from {0}:{1}'.format(*self.addr)
                )
            if self.header_t_status[1] == -1:
                raise TaskException(
                    self.header_t_status[3],
                    'Data write failed during transmission from {0}:{1}'.format(*self.addr)
                )

            self.running = False
            logger.debug('All data fetched for transfer from {0}:{1}. Waiting for plugins to close.'.format(*addr))
            self.join_subtasks(*subtasks)
            progress_t.join()
            logger.debug('Receive from {0}:{1} finished. Closing connection'.format(*addr))

        finally:
            if sock:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            close_fds(fds)

    def count_progress(self):
        last_done = 0
        progress = 0
        total_time = 0
        while self.running:
            if self.estimated_size:
                progress = int((float(self.done) / float(self.estimated_size)) * 100)
                if progress > 100:
                    progress = 100
            self.set_progress(progress, 'Transfer speed {0} B/s'.format(self.done - last_done))
            last_done = self.done
            time.sleep(1)
            total_time += 1

        if total_time:
            transfer_speed = int(float(self.done) / float(total_time))
        else:
            transfer_speed = 0
        logger.debug('Overall transfer speed {0} B/s - {1}:{2}'.format(transfer_speed, *self.addr))

    def unpack_headers(self, r_fd, w_fd, buf_size):
        cdef uint32_t *buffer
        cdef uint32_t *header_buffer
        cdef uint32_t length
        cdef uint32_t magic = 0xdeadbeef
        cdef uint32_t buffer_size = buf_size
        cdef uint32_t header_size = 2 * sizeof(uint32_t)
        cdef uint32_t ret
        cdef uint32_t ret_wr
        cdef int rd_fd = r_fd
        cdef int wr_fd = wr_fd
        try:
            with nogil:
                buffer = <uint32_t *>malloc(buffer_size * sizeof(uint8_t))
                header_buffer = <uint32_t *>malloc(header_size * sizeof(uint8_t))
            while True:
                with nogil:
                    ret = read_fd(rd_fd, header_buffer, header_size, 0)
                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Got {0} bytes of header ({1}:{2})'.format(ret, *self.addr))

                with nogil:
                    if ret != header_size:
                        ret = -1
                        break
                    if header_buffer[0] != magic:
                        break
                    length = header_buffer[1]
                    if length == 0:
                        break

                    ret = read_fd(rd_fd, buffer, length, 0)
                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Got {0} bytes of payload ({1}:{2})'.format(ret, *self.addr))
                with nogil:
                    if ret != length:
                        ret = -1
                        break

                self.done += ret

                with nogil:
                    ret_wr = write_fd(wr_fd, buffer, length)
                    if ret_wr == -1:
                        break
                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Written {0} bytes of payload -> zfs.receive ({1}:{2})'.format(length, *self.addr))

            self.header_t_status = (ret, ret_wr, header_buffer[0], errno)

        finally:
            free(buffer)
            free(header_buffer)


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
            raise VerifyException(ENOENT, 'Read file descriptor is not specified')

        if 'write_fd' not in plugin:
            raise VerifyException(ENOENT, 'Write file descriptor is not specified')

        return []

    def run(self, plugin):
        cdef uint8_t *buffer
        cdef uint32_t buffer_size
        cdef uint32_t ret
        cdef uint32_t ret_wr
        cdef uint32_t done = 0
        cdef uint8_t running = 1
        cdef int rd_fd
        cdef int wr_fd

        timer_ovf = threading.Event()

        def timer():
            IF REPLICATION_TRANSPORT_DEBUG:
                logger.debug('Starting throttle task timer')
            while running:
                time.sleep(1)
                timer_ovf.set()

        try:
            buffer_size = plugin.get('buffer_size', 50*1024*1024)
            buffer = <uint8_t *>malloc(buffer_size * sizeof(uint8_t))
            rd_fd = plugin.get('read_fd').fd
            wr_fd = plugin.get('write_fd').fd
            IF REPLICATION_TRANSPORT_DEBUG:
                logger.debug('Starting throttle task - max transfer speed {0} B/s'.format(buffer_size))

            timer_t = threading.Thread(target=timer)
            timer_t.start()

            while True:
                with nogil:
                    ret = read(rd_fd, buffer + done, buffer_size - done)
                    if ret == -1:
                        if errno in (EINTR, EAGAIN):
                            continue
                        else:
                            break

                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Got {0} bytes from read file descriptor'.format(ret))

                if ret == 0:
                    logger.debug('Null byte received. Ending task.')
                    running = 0
                    break

                with nogil:
                    ret_wr = write_fd(wr_fd, buffer + done, ret)
                    if ret_wr == -1:
                        break
                IF REPLICATION_TRANSPORT_DEBUG:
                    logger.debug('Written {0} bytes to write file descriptor'.format(ret))

                done += ret
                if done == buffer_size:
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Buffer full. Waiting')
                    timer_ovf.wait()
                    timer_ovf.clear()
                    done = 0
                    IF REPLICATION_TRANSPORT_DEBUG:
                        logger.debug('Throttle task released by timer.')

            if ret == -1:
                raise TaskException(errno, 'Throttle task failed on read from file descriptor')

            if ret_wr == -1:
                raise TaskException(errno, 'Throttle task failed on write to file descriptor')

        finally:
            free(buffer)
            close_fds([wr_fd, rd_fd])


@description('Exchange keys with remote machine for replication purposes')
@accepts(str, str, str)
class HostsPairCreateTask(Task):
    def describe(self, username, remote, password):
        return 'Exchange keys with remote machine for replication purposes'

    def verify(self, username, remote, password):
        if self.datastore.exists('replication.known_hosts', ('id', '=', remote)):
            raise VerifyException(EEXIST, 'Known hosts entry for {0} already exists'.format(remote))

        return ['system']

    def run(self, username, remote, password):
        remote_client = Client()
        try:
            remote_client.connect('ws+ssh://{0}@{1}'.format(username, remote), password=password)
            remote_client.login_service('replicator')
        except (AuthenticationException, OSError, ConnectionRefusedError):
            raise TaskException(ECONNABORTED, 'Cannot connect to {0}'.format(remote))

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
            raise VerifyException(EEXIST, 'Known hosts entry for {0} already exists'.format(known_host['name']))

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
            raise VerifyException(ENOENT, 'Known hosts entry for {0} does not exist'.format(remote))

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
                EINVAL,
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
            raise VerifyException(ENOENT, 'Known hosts entry for {0} does not exist'.format(name))

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

def close_fds(fds):
    try:
        for fd in fds:
            os.close(fd)
    except OSError:
        pass

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
            'read_fd': {'type': 'fd'},
            'write_fd': {'type': 'fd'},
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
            'read_fd': {'type': 'fd'},
            'write_fd': {'type': 'fd'},
            'auth_token': {'type': 'string'},
            'remote': {'type': 'string'},
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
            'read_fd': {'type': 'fd'},
            'write_fd': {'type': 'fd'}
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
