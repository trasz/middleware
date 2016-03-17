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
from task import Provider, Task, ProgressTask, TaskException
from paramiko import transport, sftp_client, ssh_exception, rsakey, dsskey


class BackupSSHProvider(Provider):
    def list_objects(self, backup):
        return self.dispatcher.call_task_sync('backup.ssh.list', backup)


class BackupSSHListTask(Task):
    def verify(self, backup):
        return []

    def run(self, backup):
        conn = open_ssh_connection(backup)
        sftp = sftp_client.SFTP.from_transport(conn)

        try:
            sftp.chdir(backup['directory'])
            return sftp.listdir()
        except ssh_exception.SSHException as err:
            raise TaskException(errno.EFAULT, 'Cannot list objects: {0}'.format(str(err)))


class BackupSSHPutTask(ProgressTask):
    def verify(self, backup, name, fd):
        return []

    def run(self, backup, name, fd):
        conn = open_ssh_connection(backup)
        sftp = sftp_client.SFTP.from_transport(conn)

        try:
            with os.fdopen(fd.fd, 'rb') as f:
                sftp.chdir(backup['directory'])
                sftp.putfo(name, f)
        except ssh_exception.SSHException as err:
            raise TaskException(errno.EFAULT, 'Cannot get object: {0}'.format(str(err)))



class BackupSSHGetTask(Task):
    def verify(self, backup, name, fd):
        return []

    def run(self, backup, name, fd):
        conn = open_ssh_connection(backup)
        sftp = sftp_client.SFTP.from_transport(conn)

        try:
            with os.fdopen(fd.fd, 'wb') as f:
                sftp.chdir(backup['directory'])
                sftp.getfo(name, f)
        except ssh_exception.SSHException as err:
            raise TaskException(errno.EFAULT, 'Cannot get object: {0}'.format(str(err)))


def split_hostport(str):
    if ':' in str:
        parts = str.split(':')
        return parts[0], int(parts[1])
    else:
        return str, 22


def try_key_auth(session, backup):
    try:
        key = rsakey.RSAKey.from_private_key(backup['privkey'])
        session.auth_publickey(backup['username'], key)
        return True
    except ssh_exception.SSHException:
        pass

    try:
        key = dsskey.DSSKey.from_private_key(backup['privkey'])
        session.auth_publickey(backup['username'], key)
        return True
    except ssh_exception.SSHException:
        pass

    return False


def open_ssh_connection(backup):
    try:
        session = transport.Transport(split_hostport(backup['hostport']))
        session.window_size = 1024 * 1024 * 1024
        session.packetizer.REKEY_BYTES = pow(2, 48)
        session.packetizer.REKEY_PACKETS = pow(2, 48)
        session.start_client()

        if backup['pubkey']:
            if try_key_auth(session, backup):
                return session
            else:
                raise Exception('Cannot authenticate using keys')

        session.auth_password(backup['username'], backup['password'])
        return session

    except socket.gaierror as err:
        raise Exception('Connection error: {0}'.format(err.strerror))
    except ssh_exception.BadAuthenticationType as err:
        raise Exception('Cannot authenticate: {0}'.format(str(err)))


def _depends():
    return ['BackupPlugin']


def _metadata():
    return {
        'type': 'backup',
        'method': 'ssh'
    }


def _init(dispatcher, plugin):
    plugin.register_schema_definition('backup-ssh', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'hostport': {'type': 'string'},
            'username': {'type ': 'string'},
            'password': {'type': ['string', 'null']},
            'privkey': {'type': ['string', 'null']},
            'hostkey': {'type': ['string', 'null']},
            'directory': {'type': 'string'}
        }
    })

    plugin.register_provider('backup.ssh', BackupSSHProvider)
    plugin.register_task_handler('backup.ssh.list', BackupSSHListTask)
    plugin.register_task_handler('backup.ssh.get', BackupSSHGetTask)
    plugin.register_task_handler('backup.ssh.put', BackupSSHPutTask)
