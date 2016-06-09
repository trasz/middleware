#+
# Copyright 2014 iXsystems, Inc.
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
import tempfile
from freenas.dispatcher.jsonenc import dumps, loads
from freenas.dispatcher.client import Client
from paramiko import RSAKey, AuthenticationException, SSHException
from task import TaskException


def first_or_default(f, iterable, default=None):
    i = list(filter(f, iterable))
    if i:
        return i[0]

    return default


def split_dataset(dataset_path):
    pool = dataset_path.split('/')[0]
    return pool, dataset_path


def save_config(conf_path, name_mod, entry):
    with open(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)), 'w', encoding='utf-8') as conf_file:
        conf_file.write(dumps(entry))


def load_config(conf_path, name_mod):
    with open(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)), 'r', encoding='utf-8') as conf_file:
        return loads(conf_file.read())


def delete_config(conf_path, name_mod):
    os.remove(os.path.join(conf_path, '.config-{0}.json'.format(name_mod)))


def get_replication_client(dispatcher, remote):
    host = dispatcher.call_sync(
        'peer.query',
        [('address', '=', remote), ('type', '=', 'replication')],
        {'single': True}
    )
    if not host:
        raise TaskException(errno.ENOENT, 'There are no known keys to connect to {0}'.format(remote))

    with open('/etc/replication/key') as f:
        pkey = RSAKey.from_private_key(f)

    credentials = host['credentials']

    try:
        client = Client()
        with tempfile.NamedTemporaryFile('w') as host_key_file:
            host_key_file.write(credentials['hostkey'])
            host_key_file.flush()
            client.connect(
                'ws+ssh://replication@{0}'.format(remote),
                port=credentials['port'],
                host_key_file=host_key_file.name,
                pkey=pkey
            )
        client.login_service('replicator')
        return client

    except (AuthenticationException, SSHException):
        raise TaskException(errno.EAUTH, 'Cannot connect to {0}'.format(remote))
    except (OSError, ConnectionRefusedError):
        raise TaskException(errno.ECONNREFUSED, 'Cannot connect to {0}'.format(remote))
    except IOError:
        raise TaskException(errno.EINVAL, 'Provided host key is not valid')


def call_task_and_check_state(client, name, *args):
    result = client.call_task_sync(name, *args)
    if result['state'] != 'FINISHED':
        raise TaskException(errno.EFAULT, 'Task failed: {0}'.format(
            result['error']['message']
        ))
    return result
