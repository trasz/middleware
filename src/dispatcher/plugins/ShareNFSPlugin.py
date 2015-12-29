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

import errno
import logging
from task import Task, TaskStatus, Provider, TaskException
from freenas.dispatcher.rpc import RpcException, description, accepts, returns, private
from freenas.dispatcher.rpc import SchemaHelper as h
from utils import split_dataset


logger = logging.getLogger(__name__)


@description("Provides info about configured NFS shares")
class NFSSharesProvider(Provider):
    @private
    @accepts(str)
    def get_connected_clients(self, share_id=None):
        share = self.datastore.get_one('shares', ('type', '=', 'nfs'), ('id', '=', share_id))
        result = []
        f = open('/var/db/mountdtab')
        for line in f:
            host, path = line.split()
            if share['target_path'] in path:
                result.append({
                    'host': host,
                    'share': share_id,
                    'user': None,
                    'connected_at': None
                })

        f.close()
        return result


@description("Adds new NFS share")
@accepts(h.ref('nfs-share'))
class CreateNFSShareTask(Task):
    def describe(self, share):
        return "Creating NFS share {0}".format(share['name'])

    def verify(self, share):
        return ['service:nfs']

    def run(self, share):
        id = self.datastore.insert('shares', share)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.call_sync('service.reload', 'nfs')
        return id


@description("Updates existing NFS share")
@accepts(str, h.ref('nfs-share'))
class UpdateNFSShareTask(Task):
    def describe(self, name, updated_fields):
        return "Updating NFS share {0}".format(name)

    def verify(self, name, updated_fields):
        return ['service:nfs']

    def run(self, name, updated_fields):
        share = self.datastore.get_by_id('shares', name)
        share.update(updated_fields)
        self.datastore.update('shares', name, share)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.dispatch_event('share.nfs.changed', {
            'operation': 'update',
            'ids': [name]
        })


@description("Removes NFS share")
@accepts(str)
class DeleteNFSShareTask(Task):
    def describe(self, name):
        return "Deleting NFS share {0}".format(name)

    def verify(self, name):
        return ['service:nfs']

    def run(self, name):
        self.datastore.delete('shares', name)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.call_sync('service.reload', 'nfs')
        self.dispatcher.dispatch_event('share.nfs.changed', {
            'operation': 'delete',
            'ids': [name]
        })


def _metadata():
    return {
        'type': 'sharing',
        'subtype': 'file',
        'perm_type': 'PERMS',
        'method': 'nfs'
    }


def _depends():
    return ['ZfsPlugin', 'SharingPlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('nfs-share-properties', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'alldirs': {'type': 'boolean'},
            'read_only': {'type': 'boolean'},
            'maproot_user': {'type': 'string'},
            'maproot_group': {'type': 'string'},
            'mapall_user': {'type': 'string'},
            'mapall_group': {'type': 'string'},
            'hosts': {
                'type': 'array',
                'items': {'type': 'string'}
            },
            'security': {
                'type': 'array',
                'items': {
                    'type': 'string',
                    'enum': ['sys', 'krb5', 'krb5i', 'krb5p']
                }
            }
        }
    })

    plugin.register_task_handler("share.nfs.create", CreateNFSShareTask)
    plugin.register_task_handler("share.nfs.update", UpdateNFSShareTask)
    plugin.register_task_handler("share.nfs.delete", DeleteNFSShareTask)
    plugin.register_provider("share.nfs", NFSSharesProvider)
    plugin.register_event_type('share.nfs.changed')
