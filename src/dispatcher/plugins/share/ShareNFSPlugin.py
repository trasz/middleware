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
from task import Task, TaskStatus, Provider, TaskException, VerifyException
from freenas.dispatcher.rpc import RpcException, description, accepts, returns, private
from freenas.dispatcher.rpc import SchemaHelper as h
from freenas.utils import normalize
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


@private
@description("Adds new NFS share")
@accepts(h.ref('share'))
class CreateNFSShareTask(Task):
    def describe(self, share):
        return "Creating NFS share {0}".format(share['name'])

    def verify(self, share):
        properties = share['properties']
        if (properties.get('maproot_user') or properties.get('maproot_group')) and \
           (properties.get('mapall_user') or properties.get('mapall_group')):
            raise VerifyException(errno.EINVAL, 'Cannot set maproot and mapall properties simultaneously')

        return ['service:nfs']

    def run(self, share):
        normalize(share['properties'], {
            'alldirs': False,
            'read_only': False,
            'maproot_user': None,
            'maproot_group': None,
            'mapall_user': None,
            'mapall_group': None,
            'hosts': [],
            'security': []
        })

        id = self.datastore.insert('shares', share)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.call_sync('service.reload', 'nfs')
        return id


@private
@description("Updates existing NFS share")
@accepts(str, h.ref('share'))
class UpdateNFSShareTask(Task):
    def describe(self, id, updated_fields):
        return "Updating NFS share {0}".format(id)

    def verify(self, id, updated_fields):
        if 'properties' in updated_fields:
            properties = updated_fields['properties']
            if (properties.get('maproot_user') or properties.get('maproot_group')) and \
               (properties.get('mapall_user') or properties.get('mapall_group')):
                raise VerifyException(errno.EINVAL, 'Cannot set maproot and mapall properties simultaneously')

        return ['service:nfs']

    def run(self, id, updated_fields):
        share = self.datastore.get_by_id('shares', id)
        share.update(updated_fields)
        self.datastore.update('shares', id, share)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.call_sync('service.reload', 'nfs')
        self.dispatcher.dispatch_event('share.nfs.query.changed', {
            'operation': 'update',
            'ids': [id]
        })


@private
@description("Removes NFS share")
@accepts(str)
class DeleteNFSShareTask(Task):
    def describe(self, id):
        return "Deleting NFS share {0}".format(id)

    def verify(self, id):
        return ['service:nfs']

    def run(self, id):
        self.datastore.delete('shares', id)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'nfs')
        self.dispatcher.call_sync('service.reload', 'nfs')
        self.dispatcher.dispatch_event('share.nfs.query.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@private
@description("Imports existing NFS share")
@accepts(h.ref('share'))
class ImportNFSShareTask(CreateNFSShareTask):
    def describe(self, share):
        return "Importing NFS share {0}".format(share['name'])

    def verify(self, share):
        properties = share['properties']
        if (properties.get('maproot_user') or properties.get('maproot_group')) and \
           (properties.get('mapall_user') or properties.get('mapall_group')):
            raise VerifyException(errno.EINVAL, 'Cannot set maproot and mapall properties simultaneously')

        return super(ImportNFSShareTask, self).verify(share)

    def run(self, share):
        return super(ImportNFSShareTask, self).run(share)


@private
class TerminateNFSConnectionTask(Task):
    def verify(self, address):
        return []

    def run(self, address):
        raise TaskException(errno.ENXIO, 'Not supported')


def _metadata():
    return {
        'type': 'sharing',
        'subtype': 'FILE',
        'perm_type': 'PERM',
        'method': 'nfs'
    }


def _depends():
    return ['ZfsPlugin', 'SharingPlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('share-nfs', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['share-nfs']},
            'alldirs': {'type': 'boolean'},
            'read_only': {'type': 'boolean'},
            'maproot_user': {'type': ['string', 'null']},
            'maproot_group': {'type': ['string', 'null']},
            'mapall_user': {'type': ['string', 'null']},
            'mapall_group': {'type': ['string', 'null']},
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
    plugin.register_task_handler("share.nfs.import", ImportNFSShareTask)
    plugin.register_task_handler("share.nfs.terminate_connection", TerminateNFSConnectionTask)
    plugin.register_provider("share.nfs", NFSSharesProvider)
    plugin.register_event_type('share.nfs.query.changed')
