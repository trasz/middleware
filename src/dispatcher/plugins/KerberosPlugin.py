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

from freenas.dispatcher.rpc import SchemaHelper as h, accepts, returns, description
from task import Task, TaskException, VerifyException, Provider, query


class KerberosRealmsProvider(Provider):
    @query('kerberos-realm')
    def query(self, filter=None, params=None):
        return self.datastore.query('kerberos.realms', *(filter or []), **(params or {}))


class KerberosKeytabsProvider(Provider):
    @query('kerberos-keytab')
    def query(self):
        pass


@accepts(h.ref('kerberos-realm'))
class KerberosRealmCreateTask(Task):
    def verify(self, realm):
        return ['system']

    def run(self, realm):
        id = self.datastore.insert('kerberos.realms', realm)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'kerberos')
        self.dispatcher.dispatch_event('kerberos.realm.changed', {
            'operation': 'create',
            'ids': [id]
        })
        return id


@accepts(str, h.ref('kerberos-realm'))
class KerberosRealmUpdateTask(Task):
    def verify(self, id, realm):
        return ['system']

    def run(self, id, updated_fields):
        realm = self.datastore.get_by_id('kerberos.realms', id)
        realm.update(updated_fields)
        self.datastore.update('kerberos.realms', id, realm)
        self.dispatcher.call_sync('etcd.generation.generate_group', 'kerberos')
        self.dispatcher.dispatch_event('kerberos.realm.changed', {
            'operation': 'update',
            'ids': [id]
        })

        return id


@accepts(str)
class KerberosRealmDeleteTask(Task):
    def verify(self, id):
        return ['system']

    def run(self, id):
        self.datastore.delete('kerberos.realms', id)
        self.dispatcher.dispatch_event('kerberos.realm.changed', {
            'operation': 'delete',
            'ids': [id]
        })


def _init(context, plugin):
    plugin.register_schema_definition('kerberos-realm', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'realm': {'type': 'string'},
            'kdc_address': {'type': ['string', 'null']},
            'admin_server_address': {'type': ['string', 'null']},
            'password_server_address': {'type': ['string', 'null']}
        }
    })

    plugin.register_provider('kerberos.realm', KerberosRealmsProvider)
    plugin.register_provider('kerberos.keytab', KerberosKeytabsProvider)

    plugin.register_event_type('kerberos.realm.changed')
    plugin.register_task_handler('kerberos.realm.create', KerberosRealmCreateTask)
    plugin.register_task_handler('kerberos.realm.update', KerberosRealmUpdateTask)
    plugin.register_task_handler('kerberos.realm.delete', KerberosRealmDeleteTask)
