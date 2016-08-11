#
# Copyright 2015 iXsystems, Inc.
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
from datastore import DatastoreException
from task import Task, Provider, TaskException, query, TaskDescription
from freenas.dispatcher.rpc import RpcException, accepts, description, generator
from freenas.dispatcher.rpc import SchemaHelper as h
from lib.system import system, SubprocessException

logger = logging.getLogger('NTPPlugin')


@description("Provides access to NTP Servers configuration")
class NTPServersProvider(Provider):
    @query('ntp-server')
    @generator
    def query(self, filter=None, params=None):
        return self.datastore.query_stream('ntpservers', *(filter or []), **(params or {}))


@description("Adds new NTP Server")
@accepts(h.all_of(
    h.ref('ntp-server'),
    h.required('address'),
), bool)
class NTPServerCreateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating NTP Server"

    def describe(self, ntp, force=False):
        return TaskDescription("Creating NTP Server {name}", name=ntp['address'])

    def verify(self, ntp, force=False):
        return ['system']

    def run(self, ntp, force=False):
        try:
            system('ntpdate', '-q', ntp['address'])
        except SubprocessException:
            if not force:
                raise TaskException(
                    errno.EACCES,
                    'Server could not be reached. Check "Force" to continue regardless.'
                )

        minpoll = ntp.get('minpoll', 6)
        maxpoll = ntp.get('maxpoll', 10)

        if not maxpoll > minpoll:
            raise TaskException(errno.EINVAL, 'Max Poll should be higher than Min Poll')

        try:
            pkey = self.datastore.insert('ntpservers', ntp)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'ntpd')
            self.dispatcher.call_sync('service.restart', 'ntpd')
            self.dispatcher.dispatch_event('ntp_server.changed', {
                'operation': 'create',
                'ids': [pkey]
            })
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot create NTP Server: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))
        return pkey


@description("Updates NTP Server")
@accepts(str, h.ref('ntp-server'), bool)
class NTPServerUpdateTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating NTP Server"

    def describe(self, id, updated_fields, force=False):
        ntp = self.datastore.get_by_id('ntpservers', id)
        return TaskDescription("Creating NTP Server {name}", name=ntp.get('address', '') or '')

    def verify(self, id, updated_fields, force=False):
        return ['system']

    def run(self, id, updated_fields, force=False):
        ntp = self.datastore.get_by_id('ntpservers', id)
        if ntp is None:
            raise TaskException(errno.ENOENT, 'NTP Server with given ID does not exist')

        try:
            if 'address' in updated_fields:
                system('ntpdate', '-q', updated_fields['address'])
        except SubprocessException:
            if not force:
                raise TaskException(
                    errno.EINVAL,
                    'Server could not be reached. Check "Force" to continue regardless.'
                )

        minpoll = updated_fields.get('minpoll', ntp.get('minpoll'))
        maxpoll = updated_fields.get('maxpoll', ntp.get('maxpoll'))

        if minpoll is not None and maxpoll is not None and not maxpoll > minpoll:
            raise TaskException(errno.EINVAL, 'Max Poll should be higher than Min Poll')

        try:
            ntp.update(updated_fields)
            self.datastore.update('ntpservers', id, ntp)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'ntpd')
            self.dispatcher.call_sync('service.restart', 'ntpd')
            self.dispatcher.dispatch_event('ntp_server.changed', {
                'operation': 'update',
                'ids': [id]
            })
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot update NTP Server: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))
        return id


@description("Deletes NTP Server")
@accepts(str)
class NTPServerDeleteTask(Task):
    @classmethod
    def early_describe(cls):
        return "Creating NTP Server"

    def describe(self, id):
        ntp = self.datastore.get_by_id('ntpservers', id)
        return TaskDescription("Creating NTP Server {name}", name=ntp.get('address', '') or '')

    def verify(self, id):
        return ['system']

    def run(self, id):
        ntp = self.datastore.get_by_id('ntpservers', id)
        if ntp is None:
            raise TaskException(errno.ENOENT, 'NTP Server with given ID does not exist')

        try:
            self.datastore.delete('ntpservers', id)
            self.dispatcher.call_sync('etcd.generation.generate_group', 'ntpd')
            self.dispatcher.call_sync('service.restart', 'ntpd')
            self.dispatcher.dispatch_event('ntp_server.changed', {
                'operation': 'delete',
                'ids': [id]
            })
        except DatastoreException as e:
            raise TaskException(errno.EBADMSG, 'Cannot delete NTP Server: {0}'.format(str(e)))
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot generate certificate: {0}'.format(str(e)))


def _init(dispatcher, plugin):
    plugin.register_schema_definition('ntp-server', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'address': {'type': 'string'},
            'burst': {'type': 'boolean'},
            'iburst': {'type': 'boolean'},
            'prefer': {'type': 'boolean'},
            'minpoll': {'type': 'integer'},
            'maxpoll': {'type': 'integer'},
        },
        'additionalProperties': False,
    })

    # Register events
    plugin.register_event_type('ntp_server.changed')

    # Register provider
    plugin.register_provider("ntp_server", NTPServersProvider)

    # Register tasks
    plugin.register_task_handler("ntp_server.create", NTPServerCreateTask)
    plugin.register_task_handler("ntp_server.update", NTPServerUpdateTask)
    plugin.register_task_handler("ntp_server.delete", NTPServerDeleteTask)
