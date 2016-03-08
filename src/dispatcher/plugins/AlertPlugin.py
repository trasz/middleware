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
import socket
from datetime import datetime

from datastore import DatastoreException
from freenas.dispatcher.rpc import (
    RpcException,
    SchemaHelper as h,
    accepts,
    description,
    returns,
)
from task import Provider, Task, TaskException, VerifyException, query

logger = logging.getLogger('AlertPlugin')
registered_alerts = {}


@description('Provides access to the alert system')
class AlertsProvider(Provider):
    @query('alert')
    def query(self, filter=None, params=None):
        return self.datastore.query(
            'alerts', *(filter or []), **(params or {})
        )

    @description("Dismisses/Deletes an alert from the database")
    @accepts(int)
    def dismiss(self, id):
        try:
            self.datastore.delete('alerts', id)
            self.dispatcher.dispatch_event('alert.changed', {
                'operation': 'delete',
                'ids': [id]
            })
        except DatastoreException as e:
            raise TaskException(
                errno.EBADMSG,
                'Cannot delete alert: {0}'.format(str(e))
            )

    @description("Emits an event for the provided alert")
    @accepts(h.ref('alert'))
    @returns(int)
    def emit(self, alert):
        alertprops = registered_alerts.get(alert['name'])
        if alertprops is None:
            raise RpcException(
                errno.ENOENT,
                "Alert {0} not registered".format(alert['name'])
            )

        if 'when' not in alert:
            alert['when'] = datetime.utcnow()

        # Try to find the first matching namespace
        emitters = None
        dot = alert['name'].split('.')
        for i in range(len(dot), 0, -1):
            namespace = '.'.join(dot[0:i])
            afilter = self.datastore.get_one(
                'alert-filters', ('name', '=', namespace),
                ('severity', '=', alert['severity']),
            )
            if afilter:
                emitters = afilter['emitters']

        # If there are no filters configured, set default emitters
        if emitters is None:
            if alert['severity'] == 'CRITICAL':
                emitters = ['UI', 'EMAIL']
            else:
                emitters = ['UI']

        alert['dismissed'] = False
        id = self.datastore.insert('alerts', alert)
        self.dispatcher.dispatch_event('alert.changed', {
                'operation': 'create',
            'ids': [id]
        })

        if 'EMAIL' in emitters:
            try:
                self.dispatcher.call_sync('mail.send', {
                    'subject': '{0}: {1}'.format(socket.gethostname(), alertprops['verbose_name']),
                    'message': '{0} - {1}'.format(alert['severity'], alert['description']),
                })
            except RpcException:
                logger.error('Failed to send email alert', exc_info=True)

        return id

    @description("Returns list of registered alerts")
    @accepts()
    @returns(h.ref('alert-registration'))
    def get_registered_alerts(self):
        return registered_alerts

    @description("Registers an alert")
    @accepts(str, h.any_of(str, None))
    @returns(bool)
    def register_alert(self, name, verbose_name=None):
        if name not in registered_alerts:
            registered_alerts[name] = {
                'name': name,
                'verbose_name': verbose_name,
            }
            return True
        return False


@description('Provides access to the alerts filters')
class AlertsFiltersProvider(Provider):

    @query('alert-filter')
    def query(self, filter=None, params=None):
        return self.datastore.query(
            'alert-filters', *(filter or []), **(params or {})
        )


@description("Creates an Alert Filter")
@accepts(h.ref('alert-filter'))
class AlertFilterCreateTask(Task):
    def describe(self, alertfilter):
        return 'Creating alert filter {0}'.format(alertfilter['name'])

    def verify(self, alertfilter):
        return []

    def run(self, alertfilter):
        id = self.datastore.insert('alert-filters', alertfilter)

        self.dispatcher.dispatch_event('alert.filter.changed', {
            'operation': 'create',
            'ids': [id]
        })


@description("Deletes the specified Alert Filter")
@accepts(str)
class AlertFilterDeleteTask(Task):
    def describe(self, id):
        alertfilter = self.datastore.get_by_id('alert-filters', id)
        return 'Deleting alert filter {0}'.format(alertfilter['name'])

    def verify(self, id):

        alertfilter = self.datastore.get_by_id('alert-filters', id)
        if alertfilter is None:
            raise VerifyException(
                errno.ENOENT,
                'Alert filter with ID {0} does not exist'.format(id)
            )

        return []

    def run(self, id):
        try:
            self.datastore.delete('alert-filters', id)
        except DatastoreException as e:
            raise TaskException(
                errno.EBADMSG,
                'Cannot delete alert filter: {0}'.format(str(e))
            )

        self.dispatcher.dispatch_event('alert.filter.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description("Updates the specified Alert Filter")
@accepts(str, h.ref('alert-filter'))
class AlertFilterUpdateTask(Task):
    def describe(self, id, alertfilter):
        alertfilter = self.datastore.get_by_id('alert-filters', id)
        return 'Updating alert filter {0}'.format(alertfilter['name'])

    def verify(self, id, updated_fields):
        return []

    def run(self, id, updated_fields):
        try:
            alertfilter = self.datastore.get_by_id('alert-filters', id)
            alertfilter.update(updated_fields)
            self.datastore.update('alert-filters', id, alertfilter)
        except DatastoreException as e:
            raise TaskException(
                errno.EBADMSG,
                'Cannot update alert filter: {0}'.format(str(e))
            )

        self.dispatcher.dispatch_event('alert.filter.changed', {
            'operation': 'update',
            'ids': [id],
        })


def _depends():
    return ['MailPlugin']


def _init(dispatcher, plugin):
    plugin.register_schema_definition('alert-severity', {
        'type': 'string',
        'enum': ['CRITICAL', 'WARNING', 'INFO'],
    })

    plugin.register_schema_definition('alert', {
        'type': 'object',
        'properties': {
            'id': {'type': 'integer'},
            'class': {'type': 'string'},
            'type': {'type': 'string'},
            'subtype': {'type': 'string'},
            'target': {'type': 'string'},
            'description': {'type': 'string'},
            'source': {'type': 'string'},
            'when': {'type': 'string'},
            'active': {'type': 'boolean'},
            'dismissed': {'type': 'boolean'},
            'send_count': {'type': 'integer'}
        },
        'additionalProperties': False,
        'required': ['name', 'severity'],
    })

    plugin.register_schema_definition('alert-emitter-email', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['alert-emitter-email']},
            'address': {'type': 'string'}
        }
    })

    plugin.register_schema_definition('alert-filter', {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'emitter': {'type': 'string'},
            'parameters': {
                'discriminator': 'type',
                'oneOf': [
                    'alert-emitter-email'
                ]
            },
            'predicates': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'property': {
                            'type': 'string',
                            'enum': ['class', 'type', 'subtype', 'target', 'description', 'active', 'dismissed']
                        },
                        'operator': {
                            'type': 'string',
                            'enum': ['=', '!=', '<=', '>=', '>', '<', '~']
                        },
                        'value': {'type': ['string', 'integer', 'boolean', 'null']}
                    }
                }
            }
        },
        'additionalProperties': False,
    })

    plugin.register_schema_definition('alert-class', {
        'type': 'object',
        'additionalProperties': {
            'type': 'object',
            'properties': {
                'id': {'type': 'string'},
                'type': {'type': 'string'},
                'subtype': {'type': 'string'},
                'severity': {'$ref': 'alert-severity'}
            },
            'additionalProperties': False,
        }
    })

    # Register providers
    plugin.register_provider('alert', AlertsProvider)
    plugin.register_provider('alert.filter', AlertsFiltersProvider)

    # Register task handlers
    plugin.register_task_handler('alert.filter.create', AlertFilterCreateTask)
    plugin.register_task_handler('alert.filter.delete', AlertFilterDeleteTask)
    plugin.register_task_handler('alert.filter.update', AlertFilterUpdateTask)

    # Register event types
    plugin.register_event_type(
        'alert.changed',
        schema={
            'type': 'object',
            'properties': {
                'operation': {'type': 'string', 'enum': ['create', 'delete']},
                'ids': {'type': 'array', 'items': 'integer'},
            },
            'additionalProperties': False
        }
    )
    plugin.register_event_type(
        'alert.filter.changed',
        schema={
            'type': 'object',
            'properties': {
                'operation': {'type': 'string', 'enum': ['create', 'delete', 'update']},
                'ids': {'type': 'array', 'items': 'string'},
            },
            'additionalProperties': False
        }
    )
