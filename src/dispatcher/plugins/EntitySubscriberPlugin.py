#+
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


import re
import gevent
from event import EventSource, sync


class EntitySubscriberEventSource(EventSource):
    def __init__(self, dispatcher):
        super(EntitySubscriberEventSource, self).__init__(dispatcher)
        self.handles = {}
        self.services = []
        dispatcher.register_event_handler('server.event.added', self.event_added)
        dispatcher.register_event_handler('server.event.removed', self.event_removed)

    def event_added(self, args):
        if args['name'].startswith('entity-subscriber'):
            return

        service, _, changed = args['name'].rpartition('.')
        if changed == 'changed':
            self.register(service)

    def event_removed(self, args):
        if args['name'].startswith('entity-subscriber'):
            return

        service, _, changed = args['name'].rpartition('.')
        if changed == 'changed':
            self.services.remove(service)

    def changed(self, service, event):
        ids = event.get('ids', None)
        operation = event['operation']

        if ids is None and operation != 'update':
            self.logger.warn('Bogus event {0}: no ids and operation is {1}'.format(event, operation))
            return

        if operation in ('delete', 'rename'):
            self.dispatcher.dispatch_event('entity-subscriber.{0}.changed'.format(service), {
                'service': service,
                'operation': operation,
                'ids': ids
            })
        else:
            gevent.spawn(self.fetch if ids is not None else self.fetch_one, service, operation, ids)

    def fetch(self, service, operation, ids):
        try:
            keys = list(ids.keys()) if isinstance(ids, dict) else ids
            entities = list(self.dispatcher.call_sync('{0}.query'.format(service), [('id', 'in', keys)]))
        except BaseException as e:
            self.logger.warn('Cannot fetch changed entities from service {0}: {1}'.format(service, str(e)))
            return

        self.dispatcher.dispatch_event('entity-subscriber.{0}.changed'.format(service), {
            'service': service,
            'operation': operation,
            'ids': ids,
            'entities': entities,
            'nolog': True
        })

    def fetch_one(self, service, operation, ids):
        assert operation == 'update'
        assert ids is None

        entity = self.dispatcher.call_sync('{0}.get_config'.format(service))
        self.dispatcher.dispatch_event('entity-subscriber.{0}.changed'.format(service), {
            'service': service,
            'operation': operation,
            'data': entity,
            'nolog': True
        })

    def enable(self, event):
        service = re.match(r'^entity-subscriber\.([\.\w]+)\.changed$', event).group(1)
        self.handles[service] = self.dispatcher.register_event_handler(
            '{0}.changed'.format(service),
            sync(lambda e: self.changed(service, e)))

    def disable(self, event):
        service = re.match(r'^entity-subscriber\.([\.\w]+)\.changed$', event).group(1)
        self.dispatcher.unregister_event_handler('{0}.changed'.format(service), self.handles[service])

    def register(self, service):
        self.dispatcher.register_event_type('entity-subscriber.{0}.changed'.format(service), self)
        self.logger.info('Registered subscriber for service {0}'.format(service))
        self.services.append(service)

    def run(self):
        # Scan through registered events for those ending with .changed
        for i in list(self.dispatcher.event_types.keys()):
            service, _, changed = i.rpartition('.')
            if changed == 'changed':
                self.register(service)


def _init(dispatcher, plugin):
    plugin.register_event_source('entity-subscriber', EntitySubscriberEventSource)
