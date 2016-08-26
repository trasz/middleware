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

import errno
import time
from task import Provider, query
from auth import Token
from freenas.dispatcher.rpc import RpcException, description, pass_sender, returns, accepts, generator, SchemaHelper as h
from freenas.utils import first_or_default


@description("Provides Information about the current loggedin Session")
class SessionProvider(Provider):
    @query('session')
    @generator
    def query(self, filter=None, params=None):
        return self.datastore.query_stream('sessions', *(filter or []), **(params or {}))

    @accepts()
    @returns(h.array(h.ref('session')))
    @description("Returns the logged in and active user sessions" +
                 "Does not include the service sessions in this.")
    def get_live_user_sessions(self):
        live_user_session_ids = []
        for srv in self.dispatcher.ws_servers:
            for conn in srv.connections:
                # The if check for 'uid' below is to seperate the actuall gui/cli
                # users of the websocket connection from that of system services
                # like etcd, statd and so on.
                if hasattr(conn.user, 'uid'):
                    live_user_session_ids.append(conn.session_id)
        return self.datastore.query('sessions', ('id', 'in', live_user_session_ids))

    @pass_sender
    @returns(int)
    def get_my_session_id(self, sender):
        return sender.session_id

    @description("Returns the logged in user for the current session")
    @returns(str)
    @pass_sender
    def whoami(self, sender):
        return sender.user.name

    @description("Returns new, valid authentication token")
    @returns(str)
    @pass_sender
    def create_token(self, sender):
        lifetime = self.configstore.get("middleware.token_lifetime")
        return self.dispatcher.token_store.issue_token(
            Token(
                user=sender.user,
                lifetime=lifetime,
                revocation_function=sender.logout
            )
        )

    @description("Sends a message to given session")
    @accepts(int, str)
    @pass_sender
    def send_to_session(self, id, message, sender):
        target = None
        for srv in self.dispatcher.ws_servers:
            target = first_or_default(lambda s: s.session_id == id, srv)
            if target:
                break

        if not target:
            raise RpcException(errno.ENOENT, 'Session {0} not found'.format(id))

        target.outgoing_events.put(('session.message', {
            'sender_id': sender.session_id,
            'sender_name': sender.user.name if sender.user else None,
            'message': message
        }))



def _init(dispatcher, plugin):
    def pam_event(args):
        if args['type'] == 'open_session':
            dispatcher.datastore.insert('sessions', {
                'username': args['username'],
                'resource': args['service'],
                'tty': args['tty'],
                'active': True,
                'started-at': time.time(),
                'ended-at': None
            })

        if args['type'] == 'close_session':
            session = dispatcher.datastore.get_one(
                'sessions',
                ('username', '=', args['username']),
                ('resource', '=', args['service']),
                ('tty', '=', args['tty']),
                ('active', '=', True),
                ('ended-at', '=', None)
            )

            session['ended-at'] = time.time()
            session['active'] = False
            dispatcher.datastore.update('session', session['id'], session)

    plugin.register_schema_definition('session', {
        'type': 'object',
        'properties': {
            'username': {'type': 'string'},
            'resource': {'type': ['string', 'null']},
            'tty': {'type': ['string', 'null']},
            'active': {'type': 'boolean'},
            'started-at': {'type': 'integer'},
            'ended-at': {'type': 'integer'}
        }
    })

    plugin.register_provider('session', SessionProvider)
    plugin.register_event_type('session.changed')
    plugin.register_event_type('session.message')
    plugin.register_event_handler('system.pam.event', pam_event)
