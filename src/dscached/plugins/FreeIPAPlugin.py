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

import uuid
import ldap3
import ldap3.utils.dn
import logging
import errno
from threading import Thread, Condition
from datetime import datetime
from plugin import DirectoryServicePlugin, DirectoryState
from utils import obtain_or_renew_ticket, join_dn, domain_to_dn, get_srv_records
from freenas.utils import normalize, first_or_default
from freenas.utils.query import get


FREEIPA_REALM_ID = uuid.UUID('e44553e1-0c0b-11e6-9898-000c2957240a')
TICKET_RENEW_LIFE = 30 * 86400  # 30 days
logger = logging.getLogger(__name__)


def get_ldap_address(realm):
    pass


class FreeIPAPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.parameters = None
        self.servers = None
        self.conn = None
        self.base_dn = None
        self.user_dn = None
        self.group_dn = None
        self.principal = None
        self.directory = None
        self.bind_thread = Thread(target=self.bind, daemon=True)
        self.enabled = False
        self.cv = Condition()
        self.bind_thread.start()

    def search(self, search_base, search_filter, attributes=None):
        if self.conn.closed:
            self.conn.bind()

        id = self.conn.search(search_base, search_filter, attributes=attributes or ldap3.ALL_ATTRIBUTES)
        result, status = self.conn.get_response(id)
        return result

    def search_one(self, *args, **kwargs):
        return first_or_default(None, self.search(*args, **kwargs))

    @property
    def ldap_addresses(self):
        if self.parameters['server']:
            return [self.parameters['server']]

        return [str(i) for i in get_srv_records('ldap', 'tcp', self.parameters['realm'])]

    @staticmethod
    def normalize_parameters(parameters):
        return normalize(parameters, {
            'type': 'freeipa-directory-params',
            'realm': '',
            'server': None,
            'kdc': None,
            'username': '',
            'password': '',
            'user_suffix': 'cn=users,cn=accounts',
            'group_suffix': 'cn=groups,cn=accounts'
        })

    def convert_user(self, entry):
        entry = dict(entry['attributes'])
        group = None

        if 'gidNumber.0' in entry:
            group = self.search_one(
                self.group_dn,
                '(gidNumber={0})'.format(get(entry, 'gidNumber.0')),
                attributes='ipaUniqueID'
            )

            group = dict(group['attributes'])

        return {
            'id': get(entry, 'ipaUniqueID.0'),
            'uid': int(get(entry, 'uidNumber.0')),
            'gid': int(get(entry, 'gidNumber.0')),
            'builtin': False,
            'username': get(entry, 'uid.0'),
            'full_name': get(entry, 'gecos.0', get(entry, 'displayName.0', '<unknown>')),
            'shell': get(entry, 'loginShell.0', '/bin/sh'),
            'home': get(entry, 'homeDirectory.0', '/nonexistent'),
            'sshpubkey': get(entry, 'ipaSshPubKey.0', None),
            'group': get(group, 'ipaUniqueID.0') if group else None,
            'groups': [],
            'sudo': False
        }

    def convert_group(self, entry):
        entry = dict(entry['attributes'])
        return {
            'id': get(entry, 'ipaUniqueID.0'),
            'gid': int(get(entry, 'gidNumber.0')),
            'name': get(entry, 'cn.0'),
            'builtin': False,
            'sudo': False
        }

    def getpwent(self, filter=None, params=None):
        logger.debug('getpwent(filter={0}, params={0})'.format(filter, params))
        result = self.search(self.user_dn, '(objectclass=posixAccount)')
        return (self.convert_user(i) for i in result)

    def getpwnam(self, name):
        logger.debug('getpwnam(name={0})'.format(name))
        user = self.search_one(join_dn('uid={0}'.format(name), self.user_dn), '(objectclass=posixAccount)')
        return self.convert_user(user)

    def getpwuuid(self, id):
        logger.debug('getpwuuid(uuid={0})'.format(id))
        user = self.search_one(self.user_dn, '(ipaUniqueID={0})'.format(id))
        return self.convert_user(user)

    def getpwuid(self, uid):
        logger.debug('getpwuid(uid={0})'.format(uid))
        user = self.search_one(self.user_dn, '(uidNumber={0})'.format(uid))
        return self.convert_user(user)

    def getgrent(self, filter=None, params=None):
        logger.debug('getgrent(filter={0}, params={0})'.format(filter, params))
        result = self.search(self.group_dn, '(objectclass=posixGroup)')
        return (self.convert_group(i) for i in result)

    def getgrnam(self, name):
        logger.debug('getgrnam(name={0})'.format(name))
        group = self.search_one(join_dn('cn={0}'.format(name), self.group_dn), '(objectclass=posixGroup)')
        return self.convert_group(group)

    def getgruuid(self, id):
        logger.debug('getgruuid(uuid={0})'.format(id))
        group = self.search_one(self.group_dn, '(ipaUniqueID={0})'.format(id))
        return self.convert_group(group)

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))
        group = self.search_one(self.group_dn, '(gidNumber={0})'.format(gid))
        return self.convert_group(group)

    def configure(self, enable, directory):
        with self.cv:
            self.directory = directory
            self.enabled = enable
            self.parameters = directory.parameters
            self.base_dn = domain_to_dn(self.parameters['realm'])
            self.user_dn = ','.join([self.parameters['user_suffix'], self.base_dn])
            self.group_dn = ','.join([self.parameters['group_suffix'], self.base_dn])
            self.principal = '{0}@{1}'.format(self.parameters['username'], self.parameters['realm'].upper())
            self.cv.notify_all()

        return self.parameters['realm']

    def bind(self):
        while True:
            with self.cv:
                notify = self.cv.wait(60)

                if self.enabled:
                    if self.directory.state == DirectoryState.BOUND and not notify:
                        continue

                    try:
                        self.directory.put_state(DirectoryState.JOINING)
                        obtain_or_renew_ticket(self.principal, self.parameters['password'], renew_life=TICKET_RENEW_LIFE)
                        self.servers = [ldap3.Server(i) for i in self.ldap_addresses]
                        self.conn = ldap3.Connection(
                            self.servers,
                            client_strategy='ASYNC',
                            authentication=ldap3.SASL,
                            sasl_mechanism='GSSAPI'
                        )

                        self.conn.bind()
                        self.directory.put_state(DirectoryState.BOUND)
                        continue
                    except BaseException as err:
                        self.directory.put_status(errno.ENXIO, '{0} <{1}>'.format(str(err), type(err).__name__))
                        self.directory.put_state(DirectoryState.FAILURE)
                        continue
                else:
                    self.conn.unbind()
                    self.directory.put_state(DirectoryState.DISABLED)
                    continue

    def authenticate(self, user, password):
        logger.debug('authenticate(user={0}, password=<...>)'.format(user))
        try:
            return self.conn.rebind(join_dn('uid={0}'.format(user), self.user_dn), password)
        except:
            return False

    def get_kerberos_realm(self, parameters):
        return {
            'id': FREEIPA_REALM_ID,
            'realm': parameters['realm'].upper(),
            'kdc_address': parameters['kdc'],
            'admin_server_address': None,
            'password_server_address': None,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }


def _init(context):
    context.register_plugin('freeipa', FreeIPAPlugin)

    context.register_schema('freeipa-directory-params', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['ldap-directory-params']},
            'realm': {'type': 'string'},
            'server': {'type': ['string', 'null']},
            'kdc': {'type': ['string', 'null']},
            'username': {'type': 'string'},
            'password': {'type': 'string'},
            'user_suffix': {'type': ['string', 'null']},
            'group_suffix': {'type': ['string', 'null']},
            'encryption': {
                'type': 'string',
                'enum': ['NONE', 'SSL', 'TLS']
            },
            'certificate': {'type': ['string', 'null']}
        }
    })

    context.register_schema('freeipa-directory-status', {

    })
