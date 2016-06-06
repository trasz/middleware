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
from contextlib import contextmanager
from dns import resolver
from datetime import datetime
from plugin import DirectoryServicePlugin
from utils import obtain_or_renew_ticket, join_dn, dn_to_domain, domain_to_dn
from freenas.utils import normalize, first_or_default
from freenas.utils.query import wrap


FREEIPA_REALM_ID = uuid.UUID('e44553e1-0c0b-11e6-9898-000c2957240a')
TICKET_RENEW_LIFE = 30 * 86400  # 30 days
logger = logging.getLogger(__name__)


def get_ldap_address(realm):
    pass


class FreeIPAPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.parameters = None
        self.server = None
        self.conn = None
        self.base_dn = None
        self.user_dn = None
        self.group_dn = None
        self.principal = None

    def search(self, search_base, search_filter, attributes=None):
        if self.conn.closed:
            self.conn.bind()

        id = self.conn.search(search_base, search_filter, attributes=attributes or ldap3.ALL_ATTRIBUTES)
        result, status = self.conn.get_response(id)
        return result

    def search_one(self, *args, **kwargs):
        return first_or_default(None, self.search(*args, **kwargs))

    @staticmethod
    def normalize_parameters(parameters):
        return normalize(parameters, {
            'type': 'freeipa-directory-params',
            'realm': '',
            'server': None,
            'username': '',
            'password': '',
            'user_suffix': 'cn=users,cn=accounts',
            'group_suffix': 'cn=groups,cn=accounts'
        })

    def convert_user(self, entry):
        entry = wrap(dict(entry['attributes']))
        group = None

        if 'gidNumber.0' in entry:
            group = self.search_one(self.group_dn, '(gidNumber={0})'.format(entry['gidNumber.0']))
            group = wrap(dict(group['attributes']))

        return {
            'id': entry['ipaUniqueID.0'],
            'uid': int(entry['uidNumber.0']),
            'builtin': False,
            'username': entry['uid.0'],
            'full_name': entry.get('gecos.0', entry.get('displayName.0', '<unknown>')),
            'shell': entry.get('loginShell.0', '/bin/sh'),
            'home': entry.get('homeDirectory.0', '/nonexistent'),
            'group': group['ipaUniqueID.0'] if group else None,
            'groups': [],
            'sudo': False
        }

    def convert_group(self, entry):
        entry = wrap(dict(entry['attributes']))
        return {
            'id': entry['ipaUniqueID.0'],
            'gid': int(entry['gidNumber.0']),
            'name': entry['cn.0'],
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

    def getpwid(self, uuid):
        pass

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

    def getgrid(self, uuid):
        pass

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))
        group = self.search_one(self.group_dn, '(gidNumber={0})'.format(gid))
        return self.convert_group(group)

    def configure(self, enable, uid_min, uid_max, gid_min, gid_max, parameters):
        self.parameters = parameters
        self.base_dn = domain_to_dn(parameters['realm'])
        self.user_dn = ','.join([parameters['user_suffix'], self.base_dn])
        self.group_dn = ','.join([parameters['group_suffix'], self.base_dn])
        self.principal = '{0}@{1}'.format(self.parameters['username'], self.parameters['realm'].upper())

        obtain_or_renew_ticket(self.principal, self.parameters['password'], renew_life=TICKET_RENEW_LIFE)
        self.server = ldap3.Server(self.parameters['server'])
        self.conn = ldap3.Connection(self.server, client_strategy='ASYNC', authentication=ldap3.SASL, sasl_mechanism='GSSAPI')
        self.conn.bind()

        return parameters['realm']

    def authenticate(self, user, password):
        logger.debug('authenticate(user={0}, password=<...>)'.format(user))
        try:
            self.conn.rebind(join_dn(user, self.user_dn), password)
            return True
        except:
            return False

    def get_kerberos_realm(self):
        return {
            'id': FREEIPA_REALM_ID,
            'realm': self.parameters['realm'].upper(),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }


def _init(context):
    context.register_plugin('freeipa', FreeIPAPlugin)

    context.register_schema('freeipa-directory-params', {
        'type': {'enum': ['ldap-directory-params']},
        'realm': {'type': 'string'},
        'server': {'type': ['string', 'null']},
        'username': {'type': 'string'},
        'password': {'type': 'string'},
        'user_suffix': {'type': 'string'},
        'group_suffix': {'type': 'string'},
        'encryption': {
            'type': 'string',
            'enum': ['NONE', 'SSL', 'TLS']
        },
        'certificate': {'type': ['string', 'null']},
    })

    context.register_schema('freeipa-directory-status', {

    })
