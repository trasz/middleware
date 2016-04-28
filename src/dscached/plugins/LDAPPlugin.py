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
import threading
from plugin import DirectoryServicePlugin
from utils import obtain_or_renew_ticket, join_dn, dn_to_domain, domain_to_dn
from freenas.utils import first_or_default


LDAP_USER_UUID = uuid.UUID('ACA6D9B8-AF83-49D9-9BD7-A5E771CE17EB')
LDAP_GROUP_UUID = uuid.UUID('86657E0F-C5E8-44E0-8896-DD57C78D9766')
logger = logging.getLogger(__name__)


class LDAPPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.server = None
        self.conn = None
        self.parameters = None
        self.base_dn = None
        self.user_dn = None
        self.group_dn = None
        self.bind_lock = threading.RLock()

    def search(self, search_base, search_filter, attributes=None):
        if self.conn.closed:
            with self.bind_lock:
                self.conn.bind()

        id = self.conn.search(search_base, search_filter, attributes=attributes or ldap3.ALL_ATTRIBUTES)
        result, status = self.conn.get_response(id)
        return result

    def search_one(self, *args, **kwargs):
        return first_or_default(None, self.search(*args, **kwargs))

    def get_id(self, entry):
        if 'entryUUID' in entry:
            return entry['entryUUID.0']

        if 'uidNumber' in entry:
            return uuid.uuid5(LDAP_USER_UUID, entry['uidNumber.0'])

        if 'gidNumber' in entry:
            return uuid.uuid5(LDAP_GROUP_UUID, entry['gidNumber.0'])

        return uuid.uuid4()

    def convert_user(self, entry):
        return {
            'id': self.get_id(entry),
            'sid': entry.get('sambaSID.0'),
            'uid': int(entry['uidNumber.0']),
            'builtin': False,
            'username': entry['uid.0'],
            'full_name': entry.get('gecos.0', entry.get('displayName.0', '<unknown>')),
            'shell': entry.get('loginShell.0', '/bin/sh'),
            'home': entry.get('homeDirectory.0', '/nonexistent'),
            'nthash': entry.get('sambaNTPassword.0'),
            'lmhash': entry.get('sambaLMPassword.0'),
            'groups': [],
            'sudo': False
        }

    def convert_group(self, entry):
        return {
            'id': self.get_id(entry),
            'gid': int(entry['gidNumber.0']),
            'sid': entry.get('sambaSID.0'),
            'name': entry['uid.0'],
            'builtin': False,
            'sudo': False
        }

    def getpwent(self, filter=None, params=None):
        logger.debug('getpwent(filter={0}, params={0})'.format(filter, params))
        result = self.search(self.user_dn, '(objectclass=posixAccount)')
        return (self.convert_user(i) for i in result)

    def getpwnam(self, name):
        logger.debug('getpwnam(name={0})'.format(name))
        self.connection.search(','.join([
            'uid={0}'.format(name),
            self.parameters['user_suffix'],
            self.parameters['base_dn']
        ]), '(objectclass=posixAccount)', attributes=ldap3.ALL_ATTRIBUTES)

    def getpwuid(self, uid):
        logger.debug('getpwuid(uid={0})'.format(uid))

    def getgrent(self, filter=None, params=None):
        logger.debug('getgrent(filter={0}, params={0})'.format(filter, params))
        return []

    def getgrnam(self, name):
        logger.debug('getgrnam(name={0})'.format(name))

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))

    def configure(self, enable, uid_min, uid_max, gid_min, gid_max, parameters):
        self.parameters = parameters
        self.server = ldap3.Server(self.parameters['server'])
        self.base_dn = self.parameters['base_dn']
        self.user_dn = join_dn(self.parameters['user_suffix'], self.base_dn)
        self.group_dn = join_dn(self.parameters['group_suffix'], self.base_dn)
        self.conn = ldap3.Connection(
            self.server,
            client_strategy='ASYNC',
            user=self.parameters['bind_dn'],
            password=self.parameters['password']
        )

        self.conn.bind()
        return dn_to_domain(parameters['base_dn'])

    def authenticate(self, user_name, password):
        with self.bind_lock:
            try:
                self.conn.rebind(
                    user=join_dn('uid={0}'.format(user_name), self.user_dn),
                    password=password
                )
            except ldap3.LDAPBindError:
                self.conn.bind()
                return False

            self.conn.bind()
            return True


def _init(context):
    context.register_plugin('ldap', LDAPPlugin)

    context.register_schema('ldap-directory-params', {
        'type': {'enum': ['ldap-directory-params']},
        'server': {'type': 'string'},
        'base_dn': {'type': 'string'},
        'bind_dn': {'type': 'string'},
        'password': {'type': 'string'},
        'user_suffix': {'type': ['string', 'null']},
        'group_suffix': {'type': ['string', 'null']},
        'krb_realm': {'type': ['string', 'null']}
    })

    context.register_schema('ldap-directory-status', {

    })
