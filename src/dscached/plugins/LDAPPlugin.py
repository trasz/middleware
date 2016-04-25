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
from plugin import DirectoryServicePlugin


logger = logging.getLogger(__name__)


def dn_to_domain(dn):
    return '.'.join(name for typ, name, sep in ldap3.utils.dn.parse_dn(dn))


class LDAPPlugin(DirectoryServicePlugin):
    def __init__(self, context, parameters):
        self.context = context
        self.parameters = None
        self.connection = None

    def convert_user(self, entry):
        attrs = entry.entry_get_attribute_names()
        return {
            'id': str(uuid.uuid4()),  # XXX
            'uid': int(entry.uidNumber.value),
            'builtin': False,
            'username': entry.uid.value,
            'full_name': entry.gecos.value,
            'shell': entry.loginShell.value,
            'home': entry.homeDirectory.value,
            'groups': []
        }

    def getpwent(self, filter=None, params=None):
        logger.debug('getpwent(filter={0}, params={0})'.format(filter, params))
        self.connection.search(','.join([
            self.parameters['user_suffix'],
            self.parameters['base_dn']
        ]), '(objectclass=person)', attributes=ldap3.ALL_ATTRIBUTES)

        return (self.convert_user(i) for i in self.connection.entries)

    def getpwnam(self, name):
        pass

    def getpwuid(self, name):
        pass

    def getgrent(self, filter=None, params=None):
        return []

    def configure(self, enable, uid_min, uid_max, gid_min, gid_max, parameters):
        self.parameters = parameters
        server = ldap3.Server(self.parameters['server'])
        self.connection = ldap3.Connection(server)
        self.connection.bind()

        return dn_to_domain(parameters['base_dn'])


def _init(context):
    context.register_plugin('ldap', LDAPPlugin)

    context.register_schema('ldap-directory-params', {
        'type': {'enum': ['ldap-directory-params']},
        'server': {'type': 'string'},
        'base_dn': {'type': 'string'},
        'bind_dn': {'type': 'string'},
        'password': {'type': 'string'},
        'user_suffix': {'type': 'string'},
        'group_suffix': {'type': 'string'}
    })

    context.register_schema('ldap-directory-status', {

    })
