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

import os
import uuid
import krb5
import ldap3
import smbconf
import wbclient
import logging
import subprocess
import errno
import struct
import contextlib
from threading import Thread, Condition
from datetime import datetime
from plugin import DirectoryServicePlugin, DirectoryState, params, status
from utils import domain_to_dn, join_dn, obtain_or_renew_ticket, have_ticket, get_srv_records, LdapQueryBuilder
from freenas.dispatcher.rpc import SchemaHelper as h
from freenas.utils import normalize, first_or_default
from freenas.utils.query import get


AD_REALM_ID = uuid.UUID('01a35b82-0168-11e6-88d6-0cc47a3511b4')
WINBIND_ID_BASE = uuid.UUID('e6ae958e-fdaa-44a1-8905-0a0c8d015245')
WINBINDD_PIDFILE = '/var/run/samba4/winbindd.pid'
WINBINDD_KEEPALIVE = 60
logger = logging.getLogger(__name__)


def yesno(val):
    return 'yes' if val else 'no'


def convert_sid(binary):
    version, length = struct.unpack('BB', binary[0:2])
    authority, = struct.unpack('>Q', b'\x00\x00' + binary[2:8])
    string = 'S-%d-%d' % (version, authority)
    binary = binary[8:]
    assert len(binary) == 4 * length
    for i in range(length):
        value, = struct.unpack('<L', binary[4 * i:4 * (i + 1)])
        string += '-%d' % (value)

    return string


@params(h.ref('winbind-directory-params'))
@status(h.ref('winbind-directory-status'))
class WinbindPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.uid_min = None
        self.uid_max = None
        self.dc = None
        self.enabled = False
        self.domain_info = None
        self.domain_name = None
        self.parameters = None
        self.directory = None
        self.ldap_servers = None
        self.ldap = None
        self.cv = Condition()
        self.bind_thread = Thread(target=self.bind, daemon=True)
        self.bind_thread.start()

        # Remove winbind cache files
        with contextlib.suppress(FileNotFoundError):
            os.remove('/var/db/samba4/winbindd_cache.tdb')

        with contextlib.suppress(FileNotFoundError):
            os.remove('/var/db/samba4/winbindd_cache.tdb.bak')

        with contextlib.suppress(FileNotFoundError):
            os.remove('/var/db/samba4/winbindd_cache.tdb.old')

    @property
    def realm(self):
        return self.parameters['realm']

    @property
    def base_dn(self):
        return join_dn('CN=Users', domain_to_dn(self.realm))

    @property
    def wbc(self):
        return wbclient.Context()

    @property
    def principal(self):
        return '{0}@{1}'.format(self.parameters['username'], self.parameters['realm'].upper())

    @property
    def domain_users_sid(self):
        return '{0}-{1}'.format(self.domain_info.sid, 513)

    @property
    def ldap_addresses(self):
        return [str(i) for i in get_srv_records('ldap', 'tcp', self.parameters['realm'])]

    @staticmethod
    def normalize_parameters(parameters):
        return normalize(parameters, {
            'type': 'winbind-directory-params',
            'realm': '',
            'username': 'Administrator',
            'password': None,
            'keytab': None,
            'site_name': None,
            'dc_address': None,
            'gcs_address': None,
            'allow_dns_updates': True
        })

    def is_joined(self):
        # Check if we have ticket
        if not have_ticket(self.principal):
            return False

        # Check if we can fetch domain SID
        if subprocess.call(['/usr/local/bin/net', 'getdomainsid']) != 0:
            return False

        # Check if winbind is running
        return self.wbc.interface is not None

    def __renew_ticket(self):
        obtain_or_renew_ticket(self.principal, self.parameters['password'])

    def search(self, search_base, search_filter, attributes=None):
        if self.ldap.closed:
            self.ldap.bind()

        return self.ldap.extend.standard.paged_search(
            search_base=search_base,
            search_filter=search_filter,
            search_scope=ldap3.SUBTREE,
            attributes=attributes or ldap3.ALL_ATTRIBUTES,
            paged_size=16,
            generator=True
        )

    def search_one(self, *args, **kwargs):
        return first_or_default(None, self.search(*args, **kwargs))

    def bind(self):
        logger.debug('Bind thread: starting')
        while True:
            with self.cv:
                notify = self.cv.wait(60)

                if notify:
                    pass

                if self.enabled:
                    if not self.is_joined():
                        # Try to rejoin
                        logger.debug('Keepalive thread: rejoining')
                        self.directory.put_state(DirectoryState.JOINING)
                        self.join()
                    else:
                        self.domain_info = self.wbc.get_domain_info(self.realm)
                        self.domain_name = self.wbc.interface.netbios_domain
                        self.directory.put_state(DirectoryState.BOUND)

                    if not self.ldap:
                        logger.debug('Initializing LDAP connection')
                        self.ldap_servers = [ldap3.Server(i) for i in self.ldap_addresses]
                        self.ldap = ldap3.Connection(
                            self.ldap_servers,
                            client_strategy='ASYNC',
                            authentication=ldap3.SASL,
                            sasl_mechanism='GSSAPI'
                        )

                        try:
                            self.ldap.bind()
                            logger.debug('LDAP bound')
                        except BaseException as err:
                            self.directory.put_status(errno.ENXIO, str(err))
                            self.directory.put_state(DirectoryState.FAILURE)

                else:
                    self.leave()
                    self.directory.put_state(DirectoryState.DISABLED)

    def configure_smb(self, enable):
        workgroup = self.parameters['realm'].split('.')[0]
        cfg = smbconf.SambaConfig('registry')
        params = {
            'server role': 'member server',
            'local master': 'no',
            'domain master': 'no',
            'preferred master': 'no',
            'domain logons': 'no',
            'workgroup': workgroup,
            'realm': self.parameters['realm'],
            'security': 'ads',
            'winbind cache time': str(self.context.cache_ttl),
            'winbind offline logon': 'yes',
            'winbind enum users': 'no',
            'winbind enum groups': 'no',
            'winbind nested groups': 'yes',
            'winbind use default domain': 'no',
            'winbind refresh tickets': 'no',
            'idmap config *: backend': 'tdb',
            'idmap config *: range': '0-65536',
            'idmap config {0}: backend'.format(workgroup): 'rid',
            'idmap config {0}: range'.format(workgroup):
                '{0}-{1}'.format(self.uid_min or 90000001, self.uid_max or 100000000),
            'client use spnego': 'yes',
            'allow trusted domains': 'no',
            'client ldap sasl wrapping': 'plain',
            'template shell': '/bin/sh',
            'template homedir': '/home/%U'
        }

        if enable:
            for k, v in params.items():
                logger.debug('Setting samba parameter "{0}" to "{1}"'.format(k, v))
                cfg[k] = v
        else:
            for k in params:
                del cfg[k]

            params = {
                'server role': 'auto',
                'workgroup': self.context.configstore.get('service.smb.workgroup'),
                'local master': yesno(self.context.configstore.get('service.smb.local_master'))
            }

            for k, v in params.items():
                logger.debug('Setting samba parameter "{0}" to "{1}"'.format(k, v))
                cfg[k] = v

        #self.context.client.call_sync('service.reload', 'smb', 'reload')
        subprocess.call(['/usr/sbin/service', 'samba_server', 'restart'])

    def get_directory_info(self):
        return {
            'domain_name': self.domain_name,
            'domain_controller': self.dc
        }

    def convert_user(self, entry):
        if not entry:
            return

        entry = dict(entry['attributes'])
        username = get(entry, 'sAMAccountName.0')
        logging.debug('entry={0}'.format(entry))
        wbu = self.wbc.get_user(name='{0}\\{1}'.format(self.realm, username))

        if not wbu:
            logging.warning('User {0} found in LDAP, but not in winbindd.'.format(username))
            return

        return {
            'id': str(uuid.UUID(bytes=get(entry, 'objectGUID.0'))),
            'sid': convert_sid(get(entry, 'objectSid.0')),
            'uid': wbu.passwd.pw_uid,
            'builtin': False,
            'username': username,
            'aliases': [wbu.passwd.pw_name],
            'full_name': get(entry, 'name.0'),
            'email': None,
            'locked': False,
            'sudo': False,
            'password_disabled': False,
            'group': str(uuid.uuid4()),
            'groups': [],
            'shell': wbu.passwd.pw_shell,
            'home': wbu.passwd.pw_dir
        }

    def convert_group(self, group):
        domain, groupname = group.group.gr_name.split('\\')
        return {
            'id': self.generate_id(group.sid),
            'sid': group.sid,
            'gid': group.group.gr_gid,
            'builtin': False,
            'name': groupname,
            'aliases': [group.group.gr_name],
            'sudo': False
        }

    def getpwent(self, filter=None, params=None):
        logger.debug('getpwent(filter={0}, params={1})'.format(filter, params))
        if not self.is_joined():
            logger.debug('getpwent: not joined')
            return []

        results = self.search(self.base_dn, '(objectclass=person)')
        return (self.convert_user(i) for i in results)

    def getpwuid(self, uid):
        logger.debug('getpwuid(uid={0})'.format(uid))
        if not self.is_joined():
            logger.debug('getpwuid: not joined')
            return

        return self.convert_user(self.wbc.get_user(uid=uid))

    def getpwuuid(self, id):
        logger.debug('getpwuuid(uuid={0})'.format(id))
        if not self.is_joined():
            logger.debug('getpwuuid: not joined')
            return

        sid = self.uuid_to_sid(uuid)
        return self.convert_user(self.wbc.get_user(sid=sid))

    def getpwnam(self, name):
        logger.debug('getpwnam(name={0})'.format(name))

        if '\\' not in name:
            name = '{0}\\{1}'.format(self.realm, name)

        if not self.is_joined():
            logger.debug('getpwnam: not joined')
            return

        return self.convert_user(self.wbc.get_user(name=name))

    def getgrent(self, filter=None, params=None):
        logger.debug('getgrent(filter={0}, params={1})'.format(filter, params))
        if not self.is_joined():
            logger.debug('getgrent: not joined')
            return []

        return query(
            [self.convert_group(i) for i in self.wbc.query_groups(self.domain_name)],
            *(filter or []),
            **(params or {})
        )

    def getgrnam(self, name):
        logger.debug('getgrnam(name={0})'.format(name))

        if '\\' not in name:
            name = '{0}\\{1}'.format(self.realm, name)

        if not self.is_joined():
            logger.debug('getgrnam: not joined')
            return

        return self.convert_group(self.wbc.get_group(name=name))

    def getgruuid(self, id):
        logger.debug('getgruuid(uuid={0})'.format(id))
        if not self.is_joined():
            logger.debug('getgruuid: not joined')
            return

        sid = self.uuid_to_sid(id)
        logger.debug('getgruuid: SID={0}'.format(sid))
        return self.convert_group(self.wbc.get_group(sid=sid))

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))
        if not self.is_joined():
            logger.debug('getgrgid: not joined')
            return

        return self.convert_group(self.wbc.get_group(gid=gid))

    def configure(self, enable, directory):
        with self.cv:
            self.enabled = enable
            self.directory = directory
            self.uid_min = directory.min_uid
            self.uid_max = directory.max_uid
            self.parameters = directory.parameters
            self.cv.notify_all()

        return self.realm.lower()

    def join(self):
        logger.info('Trying to join to {0}...'.format(self.realm))

        try:
            self.configure_smb(True)
            krb = krb5.Context()
            tgt = krb.obtain_tgt_password(
                self.principal,
                self.parameters['password'],
                renew_life=86400
            )

            ccache = krb5.CredentialsCache(krb)
            ccache.add(tgt)
            subprocess.call(['/usr/local/bin/net', 'ads', 'join', '-k'])

            self.dc = self.wbc.ping_dc(self.realm)
            self.domain_info = self.wbc.get_domain_info(self.realm)
            self.domain_name = self.wbc.interface.netbios_domain
            self.directory.put_state(DirectoryState.BOUND)
        except BaseException as err:
            self.directory.put_status(errno.ENXIO, str(err))
            self.directory.put_state(DirectoryState.FAILURE)
            return

        logger.info('Sucessfully joined to the domain {0}'.format(self.realm))
        self.directory.put_state(DirectoryState.BOUND)

    def leave(self):
        logger.info('Leaving domain {0}'.format(self.realm))
        subprocess.call(['/usr/local/bin/net', 'ads', 'leave', self.parameters['realm']])
        self.configure_smb(False)

    def get_kerberos_realm(self, parameters):
        return {
            'id': AD_REALM_ID,
            'realm': parameters['realm'].upper(),
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }


def _init(context):
    context.register_plugin('winbind', WinbindPlugin)

    context.register_schema('winbind-directory-params', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['winbind-directory-params']},
            'realm': {'type': 'string'},
            'username': {'type': 'string'},
            'password': {'type': ['string', 'null']},
            'keytab': {'type': ['string', 'null']},
            'site_name': {'type': ['string', 'null']},
            'dc_address': {'type': ['string', 'null']},
            'gcs_address': {'type': ['string', 'null']},
            'allow_dns_updates': {'type': 'boolean'}
        }
    })

    context.register_schema('winbind-directory-status', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['winbind-directory-status']},
            'joined': {'type': 'boolean'},
            'domain_controller': {'type': 'string'},
            'server_time': {'type': 'datetime'}
        }
    })
