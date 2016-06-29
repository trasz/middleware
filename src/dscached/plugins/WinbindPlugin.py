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
import krb5
import smbconf
import wbclient
import logging
import subprocess
import errno
from threading import Thread, Condition
from datetime import datetime
from plugin import DirectoryServicePlugin, DirectoryState, params, status
from utils import obtain_or_renew_ticket
from freenas.dispatcher.rpc import SchemaHelper as h
from freenas.utils import normalize
from freenas.utils.query import wrap


AD_REALM_ID = uuid.UUID('01a35b82-0168-11e6-88d6-0cc47a3511b4')
WINBIND_ID_BASE = uuid.UUID('e6ae958e-fdaa-44a1-8905-0a0c8d015245')
WINBINDD_PIDFILE = '/var/run/samba4/winbindd.pid'
WINBINDD_KEEPALIVE = 60
logger = logging.getLogger(__name__)


def yesno(val):
    return 'yes' if val else 'no'


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
        self.cv = Condition()
        self.bind_thread = Thread(target=self.bind, daemon=True)
        self.bind_thread.start()

    @property
    def realm(self):
        return self.parameters['realm']

    @property
    def wbc(self):
        return wbclient.Context()

    @property
    def principal(self):
        return '{0}@{1}'.format(self.parameters['username'], self.parameters['realm'].upper())

    @property
    def domain_users_sid(self):
        return '{0}-{1}'.format(self.domain_info.sid, 513)

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

    def __joined(self):
        return self.wbc.interface is not None

    def __renew_ticket(self):
        obtain_or_renew_ticket(self.principal, self.parameters['password'])

    def bind(self):
        logger.debug('Bind thread: starting')
        while True:
            with self.cv:
                notify = self.cv.wait(60)

                if notify:
                    pass

                if self.enabled:
                    try:
                        self.dc = self.wbc.ping_dc(self.realm)
                        self.domain_info = self.wbc.get_domain_info(self.realm)
                        self.domain_name = self.wbc.interface.netbios_domain
                        self.directory.put_state(DirectoryState.BOUND)
                    except wbclient.WinbindException as err:
                        # Try to rejoin
                        self.directory.put_state(DirectoryState.JOINING)
                        logger.warning('Cannot ping DC for {0}: {1}'.format(self.realm, str(err)))
                        logger.debug('Keepalive thread: rejoining')
                        self.join()
                else:
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
            'winbind enum users': 'yes',
            'winbind enum groups': 'yes',
            'winbind nested groups': 'yes',
            'winbind use default domain': 'no',
            'winbind refresh tickets': 'no',
            'idmap config *:backend': 'tdb',
            'idmap config *:range': '0-65536',
            'idmap config {0}:backend'.format(workgroup): 'rid',
            'idmap config {0}:range'.format(workgroup):
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

    def generate_id(self, sid):
        sid = str(sid).split('-')
        fields = list(WINBIND_ID_BASE.fields)
        fields[-1] = int(sid[-1])
        return str(uuid.UUID(fields=fields))

    def uuid_to_sid(self, id):
        u = uuid.UUID(id)
        return wbclient.SID('{0}-{1}'.format(str(self.domain_info.sid), u.node))

    def convert_user(self, user):
        if not user:
            return

        domain, username = user.passwd.pw_name.split('\\')

        return {
            'id': self.generate_id(user.sid),
            'sid': user.sid,
            'uid': user.passwd.pw_uid,
            'builtin': False,
            'username': username,
            'aliases': [user.passwd.pw_name],
            'full_name': user.passwd.pw_gecos,
            'email': None,
            'locked': False,
            'sudo': False,
            'password_disabled': False,
            'group': self.generate_id(self.domain_users_sid),
            'groups': [self.generate_id(sid) for sid in user.groups],
            'shell': user.passwd.pw_shell,
            'home': user.passwd.pw_dir
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
        if not self.__joined():
            logger.debug('getpwent: not joined')
            return []

        return wrap([self.convert_user(i) for i in self.wbc.query_users(self.domain_name)]).query(
            *(filter or []),
            **(params or {})
        )

    def getpwuid(self, uid):
        logger.debug('getpwuid(uid={0})'.format(uid))
        if not self.__joined():
            logger.debug('getpwuid: not joined')
            return

        return self.convert_user(self.wbc.get_user(uid=uid))

    def getpwuuid(self, id):
        logger.debug('getpwuuid(uuid={0})'.format(id))
        if not self.__joined():
            logger.debug('getpwuuid: not joined')
            return

        sid = self.uuid_to_sid(uuid)
        return self.convert_user(self.wbc.get_user(sid=sid))

    def getpwnam(self, name):
        logger.debug('getpwnam(name={0})'.format(name))

        if '\\' not in name:
            name = '{0}\\{1}'.format(self.realm, name)

        if not self.__joined():
            logger.debug('getpwnam: not joined')
            return

        return self.convert_user(self.wbc.get_user(name=name))

    def getgrent(self, filter=None, params=None):
        logger.debug('getgrent(filter={0}, params={1})'.format(filter, params))
        if not self.__joined():
            logger.debug('getgrent: not joined')
            return []

        return wrap(self.convert_group(i) for i in self.wbc.query_groups(self.domain_name)).query(
            *(filter or []),
            **(params or {})
        )

    def getgrnam(self, name):
        logger.debug('getgrnam(name={0})'.format(name))

        if '\\' not in name:
            name = '{0}\\{1}'.format(self.realm, name)

        if not self.__joined():
            logger.debug('getgrnam: not joined')
            return

        return self.convert_group(self.wbc.get_group(name=name))

    def getgruuid(self, id):
        logger.debug('getgruuid(uuid={0})'.format(id))
        if not self.__joined():
            logger.debug('getgruuid: not joined')
            return

        sid = self.uuid_to_sid(id)
        logger.debug('getgruuid: SID={0}'.format(sid))
        return self.convert_group(self.wbc.get_group(sid=sid))

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))
        if not self.__joined():
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
            subprocess.call(['/usr/local/bin/net', 'ads', 'join', '-k', self.parameters['realm']])
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
