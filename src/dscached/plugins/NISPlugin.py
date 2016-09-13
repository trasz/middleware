#
# Copyright 2016 Edward Tomasz Napierala <trasz@FreeBSD.org>
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

import logging
import threading
import time
import uuid
from bsd import yp
from plugin import DirectoryServicePlugin, DirectoryState
from freenas.dispatcher.jsonenc import load, dump
from freenas.utils import first_or_default, crypted_password, nt_password
from freenas.utils.query import query


NIS_USER_UUID = uuid.UUID('629C9D65-1351-4077-956B-C51E5DB1CFFD')
NIS_GROUP_UUID = uuid.UUID('D1DA4923-DE91-4C94-9D2B-FD7D4B5ADA1E')
logger = logging.getLogger(__name__)


class NISPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.monitor_thread = None

    # Note that the whole purpose of this is to check whether the NIS server
    # is reachable; NIS client routines handle the binding all by themselves.
    def __monitor(self):
        while True:
            try:
                yp.ypbind(self.nis_domain)
                yp.ypunbind(self.nis_domain)
                self.directory.put_state(DirectoryState.BOUND)
            except (OSError) as err:
                logger.warn('Cannot bind to NIS domain {0}: {1}'.format(self.nis_domain, str(err)))
                self.directory.put_state(DirectoryState.JOINING)
            time.sleep(60)

    # XXX: Shouldn't be needed.
    @staticmethod
    def normalize_parameters(parameters):
        return parameters

    def convert_user(self, entry):
        if not entry:
            return

        username, encrypwd, uid, gid, gecos, homedir, usershell = entry.split(':')

        group_id = None
        try:
            group_id = self.getgrgid(gid)['id']
        except OSError:
            pass

        # XXX: We might want to also include domain hash in the UUID.
        return {
            'id': str(uuid.uuid5(NIS_USER_UUID, str(uid))),
            'uid': uid,
            'gid': gid,
            'builtin': False,
            'username': username,
            'full_name': gecos,
            'email': None,
            'locked': False,
            'sudo': False,
            'groups': [],
            'group': group_id,
            'shell': usershell,
            'home': homedir,
            'nthash': None,
            'password_changed_at': None
        }

    def convert_group(self, entry):
        if not entry:
            return

        groupname, asterisk, gid, userlist = entry.split(':')

        # XXX: We might want to also include domain hash in the UUID.
        return {
            'id': str(uuid.uuid5(NIS_GROUP_UUID, str(gid))),
            'gid': gid,
            'builtin': False,
            'name': groupname,
            'aliases': None,
            'sudo': False
        }

    def getpwent(self, filter=None, params=None):
        logger.debug('getpwent(filter={0}, params={1})'.format(filter, params))
        filter = filter or []
        filter.append(('uid', '!=', 0))
        return query([self.convert_user(i) for i in yp.ypcat("passwd.byname", self.nis_domain)], *filter, **(params or {}))

    def getpwnam(self, name):
        logger.debug('getpwnam(name={0})'.format(name))
        user = self.convert_user(yp.ypmatch(name, "passwd.byname", self.nis_domain))

        # Try to fill in the unixhash field; authenticate() uses it.
        # Note that this will throw an exception when you're not root;
        # it's because NIS won't respond to this unless it comes from
        # a low TCP/IP port.
        #
        # XXX: Should we do this in all cases, even though it's not
        #      actually needed there?
        user['unixhash'] = yp.ypmatch(name, "shadow.byname", self.nis_domain).split(':')[1]
        return user

    def getpwuid(self, uid):
        logger.debug('getpwuid(uid={0})'.format(uid))
        return self.convert_user(yp.ypmatch(uid, "passwd.byuid", self.nis_domain))

    def getpwuuid(self, uuid):
        logger.debug('getpwuuid(uid={0})'.format(uuid))
        filter = []
        filter.append('id', '=', uuid)
        return self.getpwent(filter)

    def getgrent(self, filter=None, params=None):
        logger.debug('getgrent(filter={0}, params={1})'.format(filter, params))
        filter = filter or []
        filter.append(('gid', '!=', 0))
        return query([self.convert_group(i) for i in yp.ypcat("group.byname", self.nis_domain)], *filter, **(params or {}))

    def getgrnam(self, name):
        logger.debug('getgrnam(name={0})'.format(name))
        return self.convert_group(yp.ypmatch(name, "group.byname", self.nis_domain))

    def getgrgid(self, gid):
        logger.debug('getgrgid(gid={0})'.format(gid))
        return self.convert_group(yp.ypmatch(gid, "group.bygid", self.nis_domain))

    def getgruuid(self, uuid):
        logger.debug('getgruuid(uid={0})'.format(uuid))
        filter = []
        filter.append('id', '=', uuid)
        return self.getgrent(filter)

    def change_password(self, username, old_password, password):
        yp.yppasswd(username, old_password, password, self.nis_domain)

    def configure(self, enable, directory):
        self.directory = directory
        self.nis_domain = directory.parameters['nis_domain']
        directory.put_state(DirectoryState.JOINING)
        if not self.monitor_thread:
            self.monitor_thread = threading.Thread(target=self.__monitor, daemon=True)
            self.monitor_thread.start()
        directory.put_state(DirectoryState.BOUND)

def _init(context):
    context.register_plugin('nis', NISPlugin)

    context.register_schema('nis-directory-params', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['nis-directory-params']},
            'nis_domain': {'type': ['string', 'null']}
        }
    })

    context.register_schema('nis-directory-status', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'enum': ['nis-directory-status']},
        }
    })
