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
import wbclient
from plugin import DirectoryServicePlugin
from freenas.utils.query import wrap


class WinbindPlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context
        self.wbc = wbclient.Context()
        self.domain_name = self.wbc.interface.netbios_domain

    def get_directory_info(self):
        return {
            'domain_name': self.domain_name
        }

    def convert_user(self, user):
        if not user:
            return

        return {
            'id': str(uuid.uuid4()),  # XXX this is wrong - id should be mapped to SID
            'uid': user.passwd.pw_uid,
            'builtin': False,
            'username': user.passwd.pw_name,
            'full_name': user.passwd.pw_gecos,
            'email': None,
            'locked': False,
            'sudo': False,
            'password_disabled': False,
            'shell': user.passwd.pw_shell,
            'home': user.passwd.pw_dir
        }

    def convert_group(self, group):
        return {
            'gid': group.group.gr_gid,
            'builtin': False,
            'name': group.group.gr_name,
            'sudo': False
        }

    def getpwent(self, filter=None, params=None):
        return wrap(self.convert_user(i) for i in self.wbc.query_users(self.domain_name)).query(
            *(filter or []),
            **(params or {})
        )

    def getpwuid(self, uid):
        return self.convert_user(self.wbc.get_user(uid=uid))

    def getpwnam(self, name):
        return self.convert_user(self.wbc.get_user(name=name))

    def getgrent(self, filter=None, params=None):
        return wrap(self.convert_group(i) for i in self.wbc.query_groups(self.domain_name)).query(
            *(filter or []),
            **(params or {})
        )

    def getgrnam(self, name):
        return self.convert_group(self.wbc.get_group(name=name))

    def getgrgid(self, gid):
        return self.convert_group(self.wbc.get_group(gid=gid))


def _init(context):
    context.register_plugin('activedirectory', WinbindPlugin)
