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

from plugin import DirectoryServicePlugin


class LocalDatabasePlugin(DirectoryServicePlugin):
    def __init__(self, context, parameters):
        self.context = context
        self.datastore = context.datastore

    def getpwent(self, filter=None, params=None):
        filter = filter or []
        return self.datastore.query('users', *filter, **(params or {}))

    def getpwnam(self, name):
        user = self.datastore.get_one('users', ('username', '=', name))
        if not user:
            return None

        return user

    def getpwuid(self, uid):
        user = self.datastore.get_one('users', ('uid', '=', uid))
        if not user:
            return None

        return user

    def getgrent(self, filter=None, params=None):
        filter = filter or []
        return self.datastore.query('groups', *filter, **(params or {}))

    def getgrnam(self, name):
        group = self.datastore.get_one('groups', ('username', '=', name))
        if not group:
            return None

        return group

    def getgrgid(self, gid):
        group = self.datastore.get_one('groups', ('uid', '=', gid))
        if not group:
            return None

        return group


def _init(context):
    context.register_plugin('local', LocalDatabasePlugin)
