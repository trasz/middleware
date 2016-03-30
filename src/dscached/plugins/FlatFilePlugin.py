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

import json
import logging
from plugin import DirectoryServicePlugin
from freenas.utils.query import wrap


PASSWD_FILE = '/etc/passwd.json'
GROUP_FILE = '/etc/group.json'
logger = logging.getLogger(__name__)


class FlatFilePlugin(DirectoryServicePlugin):
    def __init__(self, context):
        self.context = context

        try:
            with open(PASSWD_FILE, 'r') as f:
                self.passwd = wrap(json.load(f))
        except (IOError, ValueError) as err:
            logger.warn('Cannot read {0}: {1}'.format(PASSWD_FILE, str(err)))
            self.passwd = wrap([])

        try:
            with open(GROUP_FILE, 'r') as f:
                self.group = wrap(json.load(f))
        except (IOError, ValueError) as err:
            logger.warn('Cannot read {0}: {1}'.format(GROUP_FILE, str(err)))
            self.group = wrap([])

    def getpwent(self, filter=None, params=None):
        return self.passwd.query(*(filter or []), **(params or {}))

    def getpwnam(self, name):
        return self.passwd.query(('username', '=', name), single=True)

    def getpwuid(self, uid):
        return self.passwd.query(('uid', '=', uid), single=True)

    def getgrent(self, filter=None, params=None):
        return self.group.query(*(filter or []), **(params or {}))

    def getgrnam(self, name):
        return self.group.query(('name', '=', name), single=True)

    def getgrgid(self, gid):
        return self.group.query(('gid', '=', gid), single=True)


def _init(context):
    context.register_plugin('local', FlatFilePlugin)
