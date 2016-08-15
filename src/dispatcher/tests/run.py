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
######################################################################

import sys
import argparse
import nose2
import nose2.events
from paramiko import AutoAddPolicy
from paramiko.client import SSHClient


class Context(object):
    def __init__(self, hostname, username, password, xml=False):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.xml = xml
        self.ssh_client = SSHClient()
        self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        self.ssh_client.connect(self.hostname, username=self.username, password=self.password)


class ContextInjector(object):
    def __init__(self, context):
        self.context = context

    def startTest(self, event):
        event.test.context = self.context


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', metavar='HOST')
    parser.add_argument('-u', metavar='USER', default='root')
    parser.add_argument('-p', metavar='PASSWORD')
    parser.add_argument('-x', action='store_true')
    args, rest = parser.parse_known_args()

    ctx = Context(args.a, args.u, args.p, args.x)
    nose2.discover(argv=sys.argv[:1], extraHooks=[('startTest', ContextInjector(ctx))])


if __name__ == '__main__':
    main()
