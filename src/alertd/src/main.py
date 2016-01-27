#!/usr/local/bin/python3
#
# Copyright 2014-2016 iXsystems, Inc.
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
import sys
import logging
import argparse
import json
import errno
import datastore
import time
import imp
import setproctitle
import renderers
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException
from freenas.utils import configure_logging
from freenas.utils.debug import DebugService


DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'


class ManagementService(RpcService):
    def __init__(self, ctx):
        self.context = ctx

    def rescan_plugins(self):
        self.context.scan_plugins()

    def die(self):
        pass


class AlertService(RpcService):
    def emit(self, id):
        pass


class Main(object):
    def __init__(self):
        self.logger = logging.getLogger('alertd')
        self.root = None
        self.config = None
        self.datastore = None
        self.configstore = None
        self.client = None
        self.plugin_dirs = []
        self.renderers = {}
        self.managed_files = {}

    def init_datastore(self):
        try:
            self.datastore = datastore.get_datastore(self.config)
        except datastore.DatastoreException as err:
            self.logger.error('Cannot initialize datastore: %s', str(err))
            sys.exit(1)

        self.configstore = ConfigStore(self.datastore)

    def init_dispatcher(self):
        def on_error(reason, **kwargs):
            if reason in (ClientError.CONNECTION_CLOSED, ClientError.LOGOUT):
                self.logger.warning('Connection to dispatcher lost')
                self.connect()

        self.client = Client()
        self.client.on_error(on_error)
        self.connect()

    def connect(self):
        while True:
            try:
                self.client.connect('unix:')
                self.client.login_service('alertd')
                self.client.enable_server()
                self.client.register_service('alertd.management', ManagementService(self))
                self.client.register_service('alertd.debug', DebugService())
                self.client.resume_service('alertd.management')
                self.client.resume_service('alertd.debug')
                return
            except (OSError, RpcException) as err:
                self.logger.warning('Cannot connect to dispatcher: {0}, retrying in 1 second'.format(str(err)))
                time.sleep(1)

    def scan_plugins(self):
        for i in self.plugin_dirs:
            self.scan_plugin_dir(i)

    def scan_plugin_dir(self, dir):
        self.logger.debug('Scanning plugin directory %s', dir)
        for root, dirs, files in os.walk(dir):
            for name in files:
                abspath = os.path.join(root, name)
                path = os.path.relpath(abspath, dir)
                name, ext = os.path.splitext(path)

                if name in self.managed_files.keys():
                    continue

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CONFIGFILE, help='Middleware config file')
        args = parser.parse_args()
        configure_logging('/var/log/alertd.log', 'DEBUG')

        setproctitle.setproctitle('alertd')
        self.root = args.mountpoint
        self.config = args.c
        self.scan_plugins()
        self.init_renderers()
        self.init_datastore()
        self.init_dispatcher()
        self.client.wait_forever()

if __name__ == '__main__':
    m = Main()
    m.main()

