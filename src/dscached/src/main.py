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
import re
import datastore
import time
import json
import imp
import threading
import setproctitle
from datetime import timedelta, datetime
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException
from freenas.utils import configure_logging
from freenas.utils.debug import DebugService


DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'


class AccountService(RpcService):
    def __init__(self, context):
        self.context = context

    def __annotate(self, name, user):
        user['origin'] = {'directory': name}
        return user

    def query(self, filter=None, params=None):
        results = []
        for name, plugin in self.context.plugins.items():
            results.extend(self.__annotate(name, i) for i in plugin.getpwent(filter, params))

        return results

    def getpwent(self):
        return self.getpwent()
    
    def getpwuid(self, uid):
        for name, plugin in self.context.plugins.items():
            user = plugin.getpwuid(uid)
            if user:
                return self.__annotate(name, user)

        return None

    def getpwnam(self, user_name):
        for name, plugin in self.context.plugins.items():
            user = plugin.getpwnam(user_name)
            if user:
                return self.__annotate(name, user)

        return None


class GroupService(RpcService):
    def __init__(self, context):
        self.context = context

    def query(self, filter=None, params=None):
        pass

    def getgrent(self):
        return self.datastore.query('groups')
    
    def getgrnam(self, name):
        pass
    
    def getgrgid(self, gid):
        pass


class Main(object):
    def __init__(self):
        self.logger = logging.getLogger('dscached')
        self.config = None
        self.datastore = None
        self.configstore = None
        self.client = None
        self.plugin_dirs = []
        self.plugins = {}

    def init_datastore(self):
        try:
            self.datastore = datastore.get_datastore()
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

    def parse_config(self, filename):
        try:
            f = open(filename, 'r')
            self.config = json.load(f)
            f.close()
        except IOError as err:
            self.logger.error('Cannot read config file: %s', err.message)
            sys.exit(1)
        except ValueError:
            self.logger.error('Config file has unreadable format (not valid JSON)')
            sys.exit(1)

        self.plugin_dirs = self.config['dscached']['plugin-dirs']

    def connect(self):
        while True:
            try:
                self.client.connect('unix:')
                self.client.login_service('dscached')
                self.client.enable_server()
                self.client.register_service('dscached.account', AccountService(self))
                self.client.register_service('dscached.group', GroupService(self))
                self.client.register_service('dscached.debug', DebugService())
                self.client.resume_service('dscached.account')
                self.client.resume_service('dscached.group')
                self.client.resume_service('dscached.debug')
                return
            except (OSError, RpcException) as err:
                self.logger.warning('Cannot connect to dispatcher: {0}, retrying in 1 second'.format(str(err)))
                time.sleep(1)

    def scan_plugins(self):
        for i in self.plugin_dirs:
            self.scan_plugin_dir(i)

    def scan_plugin_dir(self, dir):
        self.logger.debug('Scanning plugin directory %s', dir)
        for f in os.listdir(dir):
            name, ext = os.path.splitext(os.path.basename(f))
            if ext != '.py':
                continue

            try:
                plugin = imp.load_source(name, os.path.join(dir, f))
                plugin._init(self)
            except:
                self.logger.error('Cannot initialize plugin {0}'.format(f), exc_info=True)

    def register_plugin(self, name, cls):
        self.plugins[name] = cls(self)
        self.logger.info('Registered plugin {0} (class {1})'.format(name, cls))

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CONFIGFILE, help='Middleware config file')
        args = parser.parse_args()
        configure_logging('/var/log/dscached.log', 'DEBUG')

        setproctitle.setproctitle('dscached')
        self.config = args.c
        self.parse_config(self.config)
        self.init_datastore()
        self.init_dispatcher()
        self.scan_plugins()
        self.client.wait_forever()


if __name__ == '__main__':
    m = Main()
    m.main()

