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
import crypt
import errno
import datastore
import time
import json
import imp
import ipaddress
import socket
import setproctitle
import netif
from itertools import chain
from more_itertools import unique_everseen
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException
from freenas.utils import first_or_default, configure_logging
from freenas.utils.debug import DebugService


DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'
AF_MAP = {
    socket.AF_INET: ipaddress.IPv4Address,
    socket.AF_INET6: ipaddress.IPv6Address
}


def my_ips():
    for iface in netif.list_interfaces().values():
        for addr in iface.addresses:
            if addr.af == netif.AddressFamily.LINK:
                continue

            yield str(addr.address)


def filter_af(addresses, af):
    return [a for a in addresses if type(ipaddress.ip_address(a)) is AF_MAP[af]]


class Directory(object):
    def __init__(self, context, definition):
        self.context = context
        self.id = definition['id']
        self.plugin_type = definition['plugin']
        self.parameters = definition['parameters']
        self.min_uid, self.max_uid = definition['uid_range']
        self.min_gid, self.max_gid = definition['gid_range']
        self.enabled = definition['enabled']
        self.context.logger.info('Initializing directory {0}'.format(self.id))

        try:
            self.instance = context.plugins[self.plugin_type](self.context, self.parameters)
        except BaseException as err:
            self.context.logger.error('Failed to initialize directory {0}: {1}'.format(self.plugin_type, str(err)))
            self.context.logger.error('Parameters: {0}'.format(self.parameters))
            raise ValueError('Failed to initialize {0}'.format(self.plugin_type))

    def configure(self):
        try:
            self.instance.configure(
                self.enabled,
                self.min_uid, self.max_uid,
                self.min_gid, self.max_gid,
                self.parameters
            )
        except BaseException as err:
            self.context.logger.error('Failed to configure {0}: {1}'.format(self.id, str(err)))


class ManagementService(RpcService):
    def __init__(self, context):
        self.logger = context.logger
        self.context = context

    def get_realms(self):
        realms = []

        for d in self.context.directories:
            realm = d.instance.get_kerberos_realm()
            if realm:
                realms.append(realm)

        return realms

    def configure_directory(self, id):
        ds_d = self.context.datastore.get_by_id('directories', id)
        directory = first_or_default(lambda d: d.id == id, self.context.directories)
        if not directory:
            raise RpcException(errno.ENOENT, 'Directory {0} not found'.format(id))

        if ds_d['enabled'] and not directory.enabled:
            self.logger.info('Enabling directory {0}'.format(id))

        if not ds_d['enabled'] and directory.enabled:
            self.logger.info('Disabling directory {0}'.format(id))

        directory.min_uid, directory.max_uid = ds_d['uid_range']
        directory.min_gid, directory.max_gid = ds_d['gid_range']
        directory.enabled = ds_d['enabled']
        directory.parameters = ds_d['parameters']
        directory.configure()


class AccountService(RpcService):
    def __init__(self, context):
        self.logger = context.logger
        self.context = context

    def __annotate(self, directory, user):
        user['gids'] = [g['gid'] for g in directory.instance.getgrent([('id', 'in', user['groups'])])]
        user['origin'] = {'directory': directory.plugin_type}
        return user

    def __get_user(self, user_name):
        for d in self.context.directories:
            try:
                user = d.instance.getpwnam(user_name)
            except:
                continue

            if user:
                return self.__annotate(d, user), d.instance

        return None, None

    def query(self, filter=None, params=None):
        iters = []
        for d in self.context.directories:
            try:
                iters.append((self.__annotate(d, i) for i in d.instance.getpwent(filter, params)))
            except BaseException:
                continue

        return list(unique_everseen(chain(*iters), lambda i: i['id']))
    
    def getpwuid(self, uid):
        for d in self.context.directories:
            user = d.instance.getpwuid(uid)
            if user:
                return self.__annotate(d, user)

        return None

    def getpwnam(self, user_name):
        for d in self.context.directories:
            try:
                user = d.instance.getpwnam(user_name)
            except:
                continue

            if user:
                return self.__annotate(d, user)

        return None

    def authenticate(self, user_name, password):
        user = self.getpwnam(user_name)
        if not user:
            return False

        unixhash = crypt.crypt(password, user['unixhash'])
        return unixhash == user['unixhash']

    def change_password(self, user_name, password):
        self.logger.debug('Change password request for user {0}'.format(user_name))
        user, plugin = self.__get_user(user_name)
        if not user:
            raise RpcException(errno.ENOENT, 'User {0} not found'.format(user_name))

        plugin.change_password(user_name, password)


class GroupService(RpcService):
    def __init__(self, context):
        self.context = context

    def __annotate(self, name, user):
        user['origin'] = {'directory': name}
        return user

    def query(self, filter=None, params=None):
        iters = []
        for d in self.context.directories:
            iters.append((self.__annotate(d.plugin_type, i) for i in d.instance.getgrent(filter, params)))

        return list(unique_everseen(chain(*iters), lambda i: i['id']))
    
    def getgrnam(self, name):
        for d in self.context.directories:
            group = d.instance.getgrnam(name)
            if group:
                return self.__annotate(d.plugin_type, group)

        return None
    
    def getgrgid(self, gid):
        for d in self.context.plugins.items():
            group = d.instance.getgrgid(gid)
            if group:
                return self.__annotate(d.plugin_type, group)

        return None


class HostService(RpcService):
    def __init__(self, context):
        self.context = context

    def query(self, filter=None, params=None):
        pass

    def gethostbyname(self, name, af):
        host = self.context.datastore.get_by_id('network.hosts', name)
        if host:
            addrs = filter_af(host['addresses'], af)
            if not addrs:
                return

            return {
                'name': host['id'],
                'aliases': [],
                'addresses': addrs
            }

    def gethostbyaddr(self, addr, af):
        if addr in list(my_ips()):
            hostname = self.context.configstore.get('system.hostname')
            return {
                'name': hostname,
                'aliases': [
                    hostname.split('.')[0],
                    'localhost',
                    'localhost.localdomain'
                ],
                'addresses': [addr]
            }

        host = self.context.datastore.get_one('network.hosts', ('addresses', 'in', addr))
        if host:
            addrs = filter_af(host['addresses'], af)
            if not addrs:
                return

            return {
                'name': host['id'],
                'aliases': [],
                'addresses': addrs
            }


class Main(object):
    def __init__(self):
        self.logger = logging.getLogger('dscached')
        self.config = None
        self.datastore = None
        self.configstore = None
        self.client = None
        self.plugin_dirs = []
        self.plugins = {}
        self.directories = []

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
            with open(filename, 'r') as f:
                self.config = json.load(f)
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
                self.client.register_service('dscached.host', HostService(self))
                self.client.register_service('dscached.management', ManagementService(self))
                self.client.register_service('dscached.debug', DebugService())
                self.client.resume_service('dscached.account')
                self.client.resume_service('dscached.group')
                self.client.resume_service('dscached.host')
                self.client.resume_service('dscached.management')
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
        self.plugins[name] = cls
        self.logger.info('Registered plugin {0} (class {1})'.format(name, cls))

    def register_schema(self, name, schema):
        self.client.register_schema(name, schema)

    def init_directories(self):
        for i in self.datastore.query('directories', sort=['priority']):
            try:
                directory = Directory(self, i)
                directory.configure()
                self.directories.append(directory)
            except BaseException as err:
                continue

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
        self.init_directories()
        self.client.wait_forever()


if __name__ == '__main__':
    m = Main()
    m.main()

