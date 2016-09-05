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
import copy
import logging
import argparse
import crypt
import errno
import datastore
import time
import json
import ipaddress
import socket
import setproctitle
import netif
from threading import RLock, Thread
from datetime import datetime, timedelta
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.server import Server
from freenas.dispatcher.rpc import RpcContext, RpcService, RpcException, generator, get_sender
from freenas.utils import first_or_default, configure_logging, extend, load_module_from_file
from freenas.utils.debug import DebugService
from freenas.utils.query import query, get, set, delete
from plugin import DirectoryState


NOGROUP_GID = 65533
DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'
DEFAULT_SOCKET_ADDRESS = 'unix:///var/run/dscached.sock'
AF_MAP = {
    socket.AF_INET: ipaddress.IPv4Address,
    socket.AF_INET6: ipaddress.IPv6Address
}


def privileged():
    """
    Warning: that function shall be used only within RPC method context
    """
    sender = get_sender()
    if not sender:
        logging.warning('privileged(): sender unknown, assuming no')
        return False

    return sender.credentials and sender.credentials['uid'] == 0


def my_ips():
    for iface in netif.list_interfaces().values():
        for addr in iface.addresses:
            if addr.af == netif.AddressFamily.LINK:
                continue

            yield str(addr.address)


def filter_af(addresses, af):
    return [a for a in addresses if type(ipaddress.ip_address(a)) is AF_MAP[af]]


def alias(d, obj, name):
    aliases = obj.get('aliases', [])
    aliases.append(obj[name])
    if d.domain_name:
        aliases.append('{0}@{1}'.format(obj[name], d.domain_name))

    return aliases


def resolve_primary_group(context, obj):
    # Allow backend to prepopulate gid
    if 'gid' in obj:
        return

    obj['gid'] = NOGROUP_GID
    if obj.get('group'):
        try:
            group = context.group_service.getgruuid(obj['group'])
            obj['gid'] = group['gid']
        except:
            pass


def fix_passwords(user):
    """
    Warning: this function shall be called only within an RPC method context
    """
    if not privileged():
        return extend(user, {
            'unixhash': '*',
            'smbhash': None
        })

    return user


def annotate(user, directory, name_field, cache=None):
    return extend(user, {
        'origin': {
            'directory': directory.name,
            'cached_at': None,
            'ttl': None
        }
    })


class CacheItem(object):
    def __init__(self, id, uuid, names, value, directory, ttl):
        self.id = id
        self.uuid = uuid.lower()
        self.names = names
        self.value = value
        self.directory = directory
        self.ttl = ttl
        self.created_at = datetime.utcnow()
        self.lock = RLock()
        self.destroyed = False

    @property
    def expired(self):
        return self.created_at + timedelta(seconds=self.ttl) < datetime.utcnow()

    @property
    def annotated(self):
        return extend(self.value, {
            'origin': {
                'directory': self.directory.name,
                'cached_at': self.created_at,
                'ttl': self.ttl
            }
        })


class TTLCacheStore(object):
    def __init__(self):
        self.id_store = {}
        self.name_store = {}
        self.uuid_store = {}
        self.hits = 0
        self.misses = 0

    def __len__(self):
        return len(self.id_store)

    def __getstate__(self):
        return {
            'size': len(self),
            'hits': self.hits,
            'misses': self.misses
        }

    def get(self, id=None, uuid=None, name=None):
        if id is not None:
            item = get(self.id_store, id)
        elif uuid is not None:
            item = self.uuid_store.get(uuid.lower())
        elif name is not None:
            item = self.name_store.get(name)
        else:
            raise AssertionError('Either id=, uuid= or name= parameter must be filled')

        if item:
            if item.expired:
                self.flush(item.uuid)
                self.misses += 1
                return

            self.hits += 1
            return item

        self.misses += 1
        return

    def flush(self, uuid):
        item = self.uuid_store.get(uuid)
        if item:
            with item.lock:
                if item.destroyed:
                    return

                for i in item.names:
                    del self.name_store[i]
                del self.uuid_store[item.uuid]
                delete(self.id_store, item.id)

    def query(self, filter=None, params=None):
        return query(self.id_store, *(filter or []), **(params or {}))

    def set(self, item):
        with item.lock:
            set(self.id_store, item.id, item)
            self.uuid_store[item.uuid] = item
            for i in item.names:
                self.name_store[i] = item

    def expire(self):
        for uuid, item in self.uuid_store.items():
            if item.expired:
                self.flush(uuid)

    def clear(self):
        self.name_store.clear()
        self.uuid_store.clear()
        self.id_store.clear()


class Directory(object):
    def __init__(self, context, definition):
        self.context = context
        self.id = definition['id']
        self.name = definition['name']
        self.domain_name = None
        self.plugin_type = definition['type']
        self.parameters = definition['parameters']
        self.enabled = definition['enabled']
        self.enumerate = definition['enumerate']
        self.max_uid = self.min_uid = None
        self.max_gid = self.min_gid = None
        self.status_code = 0
        self.status_message = None
        self.state = DirectoryState.DISABLED

        if definition['uid_range']:
            self.min_uid, self.max_uid = definition['uid_range']

        if definition['gid_range']:
            self.min_gid, self.max_gid = definition['gid_range']

        self.context.logger.info('Initializing directory {0}'.format(self.name))

        try:
            self.instance = context.plugins[self.plugin_type](self.context)
        except BaseException as err:
            self.context.logger.error('Failed to initialize directory {0}: {1}'.format(self.name, str(err)))
            self.context.logger.error('Parameters: {0}'.format(self.parameters))
            raise ValueError('Failed to initialize {0}'.format(self.plugin_type))

    def configure(self):
        try:
            if self.instance.get_kerberos_realm(self.parameters):
                self.context.client.call_sync('etcd.generation.generate_group', 'kerberos')

            domain_name = self.instance.configure(self.enabled, self)
            if domain_name:
                if any(d is not self and d.domain_name == domain_name for d in self.context.directories):
                    alt_domain_name = '{0}.{1}'.format(domain_name, self.name)
                    self.context.logger.warning('Directory {0}: domain name {1} in use, using {2}'.format(
                        self.name,
                        domain_name,
                        alt_domain_name
                    ))

                    domain_name = alt_domain_name

                self.domain_name = domain_name
        except BaseException as err:
            self.context.logger.error('Failed to configure {0}: {1}'.format(self.name, str(err)))
            self.context.logger.error('Stack trace: ', exc_info=True)

    def put_state(self, state):
        if self.state == state:
            return

        self.context.logger.info('Directory {0} state: {1}'.format(self.name, state.name))
        self.state = state
        self.context.client.emit_event('directory.changed', {
            'operation': 'update',
            'ids': [self.id]
        })

    def put_status(self, code, message):
        code_str = os.strerror(code) if code else 'OK'
        self.status_code = code
        self.status_message = message
        self.context.logger.info('Directory {0} status: [{1}] {2}'.format(self.name, code_str, message))


class ManagementService(RpcService):
    def __init__(self, context):
        self.logger = context.logger
        self.context = context

    def get_realms(self):
        realms = []

        for d in self.context.directories:
            if not d.enabled:
                continue

            realm = d.instance.get_kerberos_realm(d.parameters)
            if realm:
                realms.append(realm)

        return realms

    def get_cache_stats(self):
        return {
            'users': self.context.users_cache.__getstate__(),
            'groups': self.context.groups_cache.__getstate__(),
            'hosts': self.context.hosts_cache.__getstate__()
        }

    def clean_cache(self):
        for i in self.context.users_cache, self.context.groups_cache, self.context.hosts_cache:
            i.expire()

    def flush_cache(self):
        for i in self.context.users_cache, self.context.groups_cache, self.context.hosts_cache:
            i.clear()

    def populate_caches(self):
        self.context.populate_caches()

    def normalize_parameters(self, plugin, parameters):
        cls = self.context.plugins.get(plugin)
        if not cls:
            raise RpcException(errno.ENOENT, 'Plugin {0} not found'.format(plugin))

        return cls.normalize_parameters(parameters)

    def configure_directory(self, id):
        ds_d = self.context.datastore.get_by_id('directories', id)
        directory = first_or_default(lambda d: d.id == id, self.context.directories)
        if not directory:
            try:
                directory = Directory(self.context, ds_d)
                self.context.directories.append(directory)
            except BaseException as err:
                raise RpcException(errno.ENXIO, str(err))

        if not ds_d:
            # Directory was removed
            directory.enabled = False
            directory.configure()
            self.context.directories.remove(directory)
            return

        if ds_d['enabled'] and not directory.enabled:
            self.logger.info('Enabling directory {0}'.format(id))

        if not ds_d['enabled'] and directory.enabled:
            self.logger.info('Disabling directory {0}'.format(id))

        if ds_d['uid_range']:
            directory.min_uid, directory.max_uid = ds_d['uid_range']

        if ds_d['gid_range']:
            directory.min_gid, directory.max_gid = ds_d['gid_range']

        directory.enabled = ds_d['enabled']
        directory.parameters = ds_d['parameters']
        directory.configure()

    def get_status(self, id):
        directory = first_or_default(lambda d: d.id == id, self.context.directories)
        if not directory:
            raise RpcException(errno.ENOENT, 'Directory {0} not found'.format(id))

        return {
            'state': directory.state.name,
            'status_code': directory.status_code,
            'status_message': directory.status_message
        }

    def reload_config(self):
        self.context.load_config()


class AccountService(RpcService):
    def __init__(self, context):
        self.logger = context.logger
        self.context = context

    def __get_user(self, user_name):
        for d in self.context.get_enabled_directories():
            try:
                user = d.instance.getpwnam(user_name)
            except:
                continue

            if user:
                return self.__annotate(d, user), d.instance

        return None, None

    @generator
    def query(self, filter=None, params=None):
        for d in self.context.get_enabled_directories():
            for user in d.instance.getpwent(filter, params):
                resolve_primary_group(self.context, user)
                yield fix_passwords(annotate(user, d, 'username'))

    def getpwuid(self, uid):
        # Try the cache first
        item = self.context.users_cache.get(id=uid)
        if item:
            return fix_passwords(item.annotated)

        d = self.context.get_directory_for_id(uid=uid)
        dirs = [d] if d else self.context.get_enabled_directories()

        for d in dirs:
            try:
                user = d.instance.getpwuid(uid)
            except:
                continue

            if user:
                resolve_primary_group(self.context, user)
                aliases = alias(d, user, 'username')
                item = CacheItem(user['uid'], user['id'], aliases, copy.copy(user), d, 300)
                self.context.users_cache.set(item)
                return fix_passwords(item.annotated)

        raise RpcException(errno.ENOENT, 'UID {0} not found'.format(uid))

    def getpwnam(self, user_name):
        # Try the cache first
        item = self.context.users_cache.get(name=user_name)
        if item:
            return fix_passwords(item.annotated)

        if '@' in user_name:
            # Fully qualified user name
            user_name, domain_name = user_name.split('@', 1)
            dirs = [self.context.get_directory_by_domain(domain_name)]
        else:
            dirs = self.context.get_enabled_directories()

        for d in dirs:
            try:
                user = d.instance.getpwnam(user_name)
            except:
                continue

            if user:
                resolve_primary_group(self.context, user)
                aliases = alias(d, user, 'username')
                item = CacheItem(user['uid'], user['id'], aliases, copy.copy(user), d, 300)
                self.context.users_cache.set(item)
                return fix_passwords(item.annotated)

        raise RpcException(errno.ENOENT, 'User {0} not found'.format(user_name))

    def getpwuuid(self, uuid):
        # Try the cache first
        item = self.context.users_cache.get(uuid=uuid)
        if item:
            return fix_passwords(item.annotated)

        for d in self.context.get_enabled_directories():
            try:
                user = d.instance.getpwuuid(uuid)
            except:
                continue

            if user:
                resolve_primary_group(self.context, user)
                aliases = alias(d, user, 'username')
                item = CacheItem(user['uid'], user['id'], aliases, copy.copy(user), d, 300)
                self.context.users_cache.set(item)
                return fix_passwords(item.annotated)

        raise RpcException(errno.ENOENT, 'UUID {0} not found'.format(uuid))

    def getgroupmembership(self, user_name):
        result = []
        user = self.getpwnam(user_name)

        for g in user.get('groups', []):
            try:
                group = self.context.group_service.getgruuid(g)
                result.append(group['gid'])
            except:
                continue

        return result

    def authenticate(self, user_name, password):
        user = self.getpwnam(user_name)
        if not user:
            return False

        if 'unixhash' in user:
            unixhash = crypt.crypt(password, user['unixhash'])
            return unixhash == user['unixhash']

        entry = self.context.users_cache.get(name=user_name)
        return entry.directory.instance.authenticate(user['username'], password)

    def change_password(self, user_name, old_password, password):
        self.logger.debug('Change password request for user {0}'.format(user_name))
        user, plugin = self.__get_user(user_name)
        if not user:
            raise RpcException(errno.ENOENT, 'User {0} not found'.format(user_name))

        sender = get_sender()
        if not sender.credentials:
            raise RpcException(errno.EPERM, 'Permission denied')

        if sender.credentials['uid'] not in (user['uid'], 0):
            raise RpcException(errno.EPERM, 'Permission denied')

        plugin.change_password(user_name, old_password, password)


class GroupService(RpcService):
    def __init__(self, context):
        self.context = context

    @generator
    def query(self, filter=None, params=None):
        for d in self.context.get_enabled_directories():
            for group in d.instance.getgrent(filter, params):
                yield annotate(group, d, 'name')

    def getgrnam(self, name):
        # Try the cache first
        item = self.context.groups_cache.get(name=name)
        if item:
            return item.annotated

        if '@' in name:
            # Fully qualified group name
            name, domain_name = name.split('@', 1)
            dirs = [self.context.get_directory_by_domain(domain_name)]
        else:
            dirs = self.context.get_enabled_directories()

        for d in dirs:
            try:
                group = d.instance.getgrnam(name)
            except:
                continue

            if group:
                aliases = alias(d, group, 'name')
                item = CacheItem(group['gid'], group['id'], aliases, copy.copy(group), d, 300)
                self.context.groups_cache.set(item)
                return item.annotated

        raise RpcException(errno.ENOENT, 'Group {0} not found'.format(name))
    
    def getgrgid(self, gid):
        # Try the cache first
        item = self.context.groups_cache.get(id=gid)
        if item:
            return item.annotated

        d = self.context.get_directory_for_id(gid=gid)
        dirs = [d] if d else self.context.get_enabled_directories()

        for d in dirs:
            try:
                group = d.instance.getgrgid(gid)
            except:
                continue

            if group:
                aliases = alias(d, group, 'name')
                item = CacheItem(group['gid'], group['id'], aliases, copy.copy(group), d, 300)
                self.context.groups_cache.set(item)
                return item.annotated

        raise RpcException(errno.ENOENT, 'GID {0} not found'.format(gid))

    def getgruuid(self, uuid):
        # Try the cache first
        item = self.context.groups_cache.get(uuid=uuid)
        if item:
            return item.annotated

        for d in self.context.get_enabled_directories():
            try:
                group = d.instance.getgruuid(uuid)
            except:
                continue

            if group:
                aliases = alias(d, group, 'name')
                item = CacheItem(group['gid'], group['id'], aliases, copy.copy(group), d, 300)
                self.context.groups_cache.set(item)
                return item.annotated

        raise RpcException(errno.ENOENT, 'UUID {0} not found'.format(uuid))


class HostService(RpcService):
    def __init__(self, context):
        self.context = context

    def query(self, filter=None, params=None):
        pass

    def gethostbyname(self, name, af):
        if name in ('localhost', 'localhost.localdomain'):
            host = {
                'id': 'localhost',
                'addresses': ['127.0.0.1', '::1']
            }
        else:
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
        self.rpc = RpcContext()
        self.rpc.streaming_enabled = True
        self.rpc.streaming_burst = 16
        self.client = None
        self.server = None
        self.plugin_dirs = []
        self.plugins = {}
        self.directories = []
        self.users_cache = TTLCacheStore()
        self.groups_cache = TTLCacheStore()
        self.hosts_cache = TTLCacheStore()
        self.cache_ttl = 7200
        self.search_order = []
        self.cache_enumerations = True
        self.cache_lookups = True
        self.account_service = AccountService(self)
        self.group_service = GroupService(self)
        self.rpc.register_service_instance('dscached.account', self.account_service)
        self.rpc.register_service_instance('dscached.group', self.group_service)
        self.rpc.register_service_instance('dscached.host', HostService(self))
        self.rpc.register_service_instance('dscached.management', ManagementService(self))
        self.rpc.register_service_instance('dscached.debug', DebugService())

    def get_enabled_directories(self):
        return list(filter(
            lambda d: d and d.state == DirectoryState.BOUND,
            (self.get_directory_by_name(n) for n in self.get_search_order())
        ))

    def get_search_order(self):
        return ['local', 'system'] + self.search_order

    def get_directory_by_domain(self, domain_name):
        return first_or_default(lambda d: d.domain_name == domain_name, self.directories)

    def get_directory_by_name(self, name):
        return first_or_default(lambda d: d.name == name, self.directories)

    def get_directory_for_id(self, uid=None, gid=None):
        if uid is not None:
            if uid == 0:
                # Special case for root user
                return first_or_default(lambda d: d.plugin_type == 'local', self.directories)

            return first_or_default(
                lambda d: d.max_uid and d.max_uid >= uid >= d.min_uid,
                self.directories
            )

        if gid is not None:
            if gid == 0:
                # Special case for wheel group
                return first_or_default(lambda d: d.plugin_type == 'local', self.directories)

            return first_or_default(
                lambda d: d.max_gid and d.max_gid >= gid >= d.min_gid,
                self.directories
            )

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

    def init_server(self, address):
        self.server = Server(self)
        self.server.rpc = self.rpc
        self.server.start(address, transport_options={'permissions': 0o777})
        thread = Thread(target=self.server.serve_forever)
        thread.name = 'ServerThread'
        thread.daemon = True
        thread.start()

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
                self.client.enable_server(self.rpc)
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
                plugin = load_module_from_file(name, os.path.join(dir, f))
                plugin._init(self)
            except:
                self.logger.error('Cannot initialize plugin {0}'.format(f), exc_info=True)

    def register_plugin(self, name, cls):
        self.plugins[name] = cls
        self.logger.info('Registered plugin {0} (class {1})'.format(name, cls))

    def register_schema(self, name, schema):
        self.client.register_schema(name, schema)

    def init_directories(self):
        for i in self.datastore.query('directories'):
            try:
                directory = Directory(self, i)
                self.directories.append(directory)
                directory.configure()
            except BaseException as err:
                continue

    def load_config(self):
        self.search_order = self.configstore.get('directory.search_order')
        self.cache_ttl = self.configstore.get('directory.cache_ttl')
        self.cache_enumerations = self.configstore.get('directory.cache_enumerations')
        self.cache_lookups = self.configstore.get('directory.cache_lookups')

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CONFIGFILE, help='Middleware config file')
        parser.add_argument('-s', metavar='SOCKET', default=DEFAULT_SOCKET_ADDRESS, help='Socket address to listen on')
        args = parser.parse_args()
        configure_logging('/var/log/dscached.log', 'DEBUG')

        setproctitle.setproctitle('dscached')
        self.config = args.c
        self.parse_config(self.config)
        self.init_datastore()
        self.init_dispatcher()
        self.load_config()
        self.init_server(args.s)
        self.scan_plugins()
        self.init_directories()
        self.client.wait_forever()


if __name__ == '__main__':
    m = Main()
    m.main()

