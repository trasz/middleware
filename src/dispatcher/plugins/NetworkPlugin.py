#+
# Copyright 2014 iXsystems, Inc.
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

import errno
import ipaddress
import logging
import os
from freenas.dispatcher.rpc import RpcException, description, accepts, returns
from freenas.dispatcher.rpc import SchemaHelper as h
from freenas.utils import normalize, first_or_default
from datastore.config import ConfigNode
from gevent import hub
from task import Provider, Task, TaskException, VerifyException, query, TaskWarning
from debug import AttachFile, AttachCommandOutput


logger = logging.getLogger('NetworkPlugin')


def calculate_broadcast(address, netmask):
    return ipaddress.ip_interface('{0}/{1}'.format(address, netmask)).network.broadcast_address


@description("Provides access to global network configuration settings")
class NetworkProvider(Provider):
    @returns(h.ref('network-config'))
    def get_config(self):
        return ConfigNode('network', self.configstore)

    @returns(h.ref('network-status'))
    def get_status(self):
        return {
            'gateway': self.dispatcher.call_sync('networkd.configuration.get_default_routes'),
            'dns': self.dispatcher.call_sync('networkd.configuration.get_dns_config')
        }

    @returns(h.array(str))
    def get_my_ips(self):
        ips = []
        ifaces = self.dispatcher.call_sync('networkd.configuration.query_interfaces')
        ifaces.pop('mgmt0', None)
        for i, v in ifaces.items():
            if 'LOOPBACK' in v['flags']:
                continue
            for aliases in v['aliases']:
                if aliases['address'] and aliases['type'] != 'LINK':
                    ips.append(aliases['address'])
        return ips


@description("Provides access to network interface settings")
class InterfaceProvider(Provider):
    @query('network-interface')
    def query(self, filter=None, params=None):
        ifaces = self.dispatcher.call_sync('networkd.configuration.query_interfaces')

        def extend(i):
            try:
                i['status'] = ifaces[i['id']]
            except KeyError:
                # The given interface is either removed or disconnected
                return None
            return i

        return self.datastore.query(
            'network.interfaces', *(filter or []), callback=extend, **(params or {})
        )


@description("Provides information on system's network routes")
class RouteProvider(Provider):
    @query('network-route')
    def query(self, filter=None, params=None):
        return self.datastore.query('network.routes', *(filter or []), **(params or {}))


@description("Provides access to static host entries database")
class HostsProvider(Provider):
    @query('network-host')
    def query(self, filter=None, params=None):
        return self.datastore.query('network.hosts', *(filter or []), **(params or {}))


@description("Updates global network configuration settings")
@accepts(h.ref('network-config'))
class NetworkConfigureTask(Task):
    def verify(self, settings):
        return ['system']

    def run(self, settings):
        node = ConfigNode('network', self.configstore)
        node.update(settings)

        try:
            self.dispatcher.call_sync('networkd.configuration.configure_network')
            self.dispatcher.call_sync('etcd.generation.generate_group', 'network')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(e)))


@accepts(h.all_of(
    h.ref('network-interface'),
    h.required('type'),
    h.forbidden('id', 'status')
))
@returns(str)
class CreateInterfaceTask(Task):
    def verify(self, iface):
        return ['system']

    def run(self, iface):
        type = iface['type']
        name = self.dispatcher.call_sync('networkd.configuration.get_next_name', type)
        normalize(iface, {
            'id': name,
            'type': type,
            'cloned': True,
            'enabled': True,
            'dhcp': False,
            'rtadv': False,
            'noipv6': False,
            'mtu': None,
            'media': None,
            'aliases': []
        })

        if type == 'VLAN':
            iface.setdefault('vlan', {})
            normalize(iface['vlan'], {
                'parent': None,
                'tag': None
            })

        if type == 'LAGG':
            iface.setdefault('lagg', {})
            normalize(iface['lagg'], {
                'protocol': 'FAILOVER',
                'ports': []
            })

        if type == 'BRIDGE':
            iface.setdefault('bridge', {})
            normalize(iface['bridge'], {
                'members': []
            })

        self.datastore.insert('network.interfaces', iface)

        try:
            self.dispatcher.call_sync('networkd.configuration.configure_network')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure network: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'create',
            'ids': [name]
        })

        return name


@description("Deletes interface")
@accepts(str)
class DeleteInterfaceTask(Task):
    def verify(self, id):
        iface = self.datastore.get_by_id('network.interfaces', id)
        if not iface:
            raise VerifyException(errno.ENOENT, 'Interface {0} does not exist'.format(id))

        if iface['type'] not in ('VLAN', 'LAGG', 'BRIDGE'):
            raise VerifyException(errno.EBUSY, 'Cannot delete physical interface')

        return ['system']

    def run(self, id):
        self.datastore.delete('network.interfaces', id)
        try:
            self.dispatcher.call_sync('networkd.configuration.configure_network')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure network: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description("Alters network interface configuration")
@accepts(str, h.all_of(
    h.ref('network-interface'),
    h.forbidden('id', 'type', 'status')
))
class ConfigureInterfaceTask(Task):
    def verify(self, id, updated_fields):
        if not self.datastore.exists('network.interfaces', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Interface {0} does not exist'.format(id))

        return ['system']

    def run(self, id, updated_fields):
        task = 'networkd.configuration.configure_interface'
        entity = self.datastore.get_by_id('network.interfaces', id)

        if updated_fields.get('dhcp'):
            # Check for DHCP inconsistencies
            # 1. Check whether DHCP is enabled on other interfaces
            # 2. Check whether DHCP configures default route and/or DNS server addresses
            dhcp_used = self.datastore.exists('network.interfaces', ('dhcp', '=', True), ('id', '!=', id))
            dhcp_global = self.configstore.get('network.dhcp.assign_gateway') or \
                self.configstore.get('network.dhcp.assign_dns')

            if dhcp_used and dhcp_global:
                raise TaskException(errno.ENXIO, 'DHCP is already configured on another interface')

            # Clear all aliases
            entity['aliases'] = []

        if updated_fields.get('aliases'):
            # Forbid setting any aliases on interface with DHCP
            if (updated_fields.get('dhcp') or entity['dhcp']) and len(updated_fields['aliases']) > 0:
                raise TaskException(errno.EINVAL, 'Cannot set aliases when using DHCP')

            # Check for aliases inconsistencies
            ips = [x['address'] for x in updated_fields['aliases']]
            if any(ips.count(x) > 1 for x in ips):
                raise TaskException(errno.ENXIO, 'Duplicated IP alias')

            # Add missing broadcast addresses and address family
            for i in updated_fields['aliases']:
                normalize(i, {
                    'type': 'INET'
                })

                if not i.get('broadcast') and i['type'] == 'INET':
                    i['broadcast'] = str(calculate_broadcast(i['address'], i['netmask']))

        if updated_fields.get('vlan'):
            vlan = updated_fields['vlan']
            if (not vlan['parent'] and vlan['tag']) or (vlan['parent'] and not vlan['tag']):
                raise TaskException(errno.EINVAL, 'Can only set VLAN parent interface and tag at the same time')

        if 'enabled' in updated_fields:
            if entity['enabled'] and not updated_fields['enabled']:
                task = 'networkd.configuration.down_interface'

        entity.update(updated_fields)
        self.datastore.update('network.interfaces', id, entity)

        try:
            self.dispatcher.call_sync(task, id)
        except RpcException as err:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(err)))

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Enables interface")
@accepts(str)
class InterfaceUpTask(Task):
    def verify(self, id):
        iface = self.datastore.get_by_id('network.interfaces', id)
        if not iface:
            raise VerifyException(errno.ENOENT, 'Interface {0} does not exist'.format(id))

        if not iface['enabled']:
            raise VerifyException(errno.ENXIO, 'Interface {0} is disabled'.format(id))

        return ['system']

    def run(self, id):
        try:
            self.dispatcher.call_sync('networkd.configuration.up_interface', id)
        except RpcException as err:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(err)))

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Disables interface")
@accepts(str)
class InterfaceDownTask(Task):
    def verify(self, id):
        iface = self.datastore.get_by_id('network.interfaces', id)
        if not iface:
            raise VerifyException(errno.ENOENT, 'Interface {0} does not exist'.format(id))

        if not iface['enabled']:
            raise VerifyException(errno.ENXIO, 'Interface {0} is disabled'.format(id))

        return ['system']

    def run(self, id):
        try:
            self.dispatcher.call_sync('networkd.configuration.down_interface', id)
        except RpcException as err:
            raise TaskException(err.code, err.message, err.extra)

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Renews IP lease on interface")
@accepts(str)
class InterfaceRenewTask(Task):
    def verify(self, id):
        interface = self.datastore.get_by_id('network.interfaces', id)
        if not interface:
            raise VerifyException(errno.ENOENT, 'Interface {0} does not exist'.format(id))

        if not interface['enabled']:
            raise VerifyException(errno.ENXIO, 'Interface {0} is disabled'.format(id))

        if not interface['dhcp']:
            raise VerifyException(errno.EINVAL, 'Cannot renew a lease on interface that is not configured for DHCP')

        return ['system']

    def run(self, id):
        try:
            self.dispatcher.call_sync('networkd.configuration.renew_lease', id)
        except RpcException as err:
            raise TaskException(err.code, err.message, err.extra)

        self.dispatcher.dispatch_event('network.interface.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Adds host entry to the database")
@accepts(h.all_of(
    h.ref('network-host'),
    h.required('id', 'addresses')
))
class AddHostTask(Task):
    def verify(self, host):
        if self.datastore.exists('network.hosts', ('id', '=', host['id'])):
            raise VerifyException(errno.EEXIST, 'Host entry {0} already exists'.format(host['id']))
        return ['system']

    def run(self, host):
        self.datastore.insert('network.hosts', host)

        try:
            self.dispatcher.call_sync('etcd.generation.generate_group', 'network')
        except RpcException as err:
            raise TaskException(errno.ENXIO, 'Cannot update host: {0}'.format(str(err)))

        self.dispatcher.dispatch_event('network.host.changed', {
            'operation': 'create',
            'ids': [host['id']]
        })


@description("Updates host entry in the database")
@accepts(str, h.ref('network-host'))
class UpdateHostTask(Task):
    def verify(self, id, updated_fields):
        if not self.datastore.exists('network.hosts', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Host entry {0} does not exist'.format(id))

        return ['system']

    def run(self, id, updated_fields):
        host = self.datastore.get_one('network.hosts', ('id', '=', id))
        host.update(updated_fields)
        self.datastore.update('network.hosts', host['id'], host)

        try:
            self.dispatcher.call_sync('etcd.generation.generate_group', 'network')
        except RpcException as err:
            raise TaskException(errno.ENXIO, 'Cannot update host: {0}'.format(str(err)))

        self.dispatcher.dispatch_event('network.host.changed', {
            'operation': 'update',
            'ids': [id]
        })


@description("Deletes host entry from the database")
@accepts(str)
class DeleteHostTask(Task):
    def verify(self, id):
        if not self.datastore.exists('network.hosts', ('id', '=', id)):
            raise VerifyException(errno.ENOENT, 'Host entry {0} does not exist'.format(id))

        return ['system']

    def run(self, id):
        self.datastore.delete('network.hosts', id)

        try:
            self.dispatcher.call_sync('etcd.generation.generate_group', 'network')
        except RpcException as err:
            raise TaskException(errno.ENXIO, 'Cannot delete host: {0}'.format(str(err)))

        self.dispatcher.dispatch_event('network.host.changed', {
            'operation': 'delete',
            'ids': [id]
        })


@description("Adds static route to the system")
@accepts(h.all_of(
    h.ref('network-route'),
    h.required('id', 'type', 'network', 'netmask', 'gateway')
))
class AddRouteTask(Task):
    def verify(self, route):
        if self.datastore.exists('network.routes', ('id', '=', route['id'])):
            raise VerifyException(errno.EEXIST, 'Route {0} exists'.format(route['id']))

        for r in self.dispatcher.call_sync('network.route.query'):
            if (r['network'] == route['network']) and (r['netmask'] == route['netmask']) and (r['gateway'] == route['gateway']):
                raise VerifyException(errno.EINVAL, 'Cannot create two identical routes differing only in name.')

        if route['type'] == 'INET':
            max_cidr = 32
        else:
            max_cidr = 128
        if not (0 <= route['netmask'] <= max_cidr):
            raise VerifyException(
                errno.EINVAL,
                'Netmask value {0} is not valid. Allowed values are 0-{1} (CIDR).'.format(route['netmask'], max_cidr)
            )

        try:
            ipaddress.ip_network(os.path.join(route['network'], str(route['netmask'])))
        except ValueError:
            raise VerifyException(
                errno.EINVAL,
                '{0} would have host bits set. Change network or netmask to represent a valid network'.format(os.path.join(route['network'], str(route['netmask'])))
            )

        return ['system']

    def run(self, route):
        network = ipaddress.ip_network(os.path.join(route['network'], str(route['netmask'])))
        if ipaddress.ip_address(route['gateway']) in network:
            self.add_warning(
                TaskWarning(
                    errno.EINVAL,
                    'Gateway {0} is in the destination network {1}.'.format(route['gateway'], network.exploded)
                )
            )

        self.datastore.insert('network.routes', route)
        try:
            self.dispatcher.call_sync('networkd.configuration.configure_routes')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('network.route.changed', {
            'operation': 'create',
            'ids': [route['id']]
        })


@description("Updates static route in the system")
@accepts(str, h.ref('network-route'))
class UpdateRouteTask(Task):
    def verify(self, name, updated_fields):
        if not self.datastore.exists('network.routes', ('id', '=', name)):
            raise VerifyException(errno.ENOENT, 'Route {0} does not exist'.format(name))

        route = self.datastore.get_one('network.routes', ('id', '=', name))
        net = updated_fields['network'] if 'network' in updated_fields else route['network']
        netmask = updated_fields['netmask'] if 'netmask' in updated_fields else route['netmask']
        type = updated_fields['type'] if 'type' in updated_fields else route['type']

        if type == 'INET':
            max_cidr = 32
        else:
            max_cidr = 128
        if not (0 <= netmask <= max_cidr):
            raise VerifyException(
                errno.EINVAL,
                'Netmask value {0} is not valid. Allowed values are 0-{1} (CIDR).'.format(route['netmask'], max_cidr)
            )

        try:
            ipaddress.ip_network(os.path.join(net, str(netmask)))
        except ValueError:
            raise VerifyException(
                errno.EINVAL,
                '{0} would have host bits set. Change network or netmask to represent a valid network'.format(os.path.join(net, str(netmask)))
            )

        return ['system']

    def run(self, name, updated_fields):
        route = self.datastore.get_one('network.routes', ('id', '=', name))
        gateway = updated_fields['gateway'] if 'gateway' in updated_fields else route['gateway']
        net = updated_fields['network'] if 'network' in updated_fields else route['network']
        netmask = updated_fields['netmask'] if 'netmask' in updated_fields else route['netmask']

        network = ipaddress.ip_network(os.path.join(net, str(netmask)))
        if ipaddress.ip_address(gateway) in network:
            self.add_warning(
                TaskWarning(
                    errno.EINVAL,
                    'Gateway {0} is in the destination network {1}.'.format(gateway, network.exploded)
                )
            )

        route = self.datastore.get_one('network.routes', ('id', '=', name))
        route.update(updated_fields)
        self.datastore.update('network.routes', name, route)
        try:
            self.dispatcher.call_sync('networkd.configuration.configure_routes')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('network.route.changed', {
            'operation': 'update',
            'ids': [route['id']]
        })


@description("Deletes static route from the system")
@accepts(str)
class DeleteRouteTask(Task):
    def verify(self, name):
        if not self.datastore.exists('network.routes', ('id', '=', name)):
            raise VerifyException(errno.ENOENT, 'route {0} does not exist'.format(name))

        return ['system']

    def run(self, name):
        self.datastore.delete('network.routes', name)
        try:
            self.dispatcher.call_sync('networkd.configuration.configure_routes')
        except RpcException as e:
            raise TaskException(errno.ENXIO, 'Cannot reconfigure interface: {0}'.format(str(e)))

        self.dispatcher.dispatch_event('network.route.changed', {
            'operation': 'delete',
            'ids': [name]
        })


def collect_debug(dispatcher):
    yield AttachFile('hosts', '/etc/hosts')
    yield AttachFile('resolv.conf', '/etc/resolv.conf')
    yield AttachCommandOutput('ifconfig', ['/sbin/ifconfig', '-v'])
    yield AttachCommandOutput('routing-table', ['/sbin/netstat', '-nr'])
    yield AttachCommandOutput('arp-table', ['/sbin/arp', '-an'])
    yield AttachCommandOutput('arp-table', ['/sbin/arp', '-an'])
    yield AttachCommandOutput('mbuf-stats', ['/sbin/netstat', '-m'])
    yield AttachCommandOutput('interface-stats', ['/sbin/netstat', '-i'])

    for i in ['ip', 'arp', 'udp', 'tcp', 'icmp']:
        yield AttachCommandOutput('netstat-proto-{0}'.format(i), ['/sbin/netstat', '-p', i, '-s'])


def _depends():
    return ['DevdPlugin']


def _init(dispatcher, plugin):
    def on_resolv_conf_change(args):
        # If DNS has changed lets reset our DNS resolver to reflect reality
        logger.debug('Resetting resolver')
        del hub.get_hub().resolver
        hub.get_hub()._resolver = None

    plugin.register_schema_definition('network-interface', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {'$ref': 'network-interface-type'},
            'id': {'type': 'string'},
            'name': {'type': ['string', 'null']},
            'enabled': {'type': 'boolean'},
            'dhcp': {'type': 'boolean'},
            'rtadv': {'type': 'boolean'},
            'noipv6': {'type': 'boolean'},
            'mtu': {'type': ['integer', 'null']},
            'media': {'type': ['string', 'null']},
            'mediaopts': {'$ref': 'network-interface-mediaopts'},
            'capabilities': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'add': {'$ref': 'network-interface-capabilities'},
                    'del': {'$ref': 'network-interface-capabilities'},
                }
            },
            'aliases': {
                'type': 'array',
                'items': {'$ref': 'network-interface-alias'}
            },
            'vlan': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'parent': {'type': ['string', 'null']},
                    'tag': {
                        'type': ['integer', 'null'],
                        'minimum': 1,
                        'maximum': 4095
                    }
                }
            },
            'lagg': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'protocol': {'$ref': 'network-aggregation-protocols'},
                    'ports': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    }
                }
            },
            'bridge': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'members': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    }
                }
            },
            'status': {'$ref': 'network-interface-status'}
        }
    })

    plugin.register_schema_definition('network-interface-alias', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'type': {
                'type': 'string',
                'enum': [
                    'INET',
                    'INET6'
                ]
            },
            'address': {'$ref': 'ip-address'},
            'netmask': {'type': 'integer'},
            'broadcast': {
                'oneOf': [{'$ref': 'ipv4-address'}, {'type': 'null'}]
            }
        }
    })

    plugin.register_schema_definition('network-route', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'type': {'type': 'string', 'enum': ['INET', 'INET6']},
            'network': {'$ref': 'ip-address'},
            'netmask': {'type': 'integer'},
            'gateway': {'$ref': 'ip-address'}
        }
    })

    plugin.register_schema_definition('network-host', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'id': {'type': 'string'},
            'addresses': {
                'type': 'array',
                'items': {'$ref': 'ip-address'}
            }
        }
    })

    plugin.register_schema_definition('network-config', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'autoconfigure': {'type': 'boolean'},
            'http_proxy': {'type': ['string', 'null']},
            'gateway': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'ipv4': {'oneOf': [{'$ref': 'ipv4-address'}, {'type': 'null'}]},
                    'ipv6': {'oneOf': [{'$ref': 'ipv6-address'}, {'type': 'null'}]}
                }
            },
            'dns': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'addresses': {'type': 'array', 'items': {'$ref': 'ip-address'}},
                    'search': {'type': 'array', 'items': {'type': 'string'}}
                }
            },
            'dhcp': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'assign_gateway': {'type': 'boolean'},
                    'assign_dns': {'type': 'boolean'}
                }
            },
            'netwait': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'enabled': {'type': 'boolean'},
                    'addresses': {
                        'type': 'array',
                        'items': {'$ref': 'ip-address'}
                    }
                }
            }
        }
    })

    plugin.register_schema_definition('network-status', {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'gateway': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'ipv4': {'oneOf': [{'$ref': 'ipv4-address'}, {'type': 'null'}]},
                    'ipv6': {'oneOf': [{'$ref': 'ipv6-address'}, {'type': 'null'}]}
                }
            },
            'dns': {
                'type': 'object',
                'additionalProperties': False,
                'properties': {
                    'addresses': {'type': 'array', 'items': {'$ref': 'ip-address'}},
                    'search': {'type': 'array', 'items': {'type': 'string'}}
                }
            }
        }
    })

    plugin.register_provider('network.config', NetworkProvider)
    plugin.register_provider('network.interface', InterfaceProvider)
    plugin.register_provider('network.route', RouteProvider)
    plugin.register_provider('network.host', HostsProvider)

    plugin.register_task_handler('network.config.update', NetworkConfigureTask)
    plugin.register_task_handler('network.host.create', AddHostTask)
    plugin.register_task_handler('network.host.update', UpdateHostTask)
    plugin.register_task_handler('network.host.delete', DeleteHostTask)
    plugin.register_task_handler('network.route.create', AddRouteTask)
    plugin.register_task_handler('network.route.update', UpdateRouteTask)
    plugin.register_task_handler('network.route.delete', DeleteRouteTask)
    plugin.register_task_handler('network.interface.up', InterfaceUpTask)
    plugin.register_task_handler('network.interface.down', InterfaceDownTask)
    plugin.register_task_handler('network.interface.update', ConfigureInterfaceTask)
    plugin.register_task_handler('network.interface.create', CreateInterfaceTask)
    plugin.register_task_handler('network.interface.delete', DeleteInterfaceTask)
    plugin.register_task_handler('network.interface.renew', InterfaceRenewTask)

    plugin.register_event_handler('network.dns.configured', on_resolv_conf_change)
    plugin.register_event_type('network.interface.changed')
    plugin.register_event_type('network.host.changed')
    plugin.register_event_type('network.route.changed')

    plugin.register_debug_hook(collect_debug)
