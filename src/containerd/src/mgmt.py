#
# Copyright 2015 iXsystems, Inc.
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

import ipaddress
import logging
import errno
import weakref
import gevent
import netif
from dhcp.server import Server
from dhcp.lease import Lease


class AddressAllocation(object):
    def __init__(self):
        self.vm = None
        self.hostname = None
        self.lease = None

    def __getstate__(self):
        return {
            'vm_id': self.vm().id if self.vm() else None,
            'vm_name': self.vm().name if self.vm() else None,
            'mac': self.lease.client_mac,
            'lease': self.lease.__getstate__()
        }


class ManagementNetwork(object):
    def __init__(self, context, ifname, subnet):
        self.context = context
        self.ifname = ifname
        self.subnet = subnet
        self.bridge_if = None
        self.dhcp_server_thread = None
        self.dhcp_server = Server()
        self.allocations = {}
        self.logger = logging.getLogger('ManagementNetwork:{0}'.format(self.ifname))

    def up(self):
        # Destroy old bridge (if exists)
        try:
            netif.destroy_interface(self.ifname)
        except OSError as err:
            if err.errno != errno.ENXIO:
                raise RuntimeError('Cannot destroy {0}: {1}'.format(self.ifname, str(err)))

        # Setup bridge
        self.bridge_if = netif.get_interface(netif.create_interface('bridge'))
        self.bridge_if.rename(self.ifname)
        self.bridge_if.description = 'containerd management network'
        self.bridge_if.add_address(netif.InterfaceAddress(
            netif.AddressFamily.INET,
            self.subnet
        ))
        self.bridge_if.up()

        # Start DHCP server
        self.dhcp_server.server_name = 'FreeNAS'
        self.dhcp_server.on_request = self.dhcp_request
        self.dhcp_server.start(self.subnet)
        self.dhcp_server_thread = gevent.spawn(self.dhcp_worker)

    def down(self):
        self.bridge_if.down()
        netif.destroy_interface(self.ifname)

    def dhcp_worker(self):
        self.dhcp_server.serve()

    def allocation_by_ip(self, ip):
        for allocation in self.allocations.values():
            if allocation.lease.client_ip == ip:
                return allocation

        return None

    def pick_ip_address(self):
        for i in self.subnet.network.hosts():
            if i == self.subnet.ip:
                continue

            if not self.allocation_by_ip(i):
                return i

        return None

    def allocate_ip_address(self, mac, hostname):
        allocation = self.allocations.get(mac)
        if not allocation or not allocation.vm():
            vm = self.context.vm_by_mgmt_mac(mac)
            if not vm:
                return None

            allocation = AddressAllocation()
            allocation.vm = weakref.ref(vm)
            allocation.hostname = hostname
            allocation.lease = Lease()
            allocation.lease.client_mac = mac
            allocation.lease.client_ip = self.pick_ip_address()
            allocation.lease.client_mask = self.subnet.netmask
            allocation.lease.static_routes = [
                (ipaddress.ip_network('169.254.169.254/32'), ipaddress.ip_address('169.254.16.1'))
            ]
            self.allocations[mac] = allocation

        return allocation

    def dhcp_request(self, mac, hostname):
        self.logger.info('DHCP request from {0} ({1})'.format(mac, hostname))
        allocation = self.allocate_ip_address(mac, hostname)
        if not allocation:
            self.logger.warning('Unknown MAC address {0}'.format(mac))
            return None

        self.logger.info('Allocating IP address {0} to VM {1}'.format(allocation.lease.client_ip, allocation.vm().name))
        return allocation.lease

    def dhcp_release(self, lease):
        pass
