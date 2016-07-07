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

import os
import enum
import sys
import argparse
import json
import logging
import setproctitle
import errno
import time
import string
import random
import threading
import gevent
import gevent.os
import gevent.monkey
import subprocess
import serial
import netif
import socket
import signal
import select
import tempfile
import docker
import ipaddress
import pf
import urllib.parse
from bsd import kld, sysctl
from gevent.queue import Queue
from gevent.event import Event
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource
from geventwebsocket.exceptions import WebSocketError
from pyee import EventEmitter
from datastore import DatastoreException, get_datastore
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException, private, generator
from freenas.utils.debug import DebugService
from freenas.utils import first_or_default, configure_logging
from vnc import app
from mgmt import ManagementNetwork
from ec2 import EC2MetadataServer
from proxy import ReverseProxyServer


gevent.monkey.patch_all()


BOOTROM_PATH = '/usr/local/share/containerd/firmware/BHYVE_UEFI_20160526.fd'
BOOTROM_CSM_PATH = '/usr/local/share/containerd/firmware/BHYVE_UEFI_CSM_20151002.fd'
MGMT_ADDR = ipaddress.ip_interface('172.20.0.1/16')
MGMT_INTERFACE = 'mgmt0'
NAT_ADDR = ipaddress.ip_interface('172.21.0.1/16')
NAT_INTERFACE = 'nat0'
DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'
SCROLLBACK_SIZE = 20 * 1024


vtx_enabled = False
restricted_guest = True


class VirtualMachineState(enum.Enum):
    STOPPED = 1
    BOOTLOADER = 2
    RUNNING = 3


class DockerHostState(enum.Enum):
    DOWN = 1
    OFFLINE = 2
    UP = 3


def generate_id():
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(32)])


def get_docker_ports(details):
    if 'HostConfig' not in details:
        return

    for port, config in details['HostConfig']['PortBindings'].items():
        num, proto = port.split('/')
        yield {
            'protocol': proto.upper(),
            'container_port': int(num),
            'host_port': int(config[0]['HostPort'])
        }


class BinaryRingBuffer(object):
    def __init__(self, size):
        self.data = bytearray(size)

    def push(self, data):
        del self.data[0:len(data)]
        self.data += data

    def read(self):
        return self.data


class VirtualMachine(object):
    def __init__(self, context):
        self.context = context
        self.id = None
        self.name = None
        self.nmdm = None
        self.state = VirtualMachineState.STOPPED
        self.config = None
        self.devices = []
        self.files_root = None
        self.bhyve_process = None
        self.scrollback = BinaryRingBuffer(SCROLLBACK_SIZE)
        self.console_fd = None
        self.console_queues = []
        self.console_thread = None
        self.tap_interfaces = {}
        self.vnc_socket = None
        self.vnc_port = None
        self.thread = None
        self.exiting = False
        self.docker_host = None
        self.network_ready = Event()
        self.logger = logging.getLogger('VM:{0}'.format(self.name))

    @property
    def management_lease(self):
        return self.context.mgmt.allocations.get(self.get_link_address('MANAGEMENT'))

    @property
    def nat_lease(self):
        return self.context.mgmt.allocations.get(self.get_link_address('NAT'))

    def get_link_address(self, type):
        nic = first_or_default(
            lambda d: d['type'] == 'NIC' and d['properties']['type'] == type,
            self.devices
        )

        if not nic:
            return None

        return nic['properties']['link_address']

    def build_args(self):
        xhci_devices = {}
        args = [
            '/usr/sbin/bhyve', '-A', '-H', '-P', '-c', str(self.config['ncpus']), '-m', str(self.config['memsize'])]

        if self.config['bootloader'] in ['UEFI', 'UEFI_CSM']:
            index = 3
        else:
            index = 1
            args += ['-s', '0:0,hostbridge']

        for i in self.devices:
            if i['type'] == 'DISK':
                drivermap = {
                    'AHCI': 'ahci-hd',
                    'VIRTIO': 'virtio-blk'
                }

                driver = drivermap.get(i['properties'].get('mode', 'AHCI'))
                path = self.context.client.call_sync('vm.get_disk_path', self.id, i['name'])
                args += ['-s', '{0}:0,{1},{2}'.format(index, driver, path)]
                index += 1

            if i['type'] == 'CDROM':
                path = self.context.client.call_sync('vm.get_disk_path', self.id, i['name'])
                args += ['-s', '{0}:0,ahci-cd,{1}'.format(index, path)]
                index += 1

            if i['type'] == 'VOLUME':
                if i['properties']['type'] == 'VT9P':
                    path = self.context.client.call_sync('vm.get_volume_path', self.id, i['name'])
                    args += ['-s', '{0}:0,virtio-9p,{1}={2}'.format(index, i['name'], path)]
                    index += 1

            if i['type'] == 'NIC':
                drivermap = {
                    'VIRTIO': 'virtio-net',
                    'E1000': 'e1000',
                    'NE2K': 'ne2k'
                }

                mac = i['properties']['link_address']
                iface = self.init_tap(i['name'], i['properties'], mac)
                if not iface:
                    continue

                driver = drivermap.get(i['properties'].get('device', 'VIRTIO'))
                args += ['-s', '{0}:0,{1},{2},mac={3}'.format(index, driver, iface, mac)]
                index += 1

            if i['type'] == 'GRAPHICS':
                self.init_vnc()
                w, h = i['properties']['resolution'].split('x')
                args += ['-s', '{0}:0,fbuf,unix={1},w={2},h={3},vncserver'.format(index, self.vnc_socket, w, h)]
                index += 1

            if i['type'] == 'USB':
                xhci_devices[i['properties']['device']] = i.get('config')

        if xhci_devices:
            args += ['-s', '{0}:0,xhci,{1}'.format(index, ','.join(xhci_devices.keys()))]
            index += 1

        args += ['-s', '31,lpc', '-l', 'com1,{0}'.format(self.nmdm[0])]

        if self.config['bootloader'] == 'UEFI':
            args += ['-l', 'bootrom,{0}'.format(BOOTROM_PATH)]

        if self.config['bootloader'] == 'UEFI_CSM':
            args += ['-l', 'bootrom,{0}'.format(BOOTROM_CSM_PATH)]

        args.append(self.name)
        self.logger.debug('bhyve args: {0}'.format(args))
        return args

    def init_vnc(self):
        self.vnc_socket = '/var/run/containerd/{0}.vnc.sock'.format(self.id)
        self.cleanup_vnc()

        if self.config.get('vnc_enabled', False):
            self.context.proxy_server.add_proxy(self.config['vnc_port'], self.vnc_socket)

    def cleanup_vnc(self):
        if self.config.get('vnc_enabled', False):
            self.context.proxy_server.remove_proxy(self.config['vnc_port'])

        if self.vnc_socket and os.path.exists(self.vnc_socket):
            os.unlink(self.vnc_socket)

    def init_tap(self, name, nic, mac):
        try:
            iface = netif.get_interface(netif.create_interface('tap'))
            iface.description = 'vm:{0}:{1}'.format(self.name, name)
            iface.up()

            if nic['type'] == 'BRIDGE':
                if nic['bridge']:
                    try:
                        target_if = netif.get_interface(nic['bridge'])
                    except KeyError:
                        raise RpcException(errno.ENOENT, 'Target interface {0} does not exist'.format(nic['bridge']))

                    if isinstance(target_if, netif.BridgeInterface):
                        target_if.add_member(iface.name)
                    else:
                        bridges = list(b for b in netif.list_interfaces().keys() if 'brg' in b)
                        for b in bridges:
                            bridge = netif.get_interface(b, bridge=True)
                            if nic['bridge'] in bridge.members:
                                bridge.add_member(iface.name)
                                break
                        else:
                            new_bridge = netif.get_interface(netif.create_interface('bridge'))
                            new_bridge.rename('brg{0}'.format(len(bridges)))
                            new_bridge.description = 'vm bridge to {0}'.format(nic['bridge'])
                            new_bridge.up()
                            new_bridge.add_member(nic['bridge'])
                            new_bridge.add_member(iface.name)

            if nic['type'] == 'MANAGEMENT':
                mgmt = netif.get_interface('mgmt0', bridge=True)
                mgmt.add_member(iface.name)

            if nic['type'] == 'NAT':
                mgmt = netif.get_interface('nat0', bridge=True)
                mgmt.add_member(iface.name)

            self.tap_interfaces[iface] = mac
            return iface.name
        except (KeyError, OSError) as err:
            self.logger.warning('Cannot initialize NIC {0}: {1}'.format(name, str(err)))
            return

    def cleanup_tap(self, iface):
        bridges = list(b for b in netif.list_interfaces().keys() if 'brg' in b)
        for b in bridges:
            bridge = netif.get_interface(b, bridge=True)
            if iface.name in bridge.members:
                bridge.delete_member(iface.name)

            if len([b for b in bridge.members]) == 1:
                bridge.down()
                netif.destroy_interface(bridge.name)

        iface.down()
        netif.destroy_interface(iface.name)

    def get_nmdm(self):
        index = self.context.allocate_nmdm()
        return '/dev/nmdm{0}A'.format(index), '/dev/nmdm{0}B'.format(index)

    def start(self):
        self.context.logger.info('Starting container {0} ({1})'.format(self.name, self.id))
        self.nmdm = self.get_nmdm()
        self.thread = gevent.spawn(self.run)
        self.console_thread = gevent.spawn(self.console_worker)

    def stop(self, force=False):
        self.logger.info('Stopping VM {0}'.format(self.name))

        if self.bhyve_process:
            try:
                if force:
                    self.bhyve_process.kill()
                else:
                    self.bhyve_process.terminate()
            except ProcessLookupError:
                self.logger.warning('bhyve process is already dead')

        self.thread.join()

        # Clear console
        gevent.kill(self.console_thread)
        for i in self.console_queues:
            i.put(b'\033[2J')

    def set_state(self, state):
        self.state = state
        self.changed()

    def changed(self):
        if self.management_lease and self.docker_host:
            self.network_ready.set()

        self.context.client.emit_event('vm.changed', {
            'operation': 'update',
            'ids': [self.id]
        })

    def run(self):
        while not self.exiting:
            self.set_state(VirtualMachineState.BOOTLOADER)
            self.context.vm_started.set()
            self.logger.debug('Starting bootloader...')

            if self.config['bootloader'] == 'GRUB':
                with tempfile.NamedTemporaryFile('w+', delete=False) as devmap:
                    hdcounter = 0
                    cdcounter = 0
                    bootname = ''
                    bootswitch = '-r'

                    for i in filter(lambda i: i['type'] in ('DISK', 'CDROM'), self.devices):
                        path = self.context.client.call_sync('vm.get_disk_path', self.id, i['name'])

                        if i['type'] == 'DISK':
                            name = 'hd{0}'.format(hdcounter)
                            hdcounter += 1

                        elif i['type'] == 'CDROM':
                            name = 'cd{0}'.format(cdcounter)
                            cdcounter += 1

                        print('({0}) {1}'.format(name, path), file=devmap)
                        if 'boot_device' in self.config:
                            if i['name'] == self.config['boot_device']:
                                bootname = name

                    if self.config.get('boot_partition'):
                        bootname += ',{0}'.format(self.config['boot_partition'])

                    if self.config.get('boot_directory'):
                        bootswitch = '-d'
                        bootname = os.path.join(self.files_root, self.config['boot_directory'])

                    devmap.flush()
                    self.bhyve_process = subprocess.Popen(
                        [
                            '/usr/local/sbin/grub-bhyve', '-M', str(self.config['memsize']),
                            bootswitch, bootname, '-m', devmap.name, '-c', self.nmdm[0], self.name
                        ],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        close_fds=True
                    )

            if self.config['bootloader'] == 'BHYVELOAD':
                path = self.context.client.call_sync('vm.get_disk_path', self.id, self.config['boot_device'])
                self.bhyve_process = subprocess.Popen(
                    [
                        '/usr/sbin/bhyveload', '-c', self.nmdm[0], '-m', str(self.config['memsize']),
                        '-d', path, self.name,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True
                )

            if self.config['bootloader'] not in ['UEFI', 'UEFI_CSM']:
                out, err = self.bhyve_process.communicate()
                self.bhyve_process.wait()
                self.logger.debug('bhyveload: {0}'.format(out))

            self.logger.debug('Starting bhyve...')
            args = self.build_args()
            self.set_state(VirtualMachineState.RUNNING)

            self.bhyve_process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
            out, err = self.bhyve_process.communicate()
            self.logger.debug('bhyve: {0}'.format(out))
            self.bhyve_process.wait()

            subprocess.call(['/usr/sbin/bhyvectl', '--destroy', '--vm={0}'.format(self.name)])
            if self.bhyve_process.returncode == 0:
                continue

            break

        for i in self.tap_interfaces:
            self.cleanup_tap(i)

        self.cleanup_vnc()
        self.set_state(VirtualMachineState.STOPPED)

    def console_worker(self):
        self.logger.debug('Opening console at {0}'.format(self.nmdm[1]))
        self.console_fd = serial.Serial(self.nmdm[1], 115200)
        while True:
            try:
                fd = self.console_fd.fileno()
                r, w, x = select.select([fd], [], [])
                if fd not in r:
                    continue

                ch = self.console_fd.read(self.console_fd.inWaiting())
            except serial.SerialException as e:
                print('Cannot read from serial port: {0}'.format(str(e)))
                gevent.sleep(1)
                self.console_fd = serial.Serial(self.nmdm[1], 115200)
                continue

            self.scrollback.push(ch)
            try:
                for i in self.console_queues:
                    i.put(ch, block=False)
            except:
                pass

    def console_register(self):
        queue = gevent.queue.Queue(4096)
        self.console_queues.append(queue)
        return queue

    def console_unregister(self, queue):
        self.console_queues.remove(queue)

    def console_write(self, data):
        try:
            self.console_fd.write(data)
            self.console_fd.flush()
        except (ValueError, OSError):
            pass


class Jail(object):
    def __init__(self):
        self.id = None
        self.jid = None
        self.name = None

    def start(self):
        pass

    def stop(self):
        pass


class DockerHost(object):
    def __init__(self, context):
        self.context = context
        self.vm = None
        self.state = DockerHostState.DOWN
        self.connection = None
        self.listener = None
        self.logger = logging.getLogger(self.__class__.__name__)
        gevent.spawn(self.wait_ready)

    def wait_ready(self):
        self.vm.network_ready.wait()
        ip = self.vm.management_lease.lease.client_ip
        self.logger.info('Docker instance at {0} ({1}) is ready'.format(self.vm.name, ip))
        self.connection = docker.Client(base_url='http://{0}:2375'.format(ip))
        self.listener = gevent.spawn(self.listen)

    def listen(self):
        self.logger.debug('Listening for docker events on {0}'.format(self.vm.name))
        actions = {
            'create': 'create',
            'destroy': 'delete'
        }

        while True:
            try:
                for ev in self.connection.events(decode=True):
                    self.logger.debug('Received docker event: {0}'.format(ev))
                    if ev['Type'] == 'container':
                        details = self.connection.inspect_container(ev['id'])
                        self.context.client.emit_event('docker.container.changed', {
                            'operation': actions.get(ev['Action'], 'update'),
                            'ids': [ev['id']]
                        })

                        if 'org.freenas.expose_ports_at_host' not in details['Config']['Labels']:
                            continue

                        self.logger.debug('Redirecting container {0} ports on host firewall'.format(ev['id']))

                        # Setup or destroy port redirection now, if needed
                        if ev['Action'] == 'start':
                            for i in get_docker_ports(details):
                                p = pf.PF()
                                rule = pf.Rule()
                                rule.dst.port_range = [i['host_port'], 0]
                                rule.dst.port_op = pf.RuleOperator.EQ
                                rule.action = pf.RuleAction.RDR
                                rule.af = socket.AF_INET
                                rule.ifname = self.context.default_if
                                rule.natpass = True
                                rule.redirect_pool.append(pf.Address(
                                    address=self.vm.management_lease.lease.client_ip,
                                    netmask=ipaddress.ip_address('255.255.255.255')
                                ))
                                rule.proxy_ports = [i['host_port'], 0]
                                p.append_rule('rdr', rule)

                        if ev['Action'] == 'destroy':
                            self.logger.debug(json.dumps(details, indent=4))

                    if ev['Type'] == 'image':
                        self.context.client.emit_event('docker.image.changed', {
                            'operation': actions.get(ev['Action'], 'update'),
                            'ids': [ev['id']]
                        })

            except BaseException as err:
                self.logger.info('Docker connection closed: {0}, retrying in 1 second'.format(str(err)))
                time.sleep(1)


class ManagementService(RpcService):
    def __init__(self, context):
        super(ManagementService, self).__init__()
        self.context = context

    @private
    def get_status(self, id):
        vm = self.context.containers.get(id)
        if not vm:
            return {'state': 'STOPPED'}

        mgmt_lease = vm.management_lease
        nat_lease = vm.nat_lease

        return {
            'state': vm.state.name,
            'management_lease': mgmt_lease.lease if mgmt_lease else None,
            'nat_lease': nat_lease.lease if nat_lease else None
        }

    @private
    def start_vm(self, id):
        if not vtx_enabled:
            raise RpcException(errno.EINVAL, 'Cannot start VM - Intel VT-x instruction support not available.')

        container = self.context.datastore.get_by_id('vms', id)
        if not container:
            raise RpcException(errno.ENOENT, 'VM {0} not found'.format(id))

        if restricted_guest and container['config']['bootloader'] in ['UEFI', 'UEFI_CSM']:
            raise RpcException(
                errno.ENOENT,
                'Cannot start VM {0} - unrestricted guest support is needed to start UEFI VM'.format(id)
            )

        vm = VirtualMachine(self.context)
        vm.id = container['id']
        vm.name = container['name']
        vm.config = container['config']
        vm.devices = container['devices']
        vm.files_root = self.context.client.call_sync(
            'volume.get_dataset_path',
            os.path.join(container['target'], 'vm', container['name'], 'files')
        )
        vm.start()

        if vm.config.get('docker_host', False):
            host = DockerHost(self.context)
            host.vm = vm
            vm.docker_host = host
            self.context.docker_hosts[id] = host

        with self.context.cv:
            self.context.containers[id] = vm
            self.context.cv.notify_all()

    @private
    def stop_vm(self, id, force=False):
        container = self.context.datastore.get_by_id('vms', id)
        if not container:
            raise RpcException(errno.ENOENT, 'VM {0} not found'.format(id))

        self.context.logger.info('Stopping container {0} ({1})'.format(container['name'], id))

        vm = self.context.containers.get(id)
        if not vm:
            return

        if vm.state == VirtualMachineState.STOPPED:
            raise RpcException(errno.EACCES, 'Container {0} is already stopped'.format(container['name']))

        vm.stop(force)
        with self.context.cv:
            del self.context.containers[id]
            self.context.cv.notify_all()

    @private
    def request_console(self, id):
        container = self.context.datastore.get_by_id('vms', id)
        if not container:
            raise RpcException(errno.ENOENT, 'Container {0} not found'.format(id))

        token = generate_id()
        self.context.tokens[token] = id
        return token

    @private
    def request_webvnc_console(self, id):
        token = self.request_console(id)
        return 'http://{0}/containerd/webvnc/{1}'.format(socket.gethostname(), token)

    @private
    def get_mgmt_allocations(self):
        return [i.__getstate__() for i in self.context.mgmt.allocations.values()]


class DockerService(RpcService):
    def __init__(self, context):
        super(DockerService, self).__init__()
        self.context = context

    def get_host_status(self, id):
        host = self.context.docker_hosts.get(id)
        if not host:
            raise RpcException(errno.ENOENT, 'Docker host {0} not found'.format(id))

        try:
            info = host.connection.info()
            return {
                'os': info['OperatingSystem'],
                'hostname': info['Name'],
                'unique_id': info['ID'],
                'mem_total': info['MemTotal']
            }
        except:
            raise RpcException(errno.ENXIO, 'Cannot connect to host {0}'.format(id))

    def query_containers(self):
        result = []

        def normalize_names(names):
            for i in names:
                if i[0] == '/':
                    yield i[1:]

                yield i

        for host in self.context.docker_hosts.values():
            for container in host.connection.containers(all=True):
                details = host.connection.inspect_container(container['Id'])
                result.append({
                    'id': container['Id'],
                    'image': container['Image'],
                    'names': list(normalize_names(container['Names'])),
                    'command': container['Command'],
                    'status': container['Status'],
                    'host': host.vm.id,
                    'ports': list(get_docker_ports(details)),
                    'expose_ports': 'org.freenas.expose_ports_at_host' in details['Config']['Labels']
                })

        return result

    def query_images(self):
        result = []
        for host in self.context.docker_hosts.values():
            for image in host.connection.images():
                result.append({
                    'id': image['Id'],
                    'names': image['RepoTags'],
                    'size': image['VirtualSize'],
                    'host': host.vm.name
                })

        return result

    @generator
    def pull(self, name, host):
        host = self.context.docker_hosts.get(host)
        if not host:
            raise RpcException(errno.ENOENT, 'Docker host {0} not found'.format(host))

        for line in host.connection.pull(name, stream=True):
            yield json.loads(line.decode('utf-8'))

    def start(self, id):
        host = self.context.docker_host_by_container_id(id)
        try:
            host.connection.start(container=id)
        except BaseException as err:
            raise RpcException(errno.EFAULT, 'Failed to start container: {0}'.format(str(err)))

    def stop(self, id):
        host = self.context.docker_host_by_container_id(id)
        try:
            host.connection.stop(container=id)
        except BaseException as err:
            raise RpcException(errno.EFAULT, 'Failed to stop container: {0}'.format(str(err)))

    def create(self, container):
        labels = []
        host = self.context.docker_hosts.get(container['host'])
        if not host:
            raise RpcException(errno.ENOENT, 'Docker host {0} not found'.format(container['host']))

        if container['expose_ports']:
            labels.append('org.freenas.expose_ports_at_host')

        try:
            host.connection.create_container(
                name=container['name'],
                image=container['image'],
                ports=[i['container_port'] for i in container['ports']],
                volumes=[i['container_path'] for i in container['volumes']],
                labels=labels,
                host_config=host.connection.create_host_config(
                    port_bindings={i['container_port']: i['host_port'] for i in container['ports']},
                    binds={
                        i['container_path']: {
                            'bind': i['host_path'].replace('/mnt', '/host'),
                            'mode': 'ro' if i['readonly'] else 'rw'
                        } for i in container['ports']
                    },
                )
            )
        except BaseException as err:
            raise RpcException(errno.EFAULT, str(err))

    def delete(self, id):
        host = self.context.docker_host_by_container_id(id)
        try:
            host.connection.remove_container(container=id, force=True)
        except BaseException as err:
            raise RpcException(errno.EFAULT, 'Failed to remove container: {0}'.format(str(err)))


class ServerResource(Resource):
    def __init__(self, apps=None, context=None):
        super(ServerResource, self).__init__(apps)
        self.context = context

    def __call__(self, environ, start_response):
        environ = environ
        current_app = self._app_by_path(environ['PATH_INFO'], 'wsgi.websocket' in environ)

        if current_app is None:
            raise Exception("No apps defined")

        if 'wsgi.websocket' in environ:
            ws = environ['wsgi.websocket']
            current_app = current_app(ws, self.context)
            current_app.ws = ws  # TODO: needed?
            current_app.handle()

            return None
        else:
            return current_app(environ, start_response)


class ConsoleConnection(WebSocketApplication, EventEmitter):
    BUFSIZE = 1024

    def __init__(self, ws, context):
        super(ConsoleConnection, self).__init__(ws)
        self.context = context
        self.logger = logging.getLogger('ConsoleConnection')
        self.authenticated = False
        self.console_queue = None
        self.vm = None
        self.rd = None
        self.wr = None
        self.inq = Queue()

    def worker(self):
        self.logger.info('Opening console to %s...', self.vm.name)

        def read_worker():
            while True:
                data = self.console_queue.get()
                if data is None:
                    return

                try:
                    self.ws.send(data.replace(b'\n\n', b'\r\n'))
                except WebSocketError as err:
                    self.logger.info('WebSocket connection terminated: {0}'.format(str(err)))
                    return

        def write_worker():
            for i in self.inq:
                self.vm.console_write(i)

        self.wr = gevent.spawn(write_worker)
        self.rd = gevent.spawn(read_worker)
        gevent.joinall([self.rd, self.wr])

    def on_open(self, *args, **kwargs):
        pass

    def on_close(self, *args, **kwargs):
        gevent.kill(self.rd)
        gevent.kill(self.wr)
        self.vm.console_unregister(self.console_queue)

    def on_message(self, message, *args, **kwargs):
        if message is None:
            return

        if not self.authenticated:
            message = json.loads(message.decode('utf8'))

            if type(message) is not dict:
                return

            if 'token' not in message:
                return

            cid = self.context.tokens.get(message['token'])
            if not cid:
                self.ws.send(json.dumps({'status': 'failed'}))
                return

            self.authenticated = True

            with self.context.cv:
                if not self.context.cv.wait_for(lambda: cid in self.context.containers, timeout=30):
                    return
                self.vm = self.context.containers[cid]

            self.console_queue = self.vm.console_register()
            self.ws.send(json.dumps({'status': 'ok'}))
            self.ws.send(self.vm.scrollback.read())
            gevent.spawn(self.worker)
            return

        for i in message:
            i = bytes([i])
            if i == '\r':
                i = '\n'
            self.inq.put(i)


class VncConnection(WebSocketApplication, EventEmitter):
    def __init__(self, ws, context):
        super(VncConnection, self).__init__(ws)
        self.context = context
        self.logger = logging.getLogger('VncConnection')
        self.cfd = None
        self.vm = None

    @classmethod
    def protocol_name(cls):
        return 'binary'

    def on_open(self, *args, **kwargs):
        qs = dict(urllib.parse.parse_qsl(self.ws.environ['QUERY_STRING']))
        token = qs.get('token')
        if not token:
            self.ws.close()
            return

        cid = self.context.tokens.get(token)
        if not cid:
            self.logger.warn('Invalid token {0}, closing connection'.format(token))
            self.ws.close()
            return

        def read():
            buffer = bytearray(4096)
            while True:
                n = self.cfd.recv_into(buffer)
                if n == 0:
                    self.ws.close()
                    return

                self.ws.send(buffer[:n])

        self.vm = self.context.containers[cid]
        self.logger.info('Opening VNC console to {0} (token {1})'.format(self.vm.name, token))

        self.cfd = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        self.cfd.connect(self.vm.vnc_socket)
        gevent.spawn(read)

    def on_message(self, message, *args, **kwargs):
        if message is None:
            self.ws.close()
            return

        self.cfd.send(message)

    def on_close(self, *args, **kwargs):
        self.cfd.shutdown(socket.SHUT_RDWR)


class Main(object):
    def __init__(self):
        self.client = None
        self.datastore = None
        self.configstore = None
        self.config = None
        self.mgmt = None
        self.nat = None
        self.vm_started = Event()
        self.containers = {}
        self.docker_hosts = {}
        self.tokens = {}
        self.logger = logging.getLogger('containerd')
        self.bridge_interface = None
        self.used_nmdms = []
        self.ec2 = None
        self.default_if = None
        self.proxy_server = ReverseProxyServer()
        self.cv = threading.Condition()

    def init_datastore(self):
        try:
            self.datastore = get_datastore(self.config)
        except DatastoreException as err:
            self.logger.error('Cannot initialize datastore: %s', str(err))
            sys.exit(1)

        self.configstore = ConfigStore(self.datastore)

    def allocate_nmdm(self):
        for i in range(0, 255):
            if i not in self.used_nmdms:
                self.used_nmdms.append(i)
                return i

    def release_nmdm(self, index):
        self.used_nmdms.remove(index)

    def connect(self):
        while True:
            try:
                self.client.connect('unix:')
                self.client.login_service('containerd')
                self.client.enable_server()
                self.client.rpc.streaming_enabled = True
                self.client.register_service('containerd.management', ManagementService(self))
                self.client.register_service('containerd.docker', DockerService(self))
                self.client.register_service('containerd.debug', DebugService(gevent=True, builtins={"context": self}))
                self.client.resume_service('containerd.management')
                self.client.resume_service('containerd.docker')
                self.client.resume_service('containerd.debug')

                return
            except (OSError, RpcException) as err:
                self.logger.warning('Cannot connect to dispatcher: {0}, retrying in 1 second'.format(str(err)))
                time.sleep(1)

    def init_dispatcher(self):
        def on_error(reason, **kwargs):
            if reason in (ClientError.CONNECTION_CLOSED, ClientError.LOGOUT):
                self.logger.warning('Connection to dispatcher lost')
                self.connect()

        self.client = Client()
        self.client.on_error(on_error)
        self.connect()

    def init_mgmt(self):
        self.mgmt = ManagementNetwork(self, MGMT_INTERFACE, MGMT_ADDR)
        self.mgmt.up()
        self.mgmt.bridge_if.add_address(netif.InterfaceAddress(
            netif.AddressFamily.INET,
            ipaddress.ip_interface('169.254.169.254/32')
        ))

        self.nat = ManagementNetwork(self, NAT_INTERFACE, NAT_ADDR)
        self.nat.up()
        self.nat.bridge_if.add_address(netif.InterfaceAddress(
            netif.AddressFamily.INET,
            ipaddress.ip_interface('169.254.169.254/32')
        ))

    def init_nat(self):
        self.default_if = self.client.call_sync('networkd.configuration.get_default_interface')
        if not self.default_if:
            self.logger.warning('No default route interface; not configuring NAT')
            return

        p = pf.PF()

        for addr in (MGMT_ADDR, NAT_ADDR):
            # Try to find and remove existing NAT rules for the same subnet
            oldrule = first_or_default(
                lambda r: r.src.address.address == addr.network.network_address,
                p.get_rules('nat')
            )

            if oldrule:
                p.delete_rule('nat', oldrule.index)

            rule = pf.Rule()
            rule.src.address.address = addr.network.network_address
            rule.src.address.netmask = addr.netmask
            rule.action = pf.RuleAction.NAT
            rule.af = socket.AF_INET
            rule.ifname = self.default_if
            rule.redirect_pool.append(pf.Address(ifname=self.default_if))
            rule.proxy_ports = [50001, 65535]
            p.append_rule('nat', rule)

        try:
            p.enable()
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise err

        # Last, but not least, enable IP forwarding in kernel
        try:
            sysctl.sysctlbyname('net.inet.ip.forwarding', 1)
        except OSError as err:
            raise err

    def init_dhcp(self):
        pass

    def init_ec2(self):
        self.ec2 = EC2MetadataServer(self)
        self.ec2.start()

    def vm_by_mgmt_mac(self, mac):
        for i in self.containers.values():
            for tapmac in i.tap_interfaces.values():
                if tapmac == mac:
                    return i

        return None

    def vm_by_mgmt_ip(self, ip):
        for i in self.mgmt.allocations.values():
            if i.lease.client_ip == ip:
                return i.vm()

    def docker_host_by_container_id(self, id):
        for host in self.docker_hosts.values():
            try:
                if host.connection.containers(quiet=True, filters={'id': id}):
                    return host
            except:
                pass

            continue

        raise RpcException(errno.ENOENT, 'Container {0} not found'.format(id))

    def die(self):
        self.logger.warning('Exiting')
        for i in self.containers.values():
            i.stop(True)

        self.client.disconnect()
        sys.exit(0)

    def dispatcher_error(self, error):
        self.die()

    def init_autostart(self):
        for vm in self.client.call_sync('vm.query'):
            if vm['config'].get('autostart'):
                self.client.submit_task('vm.start', vm['id'])

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CONFIGFILE, help='Middleware config file')
        parser.add_argument('-p', type=int, metavar='PORT', default=5500, help="WebSockets server port")
        args = parser.parse_args()
        configure_logging('/var/log/containerd.log', 'DEBUG')
        setproctitle.setproctitle('containerd')

        gevent.signal(signal.SIGTERM, self.die)
        gevent.signal(signal.SIGQUIT, self.die)

        # Load pf kernel module
        try:
            kld.kldload('/boot/kernel/pf.ko')
        except OSError as err:
            if err.errno != errno.EEXIST:
                self.logger.error('Cannot load PF module: %s', str(err))
                self.logger.error('NAT unavailable')

        global vtx_enabled, restricted_guest
        try:
            if sysctl.sysctlbyname('hw.vmm.vmx.initialized'):
                vtx_enabled = True
            if sysctl.sysctlbyname('hw.vmm.vmx.cap.unrestricted_guest'):
                restricted_guest = False
        except OSError:
            pass

        os.makedirs('/var/run/containerd', exist_ok=True)

        self.config = args.c
        self.init_datastore()
        self.init_dispatcher()
        self.init_mgmt()
        self.init_nat()
        self.init_ec2()
        self.init_autostart()
        self.logger.info('Started')

        # WebSockets server
        kwargs = {}
        s4 = WebSocketServer(('', args.p), ServerResource({
            '/console': ConsoleConnection,
            '/vnc': VncConnection,
            '/webvnc/[\w]+': app
        }, context=self), **kwargs)

        s6 = WebSocketServer(('::', args.p), ServerResource({
            '/console': ConsoleConnection,
            '/vnc': VncConnection,
            '/webvnc/[\w]+': app
        }, context=self), **kwargs)

        serv_threads = [gevent.spawn(s4.serve_forever), gevent.spawn(s6.serve_forever)]
        gevent.joinall(serv_threads)


if __name__ == '__main__':
    m = Main()
    m.main()
