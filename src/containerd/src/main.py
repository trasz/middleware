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
import gevent
import gevent.os
import gevent.monkey
import subprocess
import serial
import netif
import signal
import select
import tempfile
import ipaddress
from gevent.queue import Queue, Channel
from gevent.event import Event
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource
from geventwebsocket.exceptions import WebSocketError
from pyee import EventEmitter
from datastore import DatastoreException, get_datastore
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException, private
from freenas.utils.debug import DebugService
from freenas.utils import configure_logging
from mgmt import ManagementNetwork
from ec2 import EC2MetadataServer


gevent.monkey.patch_all(thread=False)


MGMT_ADDR = ipaddress.ip_interface('172.20.0.1/16')
MGMT_INTERFACE = 'mgmt0'
NAT_ADDR = ipaddress.ip_interface('172.21.0.1/16')
NAT_INTERFACE = 'nat0'
DEFAULT_CONFIGFILE = '/usr/local/etc/middleware.conf'
SCROLLBACK_SIZE = 65536


class VirtualMachineState(enum.Enum):
    STOPPED = 1
    BOOTLOADER = 2
    RUNNING = 3


class BinaryRingBuffer(object):
    def __init__(self, size):
        self.data = bytearray(size)

    def push(self, data):
        #del self.data[0:len(data)]
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
        self.thread = None
        self.exiting = False
        self.logger = logging.getLogger('VM:{0}'.format(self.name))

    def build_args(self):
        args = [
            '/usr/sbin/bhyve', '-A', '-H', '-P', '-c', str(self.config['ncpus']), '-m', str(self.config['memsize']),
            '-s', '0:0,hostbridge'
        ]

        index = 1

        for i in self.devices:
            if i['type'] == 'DISK':
                path = self.context.client.call_sync('container.get_disk_path', self.id, i['name'])
                args += ['-s', '{0}:0,ahci-hd,{1}'.format(index, path)]
                index += 1

            if i['type'] == 'CDROM':
                path = self.context.client.call_sync('container.get_disk_path', self.id, i['name'])
                args += ['-s', '{0}:0,ahci-cd,{1}'.format(index, path)]
                index += 1

            if i['type'] == 'VOLUME':
                if i['properties']['type'] == 'VT9P':
                    path = self.context.client.call_sync('container.get_volume_path', self.id, i['name'])
                    args += ['-s', '{0}:0,virtio-9p,{1}={2}'.format(index, i['name'], path)]
                    index += 1

            if i['type'] == 'NIC':
                mac = i['properties']['link_address']
                iface = self.init_tap(i['name'], i['properties'], mac)
                if not iface:
                    continue

                args += ['-s', '{0}:0,virtio-net,{1},mac={2}'.format(index, iface, mac)]
                index += 1

        args += [
            '-s', '31,lpc', '-l', 'com1,{0}'.format(self.nmdm[0]),
            self.name
        ]

        self.logger.debug('bhyve args: {0}'.format(args))
        return args

    def init_tap(self, name, nic, mac):
        try:
            iface = netif.get_interface(netif.create_interface('tap'))
            iface.description = 'vm:{0}:{1}'.format(self.name, name)
            iface.up()

            if nic['type'] == 'BRIDGE':
                if nic['bridge']:
                        bridge = netif.get_interface(nic['bridge'])
                        bridge.add_member(iface.name)

            if nic['type'] == 'MANAGEMENT':
                mgmt = netif.get_interface('mgmt0', force_type='bridge')
                mgmt.add_member(iface.name)

            if nic['type'] == 'NAT':
                mgmt = netif.get_interface('nat0', force_type='bridge')
                mgmt.add_member(iface.name)

            self.tap_interfaces[iface] = mac
            return iface.name
        except (KeyError, OSError) as err:
            self.logger.warning('Cannot initialize NIC {0}: {1}'.format(name, str(err)))
            return

    def cleanup_tap(self, iface):
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
        self.logger.info('Stopping container {0} ({1})'.format(self.name, self.id))
        if self.state == VirtualMachineState.STOPPED:
            raise RuntimeError()

        for i in self.tap_interfaces:
            self.cleanup_tap(i)

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
        self.context.client.emit_event('container.changed', {
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
                        path = self.context.client.call_sync('container.get_disk_path', self.id, i['name'])

                        if i['type'] == 'DISK':
                            name = 'hd{0}'.format(hdcounter)
                            hdcounter += 1

                        elif i['type'] == 'CDROM':
                            name = 'cd{0}'.format(cdcounter)
                            cdcounter += 1

                        print('({0}) {1}'.format(name, path), file=devmap)
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
                path = self.context.client.call_sync('container.get_disk_path', self.id, self.config['boot_device'])
                self.bhyve_process = subprocess.Popen(
                    [
                        '/usr/sbin/bhyveload', '-c', self.nmdm[0], '-m', str(self.config['memsize']),
                        '-d', path, self.name,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True
                )

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


class Docker(object):
    pass


class ManagementService(RpcService):
    def __init__(self, context):
        super(ManagementService, self).__init__()
        self.context = context

    @private
    def get_status(self, id):
        vm = self.context.containers.get(id)
        if not vm:
            return {'state': 'STOPPED'}

        return {
            'state': vm.state.name
        }

    @private
    def start_container(self, id):
        container = self.context.datastore.get_by_id('containers', id)

        if container['type'] == 'VM':
            vm = VirtualMachine(self.context)
            vm.id = container['id']
            vm.name = container['name']
            vm.config = container['config']
            vm.devices = container['devices']
            vm.files_root = self.context.client.call_sync(
                'volume.get_dataset_path',
                container['target'],
                os.path.join(container['target'], 'vm', container['name'], 'files')
            )
            vm.start()
            self.context.containers[id] = vm

    @private
    def stop_container(self, id, force=False):
        container = self.context.datastore.get_by_id('containers', id)
        self.context.logger.info('Stopping container {0} ({1})'.format(container['name'], id))

        if container['type'] == 'VM':
            vm = self.context.containers.get(id)
            if not vm:
                raise RpcException(errno.ENOENT, 'Container {0} is not started'.format(container['name']))

            vm.stop(force)
            del self.context.containers[id]

    @private
    def request_console(self, id):
        container = self.context.datastore.get_by_id('containers', id)
        if not container:
            raise RpcException(errno.ENOENT, 'Container {0} not found'.format(id))

        token = self.context.generate_id()
        self.context.tokens[token] = id
        return token

    @private
    def get_mgmt_allocations(self):
        return [i.__getstate__() for i in self.context.mgmt.allocations.values()]


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


class Main(object):
    def __init__(self):
        self.client = None
        self.datastore = None
        self.configstore = None
        self.config = None
        self.mgmt = None
        self.vm_started = Event()
        self.containers = {}
        self.tokens = {}
        self.logger = logging.getLogger('containerd')
        self.bridge_interface = None
        self.used_nmdms = []

    def init_datastore(self):
        try:
            self.datastore = get_datastore(self.config)
        except DatastoreException as err:
            self.logger.error('Cannot initialize datastore: %s', str(err))
            sys.exit(1)

        self.configstore = ConfigStore(self.datastore)

    def init_bridge(self):
        self.bridge_interface = netif.get_interface(self.configstore.get('container.bridge'))

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
                self.client.register_service('containerd.management', ManagementService(self))
                self.client.register_service('containerd.debug', DebugService(gevent=True, builtins={"context": self}))
                self.client.resume_service('containerd.management')
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
        self.client.use_bursts = True
        self.client.on_error(on_error)
        self.connect()

    def init_mgmt(self):
        self.mgmt = ManagementNetwork(self, MGMT_INTERFACE, MGMT_ADDR)
        self.mgmt.up()
        self.mgmt.bridge_if.add_address(netif.InterfaceAddress(
            netif.AddressFamily.INET,
            ipaddress.ip_interface('169.254.169.254/32')
        ))

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

    def die(self):
        self.logger.warning('Exiting')
        for i in self.containers.values():
            i.stop(True)

        self.client.disconnect()
        sys.exit(0)

    def generate_id(self):
        return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(32)])

    def dispatcher_error(self, error):
        self.die()

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', metavar='CONFIG', default=DEFAULT_CONFIGFILE, help='Middleware config file')
        parser.add_argument('-p', type=int, metavar='PORT', default=5500, help="WebSockets server port")
        args = parser.parse_args()
        configure_logging('/var/log/containerd.log', 'DEBUG')
        setproctitle.setproctitle('containerd')

        gevent.signal(signal.SIGTERM, self.die)
        gevent.signal(signal.SIGQUIT, self.die)

        self.config = args.c
        self.init_datastore()
        self.init_dispatcher()
        self.init_mgmt()
        self.init_ec2()
        self.init_bridge()
        self.logger.info('Started')

        # WebSockets server
        kwargs = {}
        s4 = WebSocketServer(('', args.p), ServerResource({
            '/console': ConsoleConnection,
        }, context=self), **kwargs)

        s6 = WebSocketServer(('::', args.p), ServerResource({
            '/console': ConsoleConnection,
        }, context=self), **kwargs)

        serv_threads = [gevent.spawn(s4.serve_forever), gevent.spawn(s6.serve_forever)]
        gevent.joinall(serv_threads)


if __name__ == '__main__':
    m = Main()
    m.main()
