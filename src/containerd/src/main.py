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

import enum
import os
import sys
import argparse
import json
import logging
import setproctitle
import errno
import io
import socket
import time
import string
import random
import gevent
import gevent.os
import gevent.monkey
import subprocess
import serial
import netif
from gevent.queue import Queue, Channel
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource
from geventwebsocket.exceptions import WebSocketError
from pyee import EventEmitter
from datastore import DatastoreException, get_datastore
from datastore.config import ConfigStore
from freenas.dispatcher.client import Client, ClientError
from freenas.dispatcher.rpc import RpcService, RpcException, private
from freenas.utils.debug import DebugService
from freenas.utils import configure_logging, to_timedelta


gevent.monkey.patch_all(thread=False)


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
        self.bhyve_process = None
        self.scrollback = BinaryRingBuffer(SCROLLBACK_SIZE)
        self.console_fd = None
        self.console_channel = Channel()
        self.console_thread = None
        self.tap_interfaces = []
        self.logger = logging.getLogger('VM:{0}'.format(self.name))

    def build_args(self):
        args = [
            '/usr/sbin/bhyve', '-A', '-H', '-P', '-c', str(self.config['ncpus']), '-m', str(self.config['memsize']),
            '-s', '0:0,hostbridge'
        ]

        index = 1

        if self.config['cdimage']:
            args += ['-s', '{0}:0,ahci-cd,{1}'.format(index, self.config['cdimage'])]
            index += 1

        for i in self.devices:
            if i['type'] == 'DISK':
                path = self.context.client.call_sync('container.get_disk_path', self.id, i['name'])
                args += ['-s', '{0}:0,ahci-hd,{1}'.format(index, path)]
                index += 1

            if i['type'] == 'NIC':
                iface = self.init_tap()
                args += ['-s', '{0}:0,virtio-net,{1}'.format(index, iface)]
                index += 1

        args += [
            '-s', '31,lpc', '-l', 'com1,{0}'.format(self.nmdm[0]),
            self.name
        ]

        self.logger.debug('bhyve args: {0}'.format(args))
        return args

    def init_tap(self):
        iface = netif.create_interface('tap')
        self.context.bridge_interface.add_member(iface)
        self.tap_interfaces.append(iface)
        return iface

    def get_nmdm(self):
        index = self.context.allocate_nmdm()
        return '/dev/nmdm{0}A'.format(index), '/dev/nmdm{0}B'.format(index)

    def start(self):
        self.nmdm = self.get_nmdm()
        gevent.spawn(self.run)
        self.console_thread = gevent.spawn(self.console_worker)

    def stop(self):
        if self.state == VirtualMachineState.STOPPED:
            raise RuntimeError()

        self.bhyve_process.terminate()
        subprocess.call(['/usr/sbin/bhyvectl', '--destroy', '--vm={0}'.format(self.name)])
        self.set_state(VirtualMachineState.STOPPED)

        # Clear console
        gevent.kill(self.console_thread)
        self.console_channel.put(b'\033[2J')

    def set_state(self, state):
        self.state = state
        self.context.client.emit_event('container.changed', {
            'operation': 'update',
            'ids': [self.id]
        })

    def run(self):
        self.set_state(VirtualMachineState.BOOTLOADER)
        self.logger.debug('Starting bootloader...')

        if self.config['bootloader'] == 'GRUB':
            self.bhyve_process = subprocess.Popen(
                ['/usr/local/lib/grub-bhyve']
            )

        if self.config['bootloader'] == 'BHYVELOAD':
            self.bhyve_process = subprocess.Popen(
                [
                    '/usr/sbin/bhyveload', '-c', self.nmdm[0], '-m', str(self.config['memsize']),
                    '-d', self.config['cdimage'], self.name,
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
        self.bhyve_process.wait()
        self.logger.debug('bhyve: {0}'.format(out))
        self.set_state(VirtualMachineState.STOPPED)

    def console_worker(self):
        self.logger.debug('Opening console at {0}'.format(self.nmdm[1]))
        self.console_fd = serial.Serial(self.nmdm[1], 115200)
        while True:
            try:
                ch = self.console_fd.read()
            except serial.SerialException as e:
                print('Cannot read from serial port: {0}'.format(str(e)))
                gevent.sleep(1)
                self.console_fd = serial.Serial(self.nmdm[1], 115200)
                continue

            self.scrollback.push(ch)
            try:
                self.console_channel.put(ch, block=False)
            except:
                pass

    def console_write(self, data):
        self.logger.debug('Write: {0}'.format(data))
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
        self.context.logger.info('Starting container {0} ({1})'.format(container['name'], id))

        if container['type'] == 'VM':
            vm = VirtualMachine(self.context)
            vm.id = container['id']
            vm.name = container['name']
            vm.config = container['config']
            vm.devices = container['devices']
            vm.start()
            self.context.containers[id] = vm

    @private
    def stop_container(self, id):
        container = self.context.datastore.get_by_id('containers', id)
        self.context.logger.info('Stopping container {0} ({1})'.format(container['name'], id))

        if container['type'] == 'VM':
            vm = self.context.containers[id]
            vm.stop()
            del self.context.containers[id]

    @private
    def request_console(self, id):
        container = self.context.datastore.get_by_id('containers', id)
        if not container:
            raise RpcException(errno.ENOENT, 'Container {0} not found'.format(id))

        token = self.context.generate_id()
        self.context.tokens[token] = id
        return token


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
        self.vm = None
        self.rd = None
        self.wr = None
        self.inq = Queue()

    def worker(self):
        self.logger.info('Opening console to %s...', self.vm.name)

        def read_worker():
            while True:
                data = self.vm.console_channel.get()
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

    def on_message(self, message, *args, **kwargs):
        if message is None:
            return

        if not self.authenticated:
            message = json.loads(message.decode('utf8'))

            if type(message) is not dict:
                return

            if 'token' not in message:
                return

            token = self.context.tokens.get(message['token'])
            if not token:
                self.ws.send(json.dumps({'status': 'failed'}))
                return

            self.authenticated = True
            self.vm = self.context.containers[token]
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
        self.containers = {}
        self.tokens = {}
        self.logger = logging.getLogger('containerd')
        self.bridge_interface = None
        self.used_nmdms = []

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

    def init_datastore(self):
        try:
            self.datastore = get_datastore(self.config['datastore']['driver'], self.config['datastore']['dsn'])
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
                self.client.connect('127.0.0.1')
                self.client.login_service('containerd')
                self.client.enable_server()
                self.client.register_service('containerd.management', ManagementService(self))
                self.client.register_service('containerd.debug', DebugService(gevent=True))
                self.client.resume_service('containerd.management')
                self.client.resume_service('containerd.debug')

                return
            except OSError as err:
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

    def die(self):
        self.logger.warning('Exiting')
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

        self.parse_config(args.c)
        self.init_datastore()
        self.init_dispatcher()
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
