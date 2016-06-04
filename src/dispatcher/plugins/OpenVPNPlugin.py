#
# Copyright 2016 iXsystems, Inc.
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions # are met:
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

import re
import netif
import errno
import logging
import ipaddress

from datastore.config import ConfigNode
from task import Task, Provider, VerifyException, TaskException, TaskDescription
from lib.system import system, SubprocessException
from freenas.dispatcher.rpc import RpcException, SchemaHelper as h, accepts, returns, description


logger = logging.getLogger(__name__)


@description('Provides OpenVPN service configuration')
class OpenVpnProvider(Provider):
    ''' I think that some of the information here needs to be excluded using exclude() 
        I need consultaion on that.        
    '''
    @returns(h.ref('service-openvpn'))
    def get_config(self):
        return ConfigNode('service.openvpn', self.configstore).__getstate__()


@description('Provides corresponding OpenVPN client configuration')
class OpenVPNClientConfigProvider(Provider):
    '''This provider is responsible for returning corresponding client configuration.
       provide_tls_auth_key retruns generated static key which needs to by copied to
       the client OpenVPN directory with 'ta.key' file name. 
    '''
    @returns(str)
    def provide_config(self):
        try:
            with open('/usr/local/etc/openvpn/openvpn_client', 'r') as f:
                return f.read()

        except FileNotFoundError:
            raise RpcException(errno.ENOENT, 'Client config file not available. '
                               'Please configure OpenVPN server first.')

    @returns(str)
    def provide_tls_auth_key(self):
        node = ConfigNode('service.openvpn', self.configstore).__getstate__()	
        return node['tls_auth']


@description('Creates OpenVPN config file')
@accepts(
    h.all_of(
        h.ref('service-openvpn'),
        h.required('dev', 'ca', 'cert', 'key', 'cipher', 'port', 'proto')
    ))
class OpenVpnConfigureTask(Task):
    @classmethod
    def early_describe(cls):
        return 'Configuring OpenVPN service'

    def describe(self, openvpn):
        return TaskDescription('Configuring OpenVPN service')

    def verify(self, openvpn):
        interface_pattern = '(tap|tun)[0-9]'
        node = ConfigNode('service.openvpn', self.configstore).__getstate__()
        node.update(openvpn)
        
        if not re.search(interface_pattern, node['dev']):
            raise VerifyException(errno.EINVAL,
                                  '{0} Bad interface name. Allowed values tap/tun[0-9].'.format(node['dev']))
	
        if node['server_bridge_extended']:
            try:
                bridge_ip = ipaddress.ip_address(node['server_bridge_ip'])
                netmask = node['server_bridge_netmask']									
                ip_range_begin = ipaddress.ip_address(node['server_bridge_range_begin'])
                ip_range_end = ipaddress.ip_address(node['server_bridge_range_end'])
                subnet = ipaddress.ip_network('{0}/{1}'.format(bridge_ip, netmask), strict=False) 
      
            except ValueError as e:
                raise VerifyException(errno.EINVAL, str(e))

            if (ip_range_begin not in subnet) or (ip_range_end not in subnet):
                raise VerifyException(errno.EINVAL, 
                                      'Provided range of remote client IP adresses is invalid.')			
			
            if (bridge_ip >= ip_range_begin) and (bridge_ip <= ip_range_end):
                raise VerifyException(errno.EINVAL, 
                                      'Provided bridge IP address is in the client ip range.')

        if (node['keepalive_ping_interval'] * 2) >= node['keepalive_peer_down']:
            raise VerifyException(errno.EINVAL, 'The second parameter to keepalive must be'
                                  'at least twice the value of the first parameter.'
                                  'Recommended setting is keepalive 10 60.')
				
        return ['system']


    def run(self, openvpn):
        node = ConfigNode('service.openvpn', self.configstore).__getstate__()
        node.update(openvpn)
        ca_cert = self.datastore.get_by_id('crypto.certificates', node['ca'])
        if not ca_cert:
            raise TaskException(errno.EINVAL,
                                'Provided CA certificate does not exist in config database.')

        server_private_key = self.datastore.get_by_id('crypto.certificates', node['key'])
        if not server_private_key:
            raise TaskException(errno.EINVAL,
                                'Provided private key does not exist in config database.')

        server_certyficate = self.datastore.get_by_id('crypto.certificates', node['cert'])
        if not server_certyficate:
            raise TaskException(errno.EINVAL,
                                'Provided certificate does not exist in config database.')

        openvpn_user = self.datastore.exists('users', ('username', '=', node['user']))
        if not openvpn_user:
            raise TaskException(errno.EINVAL, 'Provided user does not exist.')

        openvpn_group = self.datastore.exists('groups', ('name', '=', node['group']))
        if not openvpn_group:
            raise TaskException(errno.EINVAL, 'Provided user does not exist.')

        try:
            node = ConfigNode('service.openvpn', self.configstore)
            node.update(openvpn)
			
            self.dispatcher.call_sync('etcd.generation.generate_group', 'openvpn')
            self.dispatcher.dispatch_event('service.openvpn.changed', {
                'operation': 'update',
                'ids': None,
            })

        except RpcException as e:
            raise TaskException(errno.ENXIO,
                                'Cannot reconfigure OpenVPN: {0}'.format(str(e)))

        return 'RESTART'


@accepts(str, h.any_of(int, None))
@description('Generates Diffie-Hellman parameters and tls-auth key file')
class OpenVPNGenerateKeys(Task):
    ''' To get it working properly you need to set appropriate timeout on dipatcher client.
        Generation of 2048 bit dh parameters can take a long time.
        Maybe this task should be ProgressTask type? - need consultation on that 
    '''
    @classmethod
    def early_describe(cls):
        return 'Generating OpenVPN cryptographic parameters'

    def describe(self, key_type, key_length):
        return TaskDescription('Generating {key_type} OpenVPN cryptographic values', key_type=key_type)

    def verify(self, key_type, key_length):
        if key_type not in ['dh-parameters', 'tls-auth-key']:
            raise VerifyException(errno.EINVAL, 'Type dh-parameters or tls-auth-key')
		
        if key_length and key_type == 'dh-parameters':
            if key_length not in [1024, 2048]:	
                raise VerifyException(errno.EINVAL,
                                      'You have to chose between 1024 and 2048 bits.')

        return['system']

    def run(self, key_type, key_length):
        try:
            if key_type == 'dh-parameters':
                dhparams = system('/usr/bin/openssl', 'dhparam', str(key_length))[0]
			
                self.configstore.set('service.openvpn.dh', dhparams)
                self.dispatcher.call_sync('etcd.generation.generate_group', 'openvpn')
                self.dispatcher.dispatch_event('service.openvpn.changed', {
                    'operation': 'update',
                    'ids': None,
                 })

            else:
                tls_auth_key = system('/usr/local/sbin/openvpn', '--genkey', '--secret', '/dev/stdout')[0]
					
                self.configstore.set('service.openvpn.tls_auth',tls_auth_key)
                self.dispatcher.call_sync('etcd.generation.generate_group', 'openvpn')
                self.dispatcher.dispatch_event('service.openvpn.changed', {
                    'operation': 'update',
                    'ids': None,
                 })

        except RpcException as e:
            raise TaskException(errno.ENXIO,
                                'Cannot reconfigure OpenVPN: {0}'.format(str(e)))
            
        except SubprocessException as e:
            raise TaskException(errno.ENOENT,
                                'Cannont create requested key - check your system setup {0}'.format(e))



@accepts(bool)
@description("Bridges VPN tap interface to the main iterface provided by networkd")
class BridgeOpenVPNtoLocalNetwork(Task):
    ''' This is acctually all wrong. This interfere with containterd and should be managed by networkd.
        Look at it as a PoC of my approach. 
    '''
    @classmethod
    def early_describe(cls):
        return 'Bridging OpenVPN to main interface'

    def describe(self, enabled):
        return TaskDescription('Bridging OpenVPN to main interface')

    def verify(self, bridge_enable=False):
        vpn_interface = self.configstore.get('service.openvpn.dev')
        if bridge_enable:
            try:
                vpn_interface = netif.get_interface(vpn_interface)

            except KeyError:
                raise VerifyException(errno.EINVAL, 
                                      '{0} interface does not exist - Verify OpenVPN status'.format(vpn_interface))

            default_interface = self.dispatcher.call_sync('networkd.configuration.get_default_interface')
            if not default_interface:
                raise VerifyException(errno.EINVAL, 'No default interface configured. Verify network setup.')

        return['system']


    def run(self, bridge_enable=False):
        node = ConfigNode('service.openvpn', self.configstore).__getstate__()
        interface_list = list(netif.list_interfaces().keys())
        default_interface = self.dispatcher.call_sync('networkd.configuration.get_default_interface')

        vpn_interface = netif.get_interface(self.configstore.get('service.openvpn.dev'))	
        vpn_interface.up()

        if 'VPNbridge' not in interface_list:		
            bridge_interface = netif.get_interface(netif.create_interface('bridge'))
            bridge_interface.rename('VPNbridge')
            bridge_interface.description = 'OpenVPN bridge interface'

        else:
            bridge_interface = netif.get_interface('VPNbridge')

	
        if node['server_bridge_extended']:
            subnet = ipaddress.ip_interface('{0}/{1}'.format(node['server_bridge_ip'], bridge_values['server_bridge_netmask']))
            bridge_interface.add_address(netif.InterfaceAddress(
            netif.AddressFamily.INET,
            subnet
            ))

        try:
            bridge_interface.add_member(default_interface)
        
        except FileExistsError as e:
            logger.info('Default interface already bridged: {0}'.format(e))

        except OSError as e:
            raise TaskException(errno.EBUSY, 
                                'Default interface busy - check other bridge interfaces. {0}'.format(e))

        try:
            bridge_interface.add_member(self.configstore.get('service.openvpn.dev'))

        except FileExistsError as e:
            logger.info('OpenVPN interface already bridged: {0}'.format(e))

        except OSError as e:
            raise TaskException(errno.EBUSY,
                                'VPN interface busy - check other bridge interfaces. {0}'.format(e))

        else:
            bridge_interface.up()


def _depends(): 
    return ['ServiceManagePlugin', 'CryptoPlugin'] 


def _init(dispatcher, plugin):
    '''Generation of 1024 bit dh-parameters shouldn't take to long...'''
    if not dispatcher.configstore.get('service.openvpn.dh'):
        try:
            dhparams = system('/usr/bin/openssl', 'dhparam', '1024')[0]

        except SubprocessException as e: 
            logger.warning('Cannot create initial dh parameters. Check openssl setup: {0}'.format(e))

        else:
            dispatcher.configstore.set('service.openvpn.dh', dhparams)


    plugin.register_schema_definition('service-openvpn', {
        'type': 'object',
        'properties': {
        'type': {'enum': ['service-openvpn']},
        'enable': {'type': 'boolean'},
        'dev': {'type': 'string'},
        'persist_key': {'type': 'boolean'},
        'persist_tun' : {'type' : 'boolean'},
        'ca': {'type' : 'string'},
        'cert': {'type' : 'string'},
        'key': {'type' : 'string'},
        'dh': {'type' : 'string'},
        'tls_auth': {'type' : ['string', 'null']},
        'cipher': {'type' : 'string', 'enum':['BF-CBC', 'AES-128-CBC', 'DES-EDE3-CBC']},
        'server_bridge': {'type': 'boolean'},
        'server_bridge_extended':{'type': 'boolean'},
        'server_bridge_range_begin':{'type': 'string'},
        'server_bridge_range_end': {'type': 'string'},
        'server_bridge_netmask': {'type': 'string'},
        'server_bridge_ip': {'type': 'string', "format": "ip-address"},
        'max_clients': { 'type': 'integer'},
        'crl_verify': {'type': ['string', 'null']},
        'keepalive_ping_interval': {'type': 'integer'},
        'keepalive_peer_down': {'type': 'integer'},
        'user': {'type': 'string'},
        'group': {'type': 'string'},
        'port': {
                'type': 'integer',
                'minimum': 1,
                'maximum': 65535
                },

        'proto': {'type': 'string', 'enum':['tcp', 'udp']},
        'comp_lzo': {'type': 'boolean'},

        'verb': {
                'type': 'integer',
                'minimum': 0,
                'maximum': 15
                },
            'auxiliary': {'type': ['string', 'null']},
        },
        'additionalProperties': False,
		})
	
    plugin.register_provider("service.openvpn", OpenVpnProvider)
    plugin.register_provider("service.openvpn.client_config", OpenVPNClientConfigProvider)		

    plugin.register_task_handler('service.openvpn.update', OpenVpnConfigureTask)
    plugin.register_task_handler('service.openvpn.gen_key', OpenVPNGenerateKeys)
    plugin.register_task_handler('service.openvpn.bridge', BridgeOpenVPNtoLocalNetwork)
