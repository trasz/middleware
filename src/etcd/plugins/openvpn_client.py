#
# Copyright 2016 iXsystems, Inc.
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


OPENVPN_DIR = '/usr/local/etc/openvpn'

CONFIG_MESSAGE = '''
# This is the OpenVPN client configuration file generated automatically by FreeNas.
# Copy corresponding private key and certificate signed by {0} certificate authority.
# If you are using tls-auth static key you have to copy and paste displayed key
# to the OpenVPN config directory and name it as "ta.key".
# The "remote" directive should contain the ip address of device which provide port redirection
# to the FreeNas appliance configured as OpenVPN server.\n\n
'''
                

def process_config(context, conf):

    processed = {'nobind':'', 'remote':''}

    for k, v in conf.items():
        if v == None:
            continue

        if v == False:
            continue
       
        if k == 'ca':
            processed[k] = context.datastore.get_by_id('crypto.certificates', v)['name'] + '.crt'
            continue

        if k in ['cert', 'key']:
            processed[k] = ''
            continue

        if k in ['dh', 'enable', 'max-clients', 'crl-verify']:
            continue
     
        if k == 'auxiliary':
            continue

        if k == 'tls-auth': 
            processed[k] = 'ta.key 1'
            continue
                    
        if isinstance(v, list):
            if k == 'server-bridge':
                continue
            processed[k] = ' '.join(str(i) for i in v)
            continue

        if v == True:
            processed[k] = ''
            continue

        processed[k] = v

    return processed


def run(context):
    if not os.path.isdir(OPENVPN_DIR):
        os.mkdir(OPENVPN_DIR)

    openvpn_conf = context.client.call_sync('service.openvpn.get_config')
    openvpn_client_conf = process_config(context, openvpn_conf)

    with open('{0}/openvpn_client_config'.format(OPENVPN_DIR), 'w') as f:
        f.write(CONFIG_MESSAGE.format(openvpn_client_conf['ca'].split('.')[0]))

        for k, v in openvpn_client_conf.items():
             f.write('{0} {1}\n'.format(k, v))     

    context.emit_event('etcd.file_generated', {'filename': '{0}/openvpn_client_config'.format(OPENVPN_DIR)})

