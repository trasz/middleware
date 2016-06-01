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
CERT_DIR = '/etc/certificates'


def write_dh_parameters(dh):
    with open('{0}/dh.pem'.format(OPENVPN_DIR), 'w') as f:
        f.write(dh)


def write_ta_key(ta_key):
    with open('{0}/ta.key'.format(OPENVPN_DIR), 'w') as f:
        f.write(ta_key)


def convert_cert_by_id(context, cert_id, cert_type):
    cert_data = context.datastore.get_by_id('crypto.certificates', cert_id)	
   
    if cert_type == 'ca':
        return "{0}/CA/{1}.crt".format(CERT_DIR, cert_data['name'])
   
    elif cert_type == 'key':
        return "{0}/{1}.key".format(CERT_DIR, cert_data['name']) 

    else:   
        return "{0}/{1}.crt".format(CERT_DIR, cert_data['name'])


def process_config(context, conf):

    processed = {}

    for k, v in conf.items():
        if v == None:
            continue
  
        if v == False:
            continue

        if k in ['cert', 'key', 'ca']:
            processed[k] = convert_cert_by_id(context, v, k)
            continue

        if k == 'enable':
            continue

        if k == 'dh':
            write_dh_parameters(v)
            processed[k] = 'dh.pem'
            continue
       
        if k == 'dev':
            processed[k] = v + str(0)
  
        if k == 'auxiliary':
            processed[v] = ''

        if k == 'tls-auth': 
            write_ta_key(v)
            processed[k] = 'ta.key 0'
            continue
                    
        if isinstance(v, list):
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
    openvpn_conf = process_config(context, openvpn_conf)

    with open('{0}/openvpn.conf'.format(OPENVPN_DIR), 'w') as f:
        for k, v in openvpn_conf.items():
             f.write('{0} {1}\n'.format(k, v))     
	
    context.emit_event('etcd.file_generated', {'filename': '{0}/openvpn.conf'.format(OPENVPN_DIR)})

