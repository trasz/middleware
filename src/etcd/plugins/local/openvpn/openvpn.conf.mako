<%
    openvpn_conf = dispatcher.call_sync('service.openvpn.get_config')

    for cert in ['ca', 'key', 'cert']:
        cert_data = dispatcher.call_sync('crypto.certificate.query',
                                        [('id', '=', openvpn_conf[cert])], {'single': True})         
        if cert != 'key':
            openvpn_conf[cert] = cert_data['certificate_path'] 
        else:
            openvpn_conf[cert] = cert_data['privatekey_path'] 

    if openvpn_conf['server_bridge_extended']:
        processed = []

        processed.append(openvpn_conf['server_bridge_ip'])
        processed.append(openvpn_conf['server_bridge_netmask'])
        processed.append(openvpn_conf['server_bridge_range_begin'])
        processed.append(openvpn_conf['server_bridge_range_end'])

        openvpn_conf['server_bridge'] = ' '.join(processed)

    elif openvpn_conf['server_bridge']:
        openvpn_conf['server_bridge'] = ' '

%>\

dev ${openvpn_conf['dev']}
% if openvpn_conf['persist_key']:
persist-key
% endif
% if openvpn_conf['persist_tun']:
persist-tun
% endif
ca ${openvpn_conf['ca']}
cert ${openvpn_conf['cert']}
key ${openvpn_conf['key']}
dh /usr/local/etc/openvpn/dh.pem
% if openvpn_conf['tls_auth']:
tls-auth /usr/local/etc/openvpn/ta.key 0
% endif
cipher ${openvpn_conf['cipher']} 
% if openvpn_conf['server_bridge']:
server-bridge ${openvpn_conf['server_bridge']}
% endif
max-clients ${openvpn_conf['max_clients']}
user ${openvpn_conf['user']}
group ${openvpn_conf['group']}
port ${openvpn_conf['port']}
proto ${openvpn_conf['proto']}
% if openvpn_conf['comp_lzo']:
comp-lzo
% endif
verb ${openvpn_conf['verb']}
% if openvpn_conf['auxiliary']:
${openvpn_conf['auxiliary']}
% endif
