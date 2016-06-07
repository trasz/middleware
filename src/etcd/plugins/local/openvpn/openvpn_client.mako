<%
    openvpn_conf = dispatcher.call_sync('service.openvpn.get_config')
    cert_data = dispatcher.call_sync('crypto.certificate.query',
                                [('id', '=', openvpn_conf['ca'])], {'single': True})
    
    openvpn_conf['ca'] = cert_data['certificate_path'].split('/')[-1]
    openvpn_conf['dev'] = openvpn_conf['dev'].rstrip('0123456789')

    CONFIG_MESSAGE = '''
# This is the OpenVPN client configuration file generated automatically by FreeNas.
# Copy corresponding private key and certificate signed by {0} certificate authority.
# If you are using tls-auth static key you have to copy and paste displayed key
# to the OpenVPN config directory and name it as "ta.key".
# The "remote" directive should contain the ip address of device which provide port redirection
# to the FreeNas appliance configured as OpenVPN server.
'''.format(openvpn_conf['ca'])

%>\
${CONFIG_MESSAGE}
client
dev ${openvpn_conf['dev']}
% if openvpn_conf['persist_key']:
persist-key
% endif
nobind
% if openvpn_conf['persist_tun']:
persist-tun
% endif
remote
ca ${openvpn_conf['ca']}
cert 
key 
% if openvpn_conf['tls_auth']:
tls-auth ta.key 1
% endif
cipher ${openvpn_conf['cipher']}
user ${openvpn_conf['user']}
group ${openvpn_conf['group']}
port ${openvpn_conf['port']}
proto ${openvpn_conf['proto']}
% if openvpn_conf['comp_lzo']:
comp-lzo
% endif
verb ${openvpn_conf['verb']}
