% if config.get('network.dhcp.assign_dns') is False:
    supersede domain-name-servers 0.0.0.0;
% endif
% if config.get('network.dhcp.assign_gateway') is False:
    supersede routers 0.0.0.0;
% endif
