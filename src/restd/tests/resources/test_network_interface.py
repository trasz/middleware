from base import CRUDTestCase


class NetworkInterfaceTestCase(CRUDTestCase):
    name = 'network/interface'

    def get_create_data(self):
        return {
            'type': 'VLAN',
        }

    def get_delete_identifier(self):
        return 'vlan0'
