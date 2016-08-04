from base import CRUDTestCase


class NetworkInterfaceTestCase(CRUDTestCase):
    name = 'network/interface'

    def get_create_data(self):
        return {
            'type': 'VLAN',
            'id': 'vlan0',
            'dhcp': True,
            'vlan': {
                'parent': 'em0',
                'tag': 10,
            }
        }

    def test_041_down(self):
        r = self.client.post(self.name + '/vlan0/down')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_042_up(self):
        r = self.client.post(self.name + '/vlan0/up')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_043_renew(self):
        r = self.client.post(self.name + '/vlan0/renew')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def get_delete_identifier(self):
        return 'vlan0'
