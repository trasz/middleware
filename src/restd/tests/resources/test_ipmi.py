from base import RESTTestCase


class IPMITestCase(RESTTestCase):
    name = 'ipmi'

    def check_ipmi(self):
        r = self.client.get(self.name + '/is_ipmi_loaded')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data is False:
            self.skipTest('IPMI not loaded')

    def test_020_retrieve(self):
        self.check_ipmi()
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)

    def test_060_update(self):
        self.check_ipmi()
        r = self.client.put(self.name, data=[
            0, {
                'vlan_id': None,
            },
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)
