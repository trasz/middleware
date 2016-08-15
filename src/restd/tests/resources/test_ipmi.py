from base import RESTTestCase


class IPMITestCase(RESTTestCase):
    name = 'ipmi'

    def test_020_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)

    def test_060_update(self):
        r = self.client.put(self.name, data=[
            0, {
                'vlan_id': None,
            },
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)
