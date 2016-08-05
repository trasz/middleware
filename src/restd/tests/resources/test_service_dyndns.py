from base import RESTTestCase


class ServiceDynDNSTestCase(RESTTestCase):

    def test_020_retrieve(self):
        r = self.client.get('service', params={
            'name': 'dyndns',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)

    def test_040_update(self):
        r = self.client.get('service', params={
            'name': 'dyndns',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        id = data[0]['id']
        r = self.client.put('service', data=[
            id,
            {
                'config': {
                    'provider': 'dyndns@dyndns.org',
                    'domains': ['apitest.dyndns.org'],
                },
            },
        ])
        self.assertEqual(r.status_code, 200, msg=r.text)
