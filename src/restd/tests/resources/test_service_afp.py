from base import RESTTestCase


class ServiceAFPTestCase(RESTTestCase):

    def test_020_retrieve(self):
        r = self.client.get('service', params={
            'name': 'afp',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)

    def test_040_update(self):
        r = self.client.get('service', params={
            'name': 'afp',
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
                    'connections_limit': 100,
                },
            },
        ])
        self.assertEqual(r.status_code, 200, msg=r.text)
