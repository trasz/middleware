from base import RESTTestCase


class ServiceFTPTestCase(RESTTestCase):

    def test_020_retrieve(self):
        r = self.client.get('service', params={
            'name': 'ftp',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)

    def test_040_update(self):
        r = self.client.get('service', params={
            'name': 'ftp',
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
                    'reverse_dns': True,
                    'root_login': True,
                    'ident': True,
                    'filemask': {
                        'group': {
                            'read': True,
                            'execute': True,
                            'write': True
                        },
                        'others': {
                            'read': True,
                            'execute': True,
                            'write': True
                        },
                        'user': {
                            'read': False,
                            'execute': False,
                            'write': False
                        }
                    },
                    'dirmask': {
                        'group': {
                            'read': True,
                            'execute': True,
                            'write': True
                        },
                        'others': {
                            'read': True,
                            'execute': True,
                            'write': True
                        },
                        'user': {
                            'read': False,
                            'execute': False,
                            'write': False
                        }
                    },
                },
            },
        ])
        self.assertEqual(r.status_code, 200, msg=r.text)
