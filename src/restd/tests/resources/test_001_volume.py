from base import RESTTestCase


class VolumeCreateTestCase(RESTTestCase):

    def test_020_create(self):
        r = self.client.get('volume/get_available_disks')
        self.assertEqual(r.status_code, 200, msg=r.text)
        disks = r.json()
        r = self.client.post('volume', data={
            'id': 'tank',
            'type': 'zfs',
            'topology': {
                'data': [{
                    'type': 'disk',
                    'path': disks[0],
                }]
            },
        })
        self.assertEqual(r.status_code, 201, msg=r.text)
