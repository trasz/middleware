from base import CRUDTestCase


class DiskTestCase(CRUDTestCase):
    name = 'disk'

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'sort': '-path',
            'online': True,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id'], {
            'apm_mode': 5,
        }

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'sort': '-path',
            'online': False,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if len(data) == 0:
            self.skipTest('No offline disk found to delete.')
        return data[0]['id']

    def test_030_is_online(self):
        r = self.client.get(self.name, params={
            'online': True,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        path = r.json()[0]['path']
        r = self.client.post(self.name + '/is_online', data=[
            path,
        ])
        data = r.json()
        self.assertEqual(data, True)
