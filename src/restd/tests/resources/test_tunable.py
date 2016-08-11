from base import CRUDTestCase


class TunableTestCase(CRUDTestCase):
    name = 'tunable'

    def get_create_data(self):
        return {
            'type': 'RC',
            'var': 'test_enable',
            'value': 'YES',
            'comment': 'test comment',
            'enabled': True,
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'var': 'test_enable',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id'], {
            'value': 'NO',
        }

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'var': 'test_enable',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id']
