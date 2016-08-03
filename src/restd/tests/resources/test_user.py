from base import CRUDTestCase


class UserTestCase(CRUDTestCase):
    name = 'user'

    def get_create_data(self):
        return {
            'username': 'apitest',
            'password': 'changepass',
        }

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'username': 'apitest',
        })
        self.assertEqual(r.status_code, 200)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('User not found.')
