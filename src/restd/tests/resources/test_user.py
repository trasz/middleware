from base import CRUDTestCase


class GroupTestCase(CRUDTestCase):
    name = 'group'

    def get_create_data(self):
        return {
            'name': 'apitest',
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'name': 'apitest',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id'], {
                'sudo': True,
            }
        else:
            self.skipTest('User not found.')

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'apitest',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('Group not found.')


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
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('User not found.')

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'username': 'apitest',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id'], {
                'full_name': 'API Test',
            }
        else:
            self.skipTest('User not found.')

    def test_080_delete(self):
        super(UserTestCase, self).test_080_delete()
        r = self.client.delete('group/id/apitest')
        self.assertEqual(r.status_code, 204, msg=r.text)
        return r
