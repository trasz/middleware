from base import CRUDTestCase


class UserTestCase(CRUDTestCase):
    name = 'user'

    def get_create_data(self):
        return {
            'username': 'apitest',
            'password': 'changepass',
        }
