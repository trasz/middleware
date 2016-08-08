from base import CRUDTestCase


class NetworkHostTestCase(CRUDTestCase):
    name = 'network/host'

    def get_create_data(self):
        return {
            'id': 'freenas.test',
            'addresses': ['1.1.1.1', '2.2.2.2'],
        }

    def get_update_ident_data(self):
        return 'freenas.test', {
            'addresses': ['1.1.1.1'],
        }

    def get_delete_identifier(self):
        return 'freenas.test'
