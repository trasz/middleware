from base import CRUDTestCase


class KerberosRealmTestCase(CRUDTestCase):
    name = 'kerberos/realm'

    def get_create_data(self):
        return {
            'selfsigned': True,
            'id': 'realmtest',
            'realm': 'test',
            'kdc_address': None,
            'admin_server_address': None,
            'password_server_address': None,
        }

    def get_update_ident_data(self):
        return 'realmtest', {
            'realm': 'reamltest'
        }

    def get_delete_identifier(self):
        return 'realmtest'
