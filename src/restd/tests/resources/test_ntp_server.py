from base import CRUDTestCase


class NTPServerTestCase(CRUDTestCase):
    name = 'ntp_server'

    def get_create_data(self):
        return {
            'id': 'ntptest',
            'address': 'br.pool.ntp.org',
        }

    def get_update_ident_data(self):
        return 'ntptest', {
            'prefer': True,
        }

    def get_delete_identifier(self):
        return 'ntptest'
