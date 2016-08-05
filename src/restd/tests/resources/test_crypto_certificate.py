from base import CRUDTestCase


class CryptoCertificateTestCase(CRUDTestCase):
    name = 'crypto/certificate'

    def get_create_data(self):
        return {
            'name': 'test_ca_internal',
            'type': 'CA_INTERNAL',
            'lifetime': 365,
            'country': 'US',
            'state': 'CA',
            'city': 'San Jose',
            'organization': 'iXsystems Inc',
            'email': 'freenas@ixsystems.com',
            'common': 'freenas',
            'selfsigned': True,
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'name': 'test_ca_internal',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id'], {
            'name': 'test_ca_internal_rename',
        }

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'test_ca_internal_rename',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id']
