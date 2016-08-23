from base import CRUDTestCase


class VmTestCase(CRUDTestCase):
    name = 'vm'

    def get_create_data(self):
        return {
            'name': 'testvm',
            'enabled': True,
            'target': 'tank',
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        config = data[0]['config']
        config['ncpus'] = 2
        return data[0]['id'], {
            'config': config,
        }

    def get_delete_identifier(self):
        raise
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id']
