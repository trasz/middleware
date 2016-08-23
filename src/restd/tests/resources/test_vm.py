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

    def test_061_start(self):
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        vm = data[0]
        r = self.client.post(self.name + '/id/' + vm['id'] + '/start')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_065_stop(self):
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        vm = data[0]
        r = self.client.post(self.name + '/id/' + vm['id'] + '/stop')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id']
