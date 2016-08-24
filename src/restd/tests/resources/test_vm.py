from base import CRUDTestCase

import time


class VmTestCase(CRUDTestCase):
    name = 'vm'

    def get_create_data(self):
        r = self.client.get(self.name + '/template', params={
            'template.name': 'freebsd-11-zfs',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        template = r.json()[0]
        return {
            'name': 'testvm',
            'enabled': True,
            'target': 'tank',
            'template': template['template'],
        }

    def test_020_create(self):
        tid = super(VmTestCase, self).test_020_create()
        while True:
            r = self.client.get('task', params={
                'id': tid
            })
            self.assertEqual(r.status_code, 200, msg=r.text)
            task = r.json()
            self.assertIsInstance(task, list)
            self.assertEqual(len(task), 1)
            task = task[0]
            if task['state'] != 'FINISHED':
                time.sleep(1)
            else:
                break

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

    def test_061_export(self):
        r = self.client.get(self.name, params={
            'name': 'testvm',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        vm = data[0]
        r = self.client.post(self.name + '/id/' + vm['id'] + '/export')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_062_import(self):
        r = self.client.post(self.name + '/import', data=[
            'testvm', 'tank',
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_064_start(self):
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
