from base import CRUDTestCase, RESTTestCase, SingleItemTestCase

import time


class DockerConfigTestCase(SingleItemTestCase):
    name = 'docker/config'

    def get_update_data(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return {
            'default_host': data['default_host'],
        }


class DockerHostTestCase(RESTTestCase):
    name = 'docker/host'

    def test_040_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        self.assertIsInstance(r.json(), list)


class DockerImageTestCase(RESTTestCase):
    name = 'docker/image'

    def test_040_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        self.assertIsInstance(r.json(), list)


class DockerContainerTestCase(CRUDTestCase):
    name = 'docker/container'

    def get_create_data(self):
        return {
            'names': ['nginx'],
            'image': 'nginx:latest',
            'hostname': 'nginx',
			'command': [],
            'expose_ports': True,
            'ports': [
                {
                    'container_port': 80,
                    'host_port': 8080,
                },
            ]
        }

    def test_020_create(self):
        tid = super(DockerContainerTestCase, self).test_020_create()
        while True:
            r = self.client.get('task', params={
                'id': tid
            })
            self.assertEqual(r.status_code, 200, msg=r.text)
            task = r.json()
            self.assertIsInstance(task, list)
            self.assertEqual(len(task), 1)
            task = task[0]
            if task['state'] == 'FINISHED':
                break
            elif task['state'] == 'FAILED':
                raise AssertionError('docker container create failed')
            else:
                time.sleep(1)

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'image': 'nginx:latest',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        return data[0]['id']
