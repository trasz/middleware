from base import RESTTestCase


class FilesystemTestCase(RESTTestCase):
    name = 'filesystem'

    def test_020_list_dir(self):
        r = self.client.post(self.name + '/list_dir', data=['/'])
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_021_stat(self):
        r = self.client.post(self.name + '/stat', data=['/usr'])
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, dict)

    def test_022_get_open_files(self):
        r = self.client.post(self.name + '/get_open_files', data=['/'])
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)


class FileTestCase(RESTTestCase):
    name = 'file'

    def test_020_set_permissions(self):
        ssh = self.ssh_exec('touch /mnt/tank/permission')
        self.assertEqual(ssh[0], 0)
        r = self.client.post(self.name + '/set_permissions', data=[
            '/mnt/tank/permission', {
                'user': 'daemon',
            }
        ])
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
