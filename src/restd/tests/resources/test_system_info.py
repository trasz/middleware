from base import RESTTestCase


class SystemInfoTestCase(RESTTestCase):
    name = 'system/info'

    def test_020_uname_full(self):
        r = self.client.get(self.name + '/uname_full')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 5)

    def test_021_version(self):
        r = self.client.get(self.name + '/version')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, str)

    def test_022_load_avg(self):
        r = self.client.get(self.name + '/load_avg')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)

    def test_023_hardware(self):
        r = self.client.get(self.name + '/hardware')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, dict)
