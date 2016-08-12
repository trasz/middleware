from base import RESTTestCase


class SystemDeviceTestCase(RESTTestCase):
    name = 'system/device'

    def test_020_get_classes(self):
        r = self.client.get(self.name + '/get_classes')
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_020_get_devices(self):
        r = self.client.post(self.name + '/get_devices', data=['network'])
        self.assertEqual(r.status_code, 201)
        data = r.json()
        self.assertIsInstance(data, list)
