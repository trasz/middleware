from base import RESTTestCase


class StatTestCase(RESTTestCase):
    name = 'stat'

    def test_020_retrieve(self):
        r = self.client.get(self.name, params={
            'limit': 20,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_021_get_data_sources(self):
        r = self.client.get(self.name + '/get_data_sources')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)


class StatCpuTestCase(RESTTestCase):
    name = 'stat/cpu'

    def test_020_retrieve(self):
        r = self.client.get(self.name, params={
            'limit': 20,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)


class StatDiskTestCase(RESTTestCase):
    name = 'stat/disk'

    def test_020_retrieve(self):
        r = self.client.get(self.name, params={
            'limit': 20,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)


class StatSystemTestCase(RESTTestCase):
    name = 'stat/system'

    def test_020_retrieve(self):
        r = self.client.get(self.name, params={
            'limit': 20,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)


class StatNetworkTestCase(RESTTestCase):
    name = 'stat/network'

    def test_020_retrieve(self):
        r = self.client.get(self.name, params={
            'limit': 20,
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
