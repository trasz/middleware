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
