from base import RESTTestCase


class SwapTestCase(RESTTestCase):
    name = 'swap'

    def test_020_info(self):
        r = self.client.get(self.name + '/info')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
