from base import RESTTestCase


class OpenAPITestCase(RESTTestCase):

    def test_020_retrieve(self):
        r = self.client.get('')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, dict)
