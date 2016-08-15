from base import RESTTestCase


class SyslogTestCase(RESTTestCase):
    name = 'syslog'

    def test_020_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)
