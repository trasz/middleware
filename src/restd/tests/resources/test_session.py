from base import RESTTestCase


class SessionTestCase(RESTTestCase):
    name = 'session'

    def test_020_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_021_get_live_user_sessions(self):
        r = self.client.get(self.name + '/get_live_user_sessions')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_025_whoami(self):
        r = self.client.get(self.name + '/whoami')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertEqual(data, 'root')
