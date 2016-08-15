from base import RESTTestCase


class DebugTestCase(RESTTestCase):
    name = 'debug'

    def test_020_save_to_file(self):
        r = self.client.post(self.name + '/save_to_file', data=['/tmp/debug'])
        self.assertEqual(r.status_code, 201, msg=r.text)
