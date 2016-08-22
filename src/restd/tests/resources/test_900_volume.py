from base import RESTTestCase


class VolumeDeleteTestCase(RESTTestCase):

    def test_020_delete(self):
        r = self.client.delete('volume/tank')
        self.assertEqual(r.status_code, 204)
