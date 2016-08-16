from base import RESTTestCase


class ReplicationLinkTestCase(RESTTestCase):
    name = 'replication/link'

    def test_020_retrieve(self):
        r = self.client.get(self.name)
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, list)

    def test_025_get_replication_state(self):
        r = self.client.post(self.name + '/get_replication_state', data={
            'partners': [],
        })
        self.assertEqual(r.status_code, 201, msg=r.text)
