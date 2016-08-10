from base import RESTTestCase


class ShareISCSITestCase(RESTTestCase):
    name = 'share_iscsi'

    def test_005_auth_create(self):
        r = self.client.post(self.name + '/auth', data={
            'description': 'test auth',
            'type': 'NONE',
        })
        self.assertEqual(r.status_code, 201)

    def test_006_portal_create(self):
        r = self.client.get(self.name + '/auth', params={
            'description': 'test auth',
        })
        auth = r.json()[0]['id']

        r = self.client.post(self.name + '/portal', data={
            'tag': 1,
            'description': 'test portal',
            'discovery_auth_group': '1',
            'listen': [{'address': '0.0.0.0', 'port': 3260}],
        })
        self.assertEqual(r.status_code, 201)

    def test_007_target_create(self):
        r = self.client.get(self.name + '/auth', params={
            'description': 'test auth',
        })
        auth = r.json()[0]['id']
        r = self.client.get(self.name + '/portal', params={
            'description': 'test portal',
        })
        portal = r.json()[0]['id']

        r = self.client.post(self.name + '/target', data={
            'id': 'tgt0',
            'description': 'test target',
            'auth_group': auth,
            'portal_group': portal,
            'extents': []
        })
        self.assertEqual(r.status_code, 201)

    def test_008_share_create(self):
        r = self.client.post('share', data={
            'name': 'testiscsishare',
            'type': 'iscsi',
            'target_type': 'FILE',
            'target_path': '/mnt/tank/testiscsiextent',
            'properties': {
                'type': 'share-iscsi',
                'size': 1024 * 1024 * 10,
            }
        })
        self.assertEqual(r.status_code, 201)

    def test_020_target_retrieve(self):
        r = self.client.get(self.name + '/target', params={
            'id': 'tgt0',
        })
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()), 1)

    def test_021_auth_retrieve(self):
        r = self.client.get(self.name + '/auth')
        self.assertEqual(r.status_code, 200)

    def test_022_portal_retrieve(self):
        r = self.client.get(self.name + '/portal')
        self.assertEqual(r.status_code, 200)

    def test_030_target_update(self):
        r = self.client.put(self.name + '/target/tgt0', data={
            'extents': [
                {'name': 'testiscsishare', 'number': 0},
            ]
        })

    def test_090_target_delete(self):
        r = self.client.delete(self.name + '/target/id/tgt0')
        self.assertEqual(r.status_code, 204)

    def test_091_auth_delete(self):
        r = self.client.get(self.name + '/auth', params={
            'description': 'test auth',
        })
        auth = r.json()[0]['id']
        r = self.client.delete(self.name + '/auth/id/' + auth)
        self.assertEqual(r.status_code, 204)

    def test_092_portal_delete(self):
        r = self.client.get(self.name + '/portal', params={
            'description': 'test portal',
        })
        portal = r.json()[0]['id']
        r = self.client.delete(self.name + '/portal/id/' + portal)
        self.assertEqual(r.status_code, 204)

    def test_093_share_delete(self):
        r = self.client.get('share', params={
            'name': 'testiscsishare',
        })
        share = r.json()[0]['id']
        r = self.client.delete('share/id/' + share)
        self.assertEqual(r.status_code, 204)
