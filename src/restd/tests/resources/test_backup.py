from base import CRUDTestCase


class BackupTestCase(CRUDTestCase):
    name = 'backup'

    def get_create_data(self):
        return {
            'id': 'backuptest',
            'name': 'backuptest',
            'dataset': 'tank',
            'recursive': False,
            'provider': 'ssh',
            'compression': 'NONE',
            'properties': {
                'hostport': 'localhost',
                'username': 'root',
                'password': 'freenas',
                'directory': '/root',
            }
        }

    def get_update_ident_data(self):
        return 'backuptest', {
            'name': 'backup_test',
        }

    def get_delete_identifier(self):
        return 'backuptest'

    def test_030_supported_providers(self):
        r = self.client.get(self.name + '/supported_providers')
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        self.assertIsInstance(data, dict)

    def test_070_sync(self):
        r = self.client.post(self.name + '/id/backuptest/sync')
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()

    def test_071_query(self):
        r = self.client.post(self.name + '/id/backuptest/query')
        self.assertEqual(r.status_code, 201, msg=r.text)

    def test_072_restore(self):
        r = self.client.post(self.name + '/id/backuptest/restore')
        self.assertEqual(r.status_code, 201, msg=r.text)
        data = r.json()
