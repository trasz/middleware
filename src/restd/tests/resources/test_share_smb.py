from base import CRUDTestCase


class ShareSMBTestCase(CRUDTestCase):
    name = 'share'

    def get_create_data(self):
        return {
            'name': 'share_smb_test',
            'description': 'Share SMB Test',
            'target_type': 'DATASET',
            'target_path': 'tank/share_smb_test',
            'type': 'smb',
            'properties': {'type': 'service-smb'},
        }

    def get_update_ident_data(self):
        r = self.client.get(self.name, params={
            'name': 'share_smb_test',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if not data:
            self.skipTest('Share not found.')
        return data[0]['id'], {
            'sudo': True,
        }

    def get_delete_identifier(self):
        r = self.client.get(self.name, params={
            'name': 'share_smb_test',
        })
        self.assertEqual(r.status_code, 200, msg=r.text)
        data = r.json()
        if data:
            return data[0]['id']
        else:
            self.skipTest('Share not found.')
